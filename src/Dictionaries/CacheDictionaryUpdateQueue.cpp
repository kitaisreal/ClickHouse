#include "CacheDictionaryUpdateQueue.h"

#include <base/MoveOrCopyIfThrow.h>
#include <Common/setThreadName.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CACHE_DICTIONARY_UPDATE_FAIL;
    extern const int UNSUPPORTED_METHOD;
    extern const int TIMEOUT_EXCEEDED;
}

template class CacheDictionaryUpdateUnit<DictionaryKeyType::Simple>;
template class CacheDictionaryUpdateUnit<DictionaryKeyType::Complex>;

template <DictionaryKeyType dictionary_key_type>
CacheDictionaryUpdateQueue<dictionary_key_type>::CacheDictionaryUpdateQueue(
    String dictionary_name_for_logs_,
    CacheDictionaryUpdateQueueConfiguration configuration_,
    UpdateFunction && update_func_)
    : dictionary_name_for_logs(std::move(dictionary_name_for_logs_))
    , configuration(configuration_)
    , update_func(std::move(update_func_))
    , empty_count(configuration.max_update_queue_size, configuration.max_update_queue_size)
    , update_pool(configuration.max_threads_for_updates)
    , log(&Poco::Logger::get("CacheDictionaryUpdateQueue"))
{
    for (size_t i = 0; i < configuration.max_threads_for_updates; ++i)
        update_pool.scheduleOrThrowOnError([this] { updateThreadFunction(); });
}

template <DictionaryKeyType dictionary_key_type>
CacheDictionaryUpdateQueue<dictionary_key_type>::~CacheDictionaryUpdateQueue()
{
    if (finished)
        return;

    try
    {
        stopAndWait();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Caught exception during destruction");
    }
}

template <DictionaryKeyType dictionary_key_type>
void CacheDictionaryUpdateQueue<dictionary_key_type>::tryPushToUpdateQueueOrThrow(CacheDictionaryUpdateUnitPtr<dictionary_key_type> & update_unit_ptr)
{
    if (finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CacheDictionaryUpdateQueue for dictionary {} already finished", dictionary_name_for_logs);

    if (!empty_count.tryWait(configuration.update_queue_push_timeout_milliseconds))
        throw DB::Exception(ErrorCodes::CACHE_DICTIONARY_UPDATE_FAIL,
            "Cannot push to internal update queue in dictionary {}. "
            "Timelimit of {} ms. exceeded. Current queue size is {}",
            dictionary_name_for_logs,
            configuration.update_queue_push_timeout_milliseconds,
            getSize());

    try
    {
        std::unique_lock<std::mutex> update_lock(queue_mutex);
        queue.push(update_unit_ptr);

        queue_cond.notify_one();
    }
    catch (...)
    {
        empty_count.set();
        throw;
    }
}

template <DictionaryKeyType dictionary_key_type>
void CacheDictionaryUpdateQueue<dictionary_key_type>::waitForCurrentUpdateFinish(CacheDictionaryUpdateUnitPtr<dictionary_key_type> & update_unit_ptr) const
{
    if (finished)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CacheDictionaryUpdateQueue for dictionary {} already finished", dictionary_name_for_logs);

    std::unique_lock<std::mutex> update_lock(update_unit_ptr->lock);

    bool result = update_unit_ptr->is_update_finished.wait_for(
        update_lock,
        std::chrono::milliseconds(configuration.query_wait_timeout_milliseconds),
        [&]
        {
            return update_unit_ptr->is_done || update_unit_ptr->current_exception;
        });

    if (!result)
    {
        throw DB::Exception(
            ErrorCodes::TIMEOUT_EXCEEDED,
            "Dictionary {} source seems unavailable, because {} ms timeout exceeded.",
            dictionary_name_for_logs,
            toString(configuration.query_wait_timeout_milliseconds));
    }

    if (update_unit_ptr->current_exception)
    {
        // Don't just rethrow it, because sharing the same exception object
        // between multiple threads can lead to weird effects if they decide to
        // modify it, for example, by adding some error context.
        try
        {
            std::rethrow_exception(update_unit_ptr->current_exception);
        }
        catch (...)
        {
            throw DB::Exception(
                ErrorCodes::CACHE_DICTIONARY_UPDATE_FAIL,
                "Update failed for dictionary '{}': {}",
                dictionary_name_for_logs,
                getCurrentExceptionMessage(true /*with stack trace*/, true /*check embedded stack trace*/));
        }
    }
}

template <DictionaryKeyType dictionary_key_type>
void CacheDictionaryUpdateQueue<dictionary_key_type>::stopAndWait()
{
    if (finished.exchange(true))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CacheDictionaryUpdateQueue for dictionary {} already finished", dictionary_name_for_logs);

    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        std::queue<CacheDictionaryUpdateUnitPtr<dictionary_key_type>> empty_queue;
        queue.swap(empty_queue);
        queue_cond.notify_all();
    }

    update_pool.wait();
}

template <DictionaryKeyType dictionary_key_type>
void CacheDictionaryUpdateQueue<dictionary_key_type>::updateThreadFunction()
{
    setThreadName("UpdQueue");

    while (!finished)
    {
        CacheDictionaryUpdateUnitPtr<dictionary_key_type> unit_to_update;

        {
            std::unique_lock<std::mutex> queue_cond_lock(queue_mutex);
            queue_cond.wait(queue_cond_lock, [&](){ return finished || !queue.empty(); });

            if (finished)
                break;

            ::detail::moveOrCopyIfThrow(std::move(queue.front()), unit_to_update);
            queue.pop();
        }

        empty_count.set();

        try
        {
            /// Update
            update_func(unit_to_update);

            /// Notify thread about finished updating the bunch of ids
            /// where their own ids were included.
            std::unique_lock<std::mutex> lock(unit_to_update->lock);
            unit_to_update->is_done = true;
        }
        catch (...)
        {
            std::unique_lock<std::mutex> lock(unit_to_update->lock);
            unit_to_update->current_exception = std::current_exception(); // NOLINT(bugprone-throw-keyword-missing)
        }

        unit_to_update->is_update_finished.notify_all();
    }
}

template <DictionaryKeyType dictionary_key_type>
size_t CacheDictionaryUpdateQueue<dictionary_key_type>::getSize() const
{
    std::lock_guard lock(queue_mutex);
    return queue.size();
}

template class CacheDictionaryUpdateQueue<DictionaryKeyType::Simple>;
template class CacheDictionaryUpdateQueue<DictionaryKeyType::Complex>;

}
