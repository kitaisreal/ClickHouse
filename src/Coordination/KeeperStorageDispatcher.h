#pragma once

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#    include "config_core.h"
#endif

#if USE_NURAFT

#include <Common/ThreadPool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>
#include <common/logger_useful.h>
#include <functional>
#include <Coordination/KeeperServer.h>
#include <Coordination/CoordinationSettings.h>


namespace DB
{

using ZooKeeperResponseCallback = std::function<void(const Coordination::ZooKeeperResponsePtr & response)>;

class KeeperStorageDispatcher
{

private:
    std::mutex push_request_mutex;

    CoordinationSettingsPtr coordination_settings;
    using RequestsQueue = ConcurrentBoundedQueue<KeeperStorage::RequestForSession>;
    using SessionToResponseCallback = std::unordered_map<int64_t, ZooKeeperResponseCallback>;

    RequestsQueue requests_queue{1};
    ResponsesQueue responses_queue;
    SnapshotsQueue snapshots_queue{1};

    std::atomic<bool> shutdown_called{false};

    std::mutex session_to_response_callback_mutex;
    SessionToResponseCallback session_to_response_callback;

    ThreadFromGlobalPool request_thread;
    ThreadFromGlobalPool responses_thread;
    ThreadFromGlobalPool session_cleaner_thread;
    ThreadFromGlobalPool snapshot_thread;

    std::unique_ptr<KeeperServer> server;

    Poco::Logger * log;

private:
    void requestThread();
    void responseThread();
    void sessionCleanerTask();
    void snapshotThread();
    void setResponse(int64_t session_id, const Coordination::ZooKeeperResponsePtr & response);

public:
    KeeperStorageDispatcher();

    void initialize(const Poco::Util::AbstractConfiguration & config);

    void shutdown();

    ~KeeperStorageDispatcher();

    bool putRequest(const Coordination::ZooKeeperRequestPtr & request, int64_t session_id);

    bool isLeader() const
    {
        return server->isLeader();
    }

    bool hasLeader() const
    {
        return server->isLeaderAlive();
    }

    int64_t getSessionID(long session_timeout_ms)
    {
        return server->getSessionID(session_timeout_ms);
    }

    void registerSession(int64_t session_id, ZooKeeperResponseCallback callback);
    /// Call if we don't need any responses for this session no more (session was expired)
    void finishSession(int64_t session_id);
};

}

#endif
