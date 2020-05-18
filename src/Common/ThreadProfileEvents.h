#pragma once

#include <Core/Types.h>
#include <Common/ProfileEvents.h>
#include <ctime>
#include <sys/resource.h>
#include <pthread.h>
#include <common/logger_useful.h>


#if defined(__linux__)
#include <linux/taskstats.h>
#else
struct taskstats {};
#endif

/** Implement ProfileEvents with statistics about resource consumption of the current thread.
  */

namespace ProfileEvents
{
    extern const Event RealTimeMicroseconds;
    extern const Event UserTimeMicroseconds;
    extern const Event SystemTimeMicroseconds;
    extern const Event SoftPageFaults;
    extern const Event HardPageFaults;
    extern const Event VoluntaryContextSwitches;
    extern const Event InvoluntaryContextSwitches;

#if defined(__linux__)
    extern const Event OSIOWaitMicroseconds;
    extern const Event OSCPUWaitMicroseconds;
    extern const Event OSCPUVirtualTimeMicroseconds;
    extern const Event OSReadChars;
    extern const Event OSWriteChars;
    extern const Event OSReadBytes;
    extern const Event OSWriteBytes;

    extern const Event PERF_COUNT_HW_CPU_CYCLES;
    extern const Event PERF_COUNT_HW_CPU_CYCLES_RUNNING;
    extern const Event PERF_COUNT_HW_CPU_CYCLES_ENABLED;
    extern const Event PERF_COUNT_HW_INSTRUCTIONS;
    extern const Event PERF_COUNT_HW_INSTRUCTIONS_RUNNING;
    extern const Event PERF_COUNT_HW_INSTRUCTIONS_ENABLED;
    extern const Event PERF_COUNT_HW_CACHE_REFERENCES;
    extern const Event PERF_COUNT_HW_CACHE_REFERENCES_RUNNING;
    extern const Event PERF_COUNT_HW_CACHE_REFERENCES_ENABLED;
    extern const Event PERF_COUNT_HW_CACHE_MISSES;
    extern const Event PERF_COUNT_HW_CACHE_MISSES_RUNNING;
    extern const Event PERF_COUNT_HW_CACHE_MISSES_ENABLED;
    extern const Event PERF_COUNT_HW_BRANCH_INSTRUCTIONS;
    extern const Event PERF_COUNT_HW_BRANCH_INSTRUCTIONS_RUNNING;
    extern const Event PERF_COUNT_HW_BRANCH_INSTRUCTIONS_ENABLED;
    extern const Event PERF_COUNT_HW_BRANCH_MISSES;
    extern const Event PERF_COUNT_HW_BRANCH_MISSES_RUNNING;
    extern const Event PERF_COUNT_HW_BRANCH_MISSES_ENABLED;
    extern const Event PERF_COUNT_HW_BUS_CYCLES;
    extern const Event PERF_COUNT_HW_BUS_CYCLES_RUNNING;
    extern const Event PERF_COUNT_HW_BUS_CYCLES_ENABLED;
    extern const Event PERF_COUNT_HW_STALLED_CYCLES_FRONTEND;
    extern const Event PERF_COUNT_HW_STALLED_CYCLES_FRONTEND_RUNNING;
    extern const Event PERF_COUNT_HW_STALLED_CYCLES_FRONTEND_ENABLED;
    extern const Event PERF_COUNT_HW_STALLED_CYCLES_BACKEND;
    extern const Event PERF_COUNT_HW_STALLED_CYCLES_BACKEND_RUNNING;
    extern const Event PERF_COUNT_HW_STALLED_CYCLES_BACKEND_ENABLED;
    extern const Event PERF_COUNT_HW_REF_CPU_CYCLES;
    extern const Event PERF_COUNT_HW_REF_CPU_CYCLES_RUNNING;
    extern const Event PERF_COUNT_HW_REF_CPU_CYCLES_ENABLED;

//    extern const Event PERF_COUNT_SW_CPU_CLOCK;
    extern const Event PERF_COUNT_SW_TASK_CLOCK;
    extern const Event PERF_COUNT_SW_PAGE_FAULTS;
    extern const Event PERF_COUNT_SW_CONTEXT_SWITCHES;
    extern const Event PERF_COUNT_SW_CPU_MIGRATIONS;
    extern const Event PERF_COUNT_SW_PAGE_FAULTS_MIN;
    extern const Event PERF_COUNT_SW_PAGE_FAULTS_MAJ;
    extern const Event PERF_COUNT_SW_ALIGNMENT_FAULTS;
    extern const Event PERF_COUNT_SW_EMULATION_FAULTS;

    extern const Event PERF_CUSTOM_INSTRUCTIONS_PER_CPU_CYCLE_SCALED;
    extern const Event PERF_CUSTOM_INSTRUCTIONS_PER_CPU_CYCLE;
#endif
}

namespace DB
{

/// Handles overflow
template <typename TUInt>
inline TUInt safeDiff(TUInt prev, TUInt curr)
{
    return curr >= prev ? curr - prev : 0;
}


inline UInt64 getCurrentTimeNanoseconds(clockid_t clock_type = CLOCK_MONOTONIC)
{
    struct timespec ts;
    clock_gettime(clock_type, &ts);
    return ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}


struct RUsageCounters
{
    /// In nanoseconds
    UInt64 real_time = 0;
    UInt64 user_time = 0;
    UInt64 sys_time = 0;

    UInt64 soft_page_faults = 0;
    UInt64 hard_page_faults = 0;

    RUsageCounters() = default;
    RUsageCounters(const ::rusage & rusage_, UInt64 real_time_)
    {
        set(rusage_, real_time_);
    }

    void set(const ::rusage & rusage, UInt64 real_time_)
    {
        real_time = real_time_;
        user_time = rusage.ru_utime.tv_sec * 1000000000UL + rusage.ru_utime.tv_usec * 1000UL;
        sys_time = rusage.ru_stime.tv_sec * 1000000000UL + rusage.ru_stime.tv_usec * 1000UL;

        soft_page_faults = static_cast<UInt64>(rusage.ru_minflt);
        hard_page_faults = static_cast<UInt64>(rusage.ru_majflt);
    }

    static RUsageCounters zeros(UInt64 real_time_ = getCurrentTimeNanoseconds())
    {
        RUsageCounters res;
        res.real_time = real_time_;
        return res;
    }

    static RUsageCounters current(UInt64 real_time_ = getCurrentTimeNanoseconds())
    {
        ::rusage rusage {};
#if !defined(__APPLE__)
        ::getrusage(RUSAGE_THREAD, &rusage);
#endif
        return RUsageCounters(rusage, real_time_);
    }

    static void incrementProfileEvents(const RUsageCounters & prev, const RUsageCounters & curr, ProfileEvents::Counters & profile_events)
    {
        profile_events.increment(ProfileEvents::RealTimeMicroseconds,   (curr.real_time - prev.real_time) / 1000U);
        profile_events.increment(ProfileEvents::UserTimeMicroseconds,   (curr.user_time - prev.user_time) / 1000U);
        profile_events.increment(ProfileEvents::SystemTimeMicroseconds, (curr.sys_time - prev.sys_time) / 1000U);

        profile_events.increment(ProfileEvents::SoftPageFaults, curr.soft_page_faults - prev.soft_page_faults);
        profile_events.increment(ProfileEvents::HardPageFaults, curr.hard_page_faults - prev.hard_page_faults);
    }

    static void updateProfileEvents(RUsageCounters & last_counters, ProfileEvents::Counters & profile_events)
    {
        auto current_counters = current();
        incrementProfileEvents(last_counters, current_counters, profile_events);
        last_counters = current_counters;
    }
};

#if defined(__linux__)

struct PerfEventInfo
{
    // see perf_event.h/perf_type_id enum
    int event_type;
    // see configs in perf_event.h
    int event_config;
    ProfileEvents::Event profile_event;
    std::optional<ProfileEvents::Event> profile_event_running;
    std::optional<ProfileEvents::Event> profile_event_enabled;
};

struct PerfEventValue
{
    UInt64 value;
    UInt64 time_enabled;
    UInt64 time_running;
};

struct PerfDescriptorsHolder;

struct PerfEventsCounters
{
    // cat /proc/sys/kernel/perf_event_paranoid - if perf_event_paranoid is set to 3, all calls to `perf_event_open` are rejected (even for the current process)
    // https://lwn.net/Articles/696234/
    // -1: Allow use of (almost) all events by all users
    // >=0: Disallow raw tracepoint access by users without CAP_IOC_LOCK
    // >=1: Disallow CPU event access by users without CAP_SYS_ADMIN
    // >=2: Disallow kernel profiling by users without CAP_SYS_ADMIN
    // >=3: Disallow all event access by users without CAP_SYS_ADMIN

    // https://lwn.net/Articles/696216/
    // It adds a another value that can be set for the sysctl parameter (i.e. kernel.perf_event_paranoid=3)
    // that restricts perf_event_open() to processes with the CAP_SYS_ADMIN capability
    // todo: check whether perf_event_open() is available with CAP_SYS_ADMIN

    static constexpr size_t NUMBER_OF_RAW_EVENTS = 18;

    static const PerfEventInfo raw_events_info[PerfEventsCounters::NUMBER_OF_RAW_EVENTS];

    static void initializeProfileEvents(PerfEventsCounters & counters);

    static void finalizeProfileEvents(PerfEventsCounters & counters, ProfileEvents::Counters & profile_events);

private:
    // used to write information about perf unavailability only once for all threads
    static std::atomic<bool> perf_unavailability_logged;
    // used to write information about particular perf events unavailability only once for all threads
    static std::atomic<bool> particular_events_unavailability_logged;

    static thread_local PerfDescriptorsHolder thread_events_descriptors_holder;
    static thread_local bool thread_events_descriptors_opened;
    static thread_local PerfEventsCounters * current_thread_counters;

    // temp array just to not create it each time event processing finishes
    PerfEventValue raw_event_values[NUMBER_OF_RAW_EVENTS]{};

    static Logger * getLogger();

    static bool initializeThreadLocalEvents(PerfEventsCounters & counters);

    [[nodiscard]] PerfEventValue getRawValue(int event_type, int event_config) const;
};

struct PerfDescriptorsHolder
{
    int descriptors[PerfEventsCounters::NUMBER_OF_RAW_EVENTS]{};

    PerfDescriptorsHolder();

    ~PerfDescriptorsHolder();

    static Logger * getLogger();
};

#else

struct PerfEventsCounters
{
    static void initializeProfileEvents(PerfEventsCounters & counters);
    static void finalizeProfileEvents(PerfEventsCounters & counters, ProfileEvents::Counters & profile_events);
};

#endif

#if defined(__linux__)

class TasksStatsCounters
{
public:
    static bool checkIfAvailable();
    static std::unique_ptr<TasksStatsCounters> create(const UInt64 tid);

    void reset();
    void updateCounters(ProfileEvents::Counters & profile_events);

private:
    ::taskstats stats;  //-V730_NOINIT
    std::function<::taskstats()> stats_getter;

    enum class MetricsProvider
    {
        None,
        Procfs,
        Netlink
    };

private:
    explicit TasksStatsCounters(const UInt64 tid, const MetricsProvider provider);

    static MetricsProvider findBestAvailableProvider();
    static void incrementProfileEvents(const ::taskstats & prev, const ::taskstats & curr, ProfileEvents::Counters & profile_events);
};

#else

class TasksStatsCounters
{
public:
    static bool checkIfAvailable() { return false; }
    static std::unique_ptr<TasksStatsCounters> create(const UInt64 /*tid*/) { return {}; }

    void reset() {}
    void updateCounters(ProfileEvents::Counters &) {}

private:
    TasksStatsCounters(const UInt64 /*tid*/) {}
};

#endif

}
