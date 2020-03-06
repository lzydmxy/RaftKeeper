#include <Common/ThreadProfileEvents.h>

#if defined(__linux__)
#include <unistd.h>
#include <linux/perf_event.h>
#include <syscall.h>
#include <sys/ioctl.h>
#include <cerrno>
#endif

namespace DB {

#if defined(__linux__)

    static PerfEventInfo softwareEvent(int event_config, ProfileEvents::Event profile_event)
    {
        return PerfEventInfo
        {
            .event_type = perf_type_id::PERF_TYPE_SOFTWARE,
            .event_config = event_config,
            .profile_event = profile_event
        };
    }

    static PerfEventInfo hardwareEvent(int event_config, ProfileEvents::Event profile_event)
    {
        return PerfEventInfo
        {
            .event_type = perf_type_id::PERF_TYPE_HARDWARE,
            .event_config = event_config,
            .profile_event = profile_event
        };
    }

    // descriptions' source: http://man7.org/linux/man-pages/man2/perf_event_open.2.html
    const PerfEventInfo PerfEventsCounters::perf_raw_events_info[] = {
            hardwareEvent(PERF_COUNT_HW_CPU_CYCLES, ProfileEvents::PERF_COUNT_HW_CPU_CYCLES),
            hardwareEvent(PERF_COUNT_HW_INSTRUCTIONS, ProfileEvents::PERF_COUNT_HW_INSTRUCTIONS),
            hardwareEvent(PERF_COUNT_HW_CACHE_REFERENCES, ProfileEvents::PERF_COUNT_HW_CACHE_REFERENCES),
            hardwareEvent(PERF_COUNT_HW_CACHE_MISSES, ProfileEvents::PERF_COUNT_HW_CACHE_MISSES),
            hardwareEvent(PERF_COUNT_HW_BRANCH_INSTRUCTIONS, ProfileEvents::PERF_COUNT_HW_BRANCH_INSTRUCTIONS),
            hardwareEvent(PERF_COUNT_HW_BRANCH_MISSES, ProfileEvents::PERF_COUNT_HW_BRANCH_MISSES),
            hardwareEvent(PERF_COUNT_HW_BUS_CYCLES, ProfileEvents::PERF_COUNT_HW_BUS_CYCLES),
            hardwareEvent(PERF_COUNT_HW_STALLED_CYCLES_FRONTEND, ProfileEvents::PERF_COUNT_HW_STALLED_CYCLES_FRONTEND),
            hardwareEvent(PERF_COUNT_HW_STALLED_CYCLES_BACKEND, ProfileEvents::PERF_COUNT_HW_STALLED_CYCLES_BACKEND),
            hardwareEvent(PERF_COUNT_HW_REF_CPU_CYCLES, ProfileEvents::PERF_COUNT_HW_REF_CPU_CYCLES),
            
            // This reports the CPU clock, a high-resolution per-CPU timer.
            // a bit broken according to this: https://stackoverflow.com/a/56967896
//            makeInfo(perf_type_id::PERF_TYPE_SOFTWARE, perf_sw_ids::PERF_COUNT_SW_CPU_CLOCK, ProfileEvents::PERF_COUNT_SW_CPU_CLOCK),
            softwareEvent(PERF_COUNT_SW_TASK_CLOCK, ProfileEvents::PERF_COUNT_SW_TASK_CLOCK),
            softwareEvent(PERF_COUNT_SW_PAGE_FAULTS, ProfileEvents::PERF_COUNT_SW_PAGE_FAULTS),
            softwareEvent(PERF_COUNT_SW_CONTEXT_SWITCHES, ProfileEvents::PERF_COUNT_SW_CONTEXT_SWITCHES),
            softwareEvent(PERF_COUNT_SW_CPU_MIGRATIONS, ProfileEvents::PERF_COUNT_SW_CPU_MIGRATIONS),
            softwareEvent(PERF_COUNT_SW_PAGE_FAULTS_MIN, ProfileEvents::PERF_COUNT_SW_PAGE_FAULTS_MIN),
            softwareEvent(PERF_COUNT_SW_PAGE_FAULTS_MAJ, ProfileEvents::PERF_COUNT_SW_PAGE_FAULTS_MAJ),
            softwareEvent(PERF_COUNT_SW_ALIGNMENT_FAULTS, ProfileEvents::PERF_COUNT_SW_ALIGNMENT_FAULTS),
            softwareEvent(PERF_COUNT_SW_EMULATION_FAULTS, ProfileEvents::PERF_COUNT_SW_EMULATION_FAULTS)
            // This is a placeholder event that counts nothing. Informational sample record types such as mmap or
            // comm must be associated with an active event. This dummy event allows gathering such records
            // without requiring a counting event.
//            softwareEventInfo(perf_sw_ids::PERF_COUNT_SW_DUMMY, ProfileEvents::PERF_COUNT_SW_DUMMY)
    };

    static_assert(std::size(PerfEventsCounters::perf_raw_events_info) == PerfEventsCounters::NUMBER_OF_RAW_EVENTS);

    std::atomic<bool> PerfEventsCounters::events_availability_logged = false;

    Logger * PerfEventsCounters::getLogger()
    {
        return &Logger::get("PerfEventsCounters");
    }

    long long PerfEventsCounters::getRawValue(int event_type, int event_config) const
    {
        for (size_t i = 0; i < NUMBER_OF_RAW_EVENTS; ++i)
        {
            const PerfEventInfo & event_info = perf_raw_events_info[i];
            if (event_info.event_type == event_type && event_info.event_config == event_config)
                return raw_event_values[i];
        }

        LOG_WARNING(getLogger(), "Can't find perf event info for event_type=" << event_type << ", event_config=" << event_config);
        return 0;
    }

    static int openPerfEvent(perf_event_attr *hw_event, pid_t pid, int cpu, int group_fd, unsigned long flags) {
        return static_cast<int>(syscall(SYS_perf_event_open, hw_event, pid, cpu, group_fd, flags));
    }

    static bool getPerfEventParanoid(int &result) {
        // the longest possible variant: "-1\0"
        constexpr int MAX_LENGTH = 3;
        FILE *fp;
        char str[MAX_LENGTH];

        fp = fopen("/proc/sys/kernel/perf_event_paranoid", "r");
        if (fp == nullptr)
            return false;

        char *res = fgets(str, MAX_LENGTH, fp);
        fclose(fp);

        if (res == nullptr)
            return false;

        str[MAX_LENGTH - 1] = '\0';
        long value = strtol(str, nullptr, 10);
        // the only way to be incorrect is to not be a number
        if (value == 0 && errno != 0)
            return false;

        result = static_cast<int>(value);
        return true;
    }

    static void perfEventOpenDisabled(int perf_event_paranoid, int perf_event_type, int perf_event_config, int &event_file_descriptor) {
        perf_event_attr pe = perf_event_attr();
        pe.type = perf_event_type;
        pe.size = sizeof(struct perf_event_attr);
        pe.config = perf_event_config;
        // disable by default to add as little extra time as possible
        pe.disabled = 1;
        // can record kernel only when `perf_event_paranoid` <= 1
        pe.exclude_kernel = perf_event_paranoid >= 2;

        event_file_descriptor = openPerfEvent(&pe, /* measure the calling thread */ 0, /* on any cpu */ -1, -1, 0);
    }

    void PerfEventsCounters::initializeProfileEvents(PerfEventsCounters & counters) {
        if (counters.perf_events_recording)
            return;

        int perf_event_paranoid = 0;
        bool is_pref_available = getPerfEventParanoid(perf_event_paranoid);
//        printf("is_perf_available: %s, perf_event_paranoid: %d\n", is_pref_available ? "true" : "false", perf_event_paranoid);

        if (!is_pref_available)
            return;

        bool expected = false;
        bool log_unsupported_event = events_availability_logged.compare_exchange_strong(expected, true);
        for (size_t i = 0; i < NUMBER_OF_RAW_EVENTS; ++i)
        {
            counters.raw_event_values[i] = 0;
            const PerfEventInfo & event_info = perf_raw_events_info[i];
            int & fd = counters.events_descriptors[i];
            perfEventOpenDisabled(perf_event_paranoid, event_info.event_type, event_info.event_config, fd);

            if (fd == -1 && log_unsupported_event)
            {
                LOG_WARNING(
                        getLogger(),
                        "Perf event is unsupported: event_type=" << event_info.event_type
                            << ", event_config=" << event_info.event_config);
            }
        }

        for (int fd : counters.events_descriptors)
        {
            if (fd != -1)
                ioctl(fd, PERF_EVENT_IOC_ENABLE, 0);
        }

        counters.perf_events_recording = true;
    }

    void PerfEventsCounters::finalizeProfileEvents(PerfEventsCounters & counters, ProfileEvents::Counters & profile_events) {
        if (!counters.perf_events_recording)
            return;

        // process raw events

        // only read counters here to have as little overhead for processing as possible
        for (size_t i = 0; i < NUMBER_OF_RAW_EVENTS; ++i)
        {
            int fd = counters.events_descriptors[i];
            if (fd != -1)
                read(fd, &counters.raw_event_values[i], sizeof(long long));
        }

        // actually process counters' values and release resources
        for (size_t i = 0; i < NUMBER_OF_RAW_EVENTS; ++i)
        {
            int & fd = counters.events_descriptors[i];
            if (fd == -1)
                continue;

            profile_events.increment(perf_raw_events_info[i].profile_event, counters.raw_event_values[i]);

            if (ioctl(fd, PERF_EVENT_IOC_DISABLE, 0))
                LOG_WARNING(getLogger(), "Can't disable perf event with file descriptor: " << fd);

            if (close(fd))
                LOG_WARNING(getLogger(), "Can't close perf event file descriptor: " << fd << "; error: " << errno << " - " << strerror(errno));

            fd = -1;
        }

        // process custom events which depend on the raw ones
        long long hw_cpu_cycles = counters.getRawValue(PERF_TYPE_HARDWARE, PERF_COUNT_HW_CPU_CYCLES);
        long long hw_ref_cpu_cycles = counters.getRawValue(PERF_TYPE_HARDWARE, PERF_COUNT_HW_REF_CPU_CYCLES);

        long long instructions_per_cpu_scaled = hw_cpu_cycles != 0
                ? counters.getRawValue(PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS) / hw_cpu_cycles
                : 0;
        long long instructions_per_cpu = hw_ref_cpu_cycles != 0
                ? counters.getRawValue(PERF_TYPE_HARDWARE, PERF_COUNT_HW_INSTRUCTIONS) / hw_ref_cpu_cycles
                : 0;

        profile_events.increment(ProfileEvents::PERF_CUSTOM_INSTRUCTIONS_PER_CPU_CYCLE_SCALED, instructions_per_cpu_scaled);
        profile_events.increment(ProfileEvents::PERF_CUSTOM_INSTRUCTIONS_PER_CPU_CYCLE, instructions_per_cpu);

        counters.perf_events_recording = false;
    }

#else

    void PerfEventsCounters::initializeProfileEvents(PerfEventsCounters &) {}
    void PerfEventsCounters::finalizeProfileEvents(PerfEventsCounters &, ProfileEvents::Counters &) {}

#endif

}