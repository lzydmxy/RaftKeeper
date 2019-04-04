#pragma once

#include <mutex>
#include <sys/statvfs.h>
#include <memory>
#include <boost/noncopyable.hpp>
#include <Poco/Util/AbstractConfiguration.h>
#include <common/logger_useful.h>
#include <Common/Exception.h>
#include <IO/WriteHelpers.h>
#include <Common/formatReadable.h>
#include <Common/CurrentMetrics.h>


namespace CurrentMetrics
{
    extern const Metric DiskSpaceReservedForMerge;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_STATVFS;
    extern const int NOT_ENOUGH_SPACE;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

struct Disk {
    String path;
    UInt64 keep_free_space_bytes;

    Disk(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);
};


/** Determines amount of free space in filesystem.
  * Could "reserve" space, for different operations to plan disk space usage.
  * Reservations are not separated for different filesystems,
  *  instead it is assumed, that all reservations are done within same filesystem.
  *
  *  It is necessary to set all paths in map before MergeTreeData starts
  */
class DiskSpaceMonitor
{
public:
    struct DiskReserve {
        UInt64 reserved_bytes;
        UInt64 reservation_count;
    };

    class Reservation : private boost::noncopyable
    {
    public:
        ~Reservation()
        {
            try
            {
                std::lock_guard lock(DiskSpaceMonitor::mutex);
                if (reserves->reserved_bytes < size)
                {
                    reserves->reserved_bytes = 0;
                    LOG_ERROR(&Logger::get("DiskSpaceMonitor"), "Unbalanced reservations size; it's a bug");
                }
                else
                {
                    reserves->reserved_bytes -= size;
                }

                if (reserves->reservation_count == 0)
                {
                    LOG_ERROR(&Logger::get("DiskSpaceMonitor"), "Unbalanced reservation count; it's a bug");
                }
                else
                {
                    --reserves->reservation_count;
                }
            }
            catch (...)
            {
                tryLogCurrentException("~DiskSpaceMonitor");
            }
        }

        /// Change amount of reserved space. When new_size is greater than before, availability of free space is not checked.
        void update(UInt64 new_size)
        {
            std::lock_guard lock(DiskSpaceMonitor::mutex);
            reserves->reserved_bytes -= size;
            size = new_size;
            reserves->reserved_bytes += size;
        }

        UInt64 getSize() const
        {
            return size;
        }

        const String & getPath() const {
            return path;
        }

        void addEnclosedDirToPath(const String & dir) {
            path += dir + '/';
        }

        Reservation(UInt64 size_, DiskReserve * reserves_, const String & path_)
            : size(size_), metric_increment(CurrentMetrics::DiskSpaceReservedForMerge, size), reserves(reserves_),
              path(path_)
        {
            std::lock_guard lock(DiskSpaceMonitor::mutex);
            reserves->reserved_bytes += size;
            ++reserves->reservation_count;
        }

    private:
        UInt64 size;
        CurrentMetrics::Increment metric_increment;
        DiskReserve * reserves;
        String path;
    };

    using ReservationPtr = std::unique_ptr<Reservation>;

    static UInt64 getUnreservedFreeSpace(const Disk & disk)
    {
        struct statvfs fs;

        if (statvfs(disk.path.c_str(), &fs) != 0)
            throwFromErrno("Could not calculate available disk space (statvfs)", ErrorCodes::CANNOT_STATVFS);

        UInt64 res = fs.f_bfree * fs.f_bsize;

        res -= std::min(res, disk.keep_free_space_bytes);  ///@TODO_IGR ASK Is Heuristic by Michael Kolupaev actual?

        /// Heuristic by Michael Kolupaev: reserve 30 MB more, because statvfs shows few megabytes more space than df.
        res -= std::min(res, static_cast<UInt64>(30 * (1ul << 20)));

        std::lock_guard lock(mutex);

        auto & reserved_bytes = reserved[disk.path].reserved_bytes;

        if (reserved_bytes > res)
            res = 0;
        else
            res -= reserved_bytes;

        return res;
    }

    static UInt64 getAllReservedSpace()
    {
        std::lock_guard lock(mutex);
        UInt64 res;
        for (const auto & reserve : reserved) {
            res += reserve.second.reserved_bytes;
        }
        return res;
    }

    static UInt64 getAllReservationCount()
    {
        std::lock_guard lock(mutex);
        UInt64 res;
        for (const auto & reserve : reserved) {
            res += reserve.second.reservation_count;
        }
        return res;
    }

    /// If not enough (approximately) space, do not reserve.
    static ReservationPtr tryToReserve(const Disk & disk, UInt64 size)
    {
        UInt64 free_bytes = getUnreservedFreeSpace(disk);
        ///@TODO_IGR ASK twice reservation?
        if (free_bytes < size)
        {
            return {};
        }
        return std::make_unique<Reservation>(size, &reserved[disk.path], disk.path);
    }

private:
    static std::map<String, DiskReserve> reserved;
    static std::mutex mutex;
};

class DisksSelector {
public:
    DisksSelector() = default;

    DisksSelector(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

    const Disk & operator[](const String & name) const;

    ///@TODO_IGR REMOVE it
    size_t size() const {
        return disks.size();
    }

private:
    std::map<String, Disk> disks;
};

class Schema
{
    class Volume {
        friend class Schema;

    public:
        Volume(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const DisksSelector & disk_selector);

        Volume(const Volume & other)
            : max_data_part_size(other.max_data_part_size),
              disks(other.disks),
              last_used(0)
        {
        }

        bool setDefaultPath(const String & path);

        DiskSpaceMonitor::ReservationPtr reserve(UInt64 expected_size) const;

        UInt64 getMaxUnreservedFreeSpace() const;

    private:
        UInt64 max_data_part_size;

        std::vector<Disk> disks;
        mutable std::atomic<size_t> last_used = 0; ///@TODO_IGR ASK It is thread safe, but it is not consistent. :(
                                                   /// P.S. I do not want to use mutex here
    };

public:
    Schema(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const DisksSelector & disks);

    void setDefaultPath(const String & path);

    Strings getFullPaths() const;

    UInt64 getMaxUnreservedFreeSpace() const;

    DiskSpaceMonitor::ReservationPtr reserve(UInt64 expected_size) const;

private:
    std::vector<Volume> volumes;
};

class SchemaSelector {
public:
    SchemaSelector(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix, const DisksSelector & disks);

    const Schema & operator[](const String & name) const;

private:
    std::map<String, Schema> schemes;
};

class MergeTreeStorageConfiguration {
public:
    MergeTreeStorageConfiguration(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix);

    const Schema & operator[](const String & name) const {
        return schema_selector[name];
    }

private:
    DisksSelector disks;
    SchemaSelector schema_selector;
};

}
