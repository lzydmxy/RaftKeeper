#pragma once
#include <cstdint>


namespace DB
{

/** Opens a file /proc/self/mstat. Keeps it open and reads memory statistics via 'pread'.
  * This is Linux specific.
  * See: man procfs
  *
  * Note: a class is used instead of a single function to avoid excessive file open/close on every use.
  * pread is used to avoid lseek.
  */
class MemoryStatisticsOS
{
public:
    /// In number of bytes.
    struct Data
    {
        uint64_t virt;
        uint64_t resident;
        uint64_t shared;
        uint64_t code;
        uint64_t data_and_stack;
    };

    MemoryStatisticsOS();
    ~MemoryStatisticsOS();

    /// Thread-safe.
    Data get() const;

private:
    int fd;
};

}
