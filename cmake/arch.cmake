if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64.*|AARCH64.*)")
    set (ARCH_AARCH64 1)
endif ()
if (ARCH_AARCH64 OR CMAKE_SYSTEM_PROCESSOR MATCHES "arm")
    set (ARCH_ARM 1)
endif ()
if (CMAKE_LIBRARY_ARCHITECTURE MATCHES "i386")
    set (ARCH_I386 1)
endif ()
if ( ( ARCH_ARM AND NOT ARCH_AARCH64 ) OR ARCH_I386)
    set (ARCH_32 1)
    message (WARNING "Support for 32bit platforms is highly experimental")
endif ()
if (CMAKE_SYSTEM MATCHES "Linux")
    set (ARCH_LINUX 1)
endif ()
if (CMAKE_SYSTEM MATCHES "FreeBSD")
    set (ARCH_FREEBSD 1)
endif ()
