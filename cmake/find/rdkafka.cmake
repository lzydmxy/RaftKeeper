# Freebsd: contrib/cppkafka/include/cppkafka/detail/endianness.h:53:23: error: 'betoh16' was not declared in this scope
if (NOT ARCH_ARM AND NOT OS_FREEBSD AND OPENSSL_FOUND)
    option (ENABLE_RDKAFKA "Enable kafka" ${ENABLE_LIBRARIES})
elseif(ENABLE_RDKAFKA AND NOT OPENSSL_FOUND)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use librdkafka without SSL")
else(ENABLE_RDKAFKA)
    message (${RECONFIGURE_MESSAGE_LEVEL} "librdafka is not supported on ARM and on FreeBSD")
endif ()

if (NOT ENABLE_RDKAFKA)
    if (USE_INTERNAL_RDKAFKA_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use internal librdkafka with ENABLE_RDKAFKA=OFF")
    endif()
    return()
endif()

if (NOT ARCH_ARM AND USE_LIBGSASL)
    option (USE_INTERNAL_RDKAFKA_LIBRARY "Set to FALSE to use system librdkafka instead of the bundled" ${NOT_UNBUNDLED})
elseif(USE_INTERNAL_RDKAFKA_LIBRARY)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use internal librdkafka with ARCH_ARM=${ARCH_ARM} AND USE_LIBGSASL=${USE_LIBGSASL}")
endif ()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/cppkafka/CMakeLists.txt")
    if(USE_INTERNAL_RDKAFKA_LIBRARY)
        message (WARNING "submodule contrib/cppkafka is missing. to fix try run: \n git submodule update --init --recursive")
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal cppkafka")
        set (USE_INTERNAL_RDKAFKA_LIBRARY 0)
    endif()
    set (MISSING_INTERNAL_CPPKAFKA_LIBRARY 1)
endif ()

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/librdkafka/CMakeLists.txt")
    if(USE_INTERNAL_RDKAFKA_LIBRARY OR MISSING_INTERNAL_CPPKAFKA_LIBRARY)
        message (WARNING "submodule contrib/librdkafka is missing. to fix try run: \n git submodule update --init --recursive")
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find internal rdkafka")
        set (USE_INTERNAL_RDKAFKA_LIBRARY 0)
    endif()
    set (MISSING_INTERNAL_RDKAFKA_LIBRARY 1)
endif ()

if (NOT USE_INTERNAL_RDKAFKA_LIBRARY)
    find_library (RDKAFKA_LIB rdkafka)
    find_path (RDKAFKA_INCLUDE_DIR NAMES librdkafka/rdkafka.h PATHS ${RDKAFKA_INCLUDE_PATHS})
    if (NOT RDKAFKA_LIB OR NOT RDKAFKA_INCLUDE_DIR)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system librdkafka")
    endif()

    if (USE_STATIC_LIBRARIES AND NOT OS_FREEBSD)
       find_library (SASL2_LIBRARY sasl2)
       if (NOT SASL2_LIBRARY)
           message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system sasl2 library needed for static librdkafka")
       endif()
    endif ()
    set (CPPKAFKA_LIBRARY cppkafka) # TODO: try to use unbundled version.
endif ()

if (RDKAFKA_LIB AND RDKAFKA_INCLUDE_DIR)
    set (USE_RDKAFKA 1)
    set (RDKAFKA_LIBRARY ${RDKAFKA_LIB} ${OPENSSL_LIBRARIES})
    set (CPPKAFKA_LIBRARY cppkafka)
    if (SASL2_LIBRARY)
       list (APPEND RDKAFKA_LIBRARY ${SASL2_LIBRARY})
    endif ()
    if (LZ4_LIBRARY)
       list (APPEND RDKAFKA_LIBRARY ${LZ4_LIBRARY})
    endif ()
elseif (NOT MISSING_INTERNAL_RDKAFKA_LIBRARY AND NOT MISSING_INTERNAL_CPPKAFKA_LIBRARY AND NOT ARCH_ARM)
    set (USE_INTERNAL_RDKAFKA_LIBRARY 1)
    set (RDKAFKA_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/librdkafka/src")
    set (RDKAFKA_LIBRARY rdkafka)
    set (CPPKAFKA_LIBRARY cppkafka)
    set (USE_RDKAFKA 1)
elseif(ARCH_ARM)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Using internal rdkafka on ARM is not supported")
endif ()

message (STATUS "Using librdkafka=${USE_RDKAFKA}: ${RDKAFKA_INCLUDE_DIR} : ${RDKAFKA_LIBRARY} ${CPPKAFKA_LIBRARY}")
