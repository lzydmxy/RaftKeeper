option (ENABLE_RDKAFKA "Enable kafka" ${NOT_MSVC})

if (ENABLE_RDKAFKA)

option (USE_INTERNAL_RDKAFKA_LIBRARY "Set to FALSE to use system librdkafka instead of the bundled" ${NOT_UNBUNDLED})

if (USE_INTERNAL_RDKAFKA_LIBRARY AND NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/librdkafka/CMakeLists.txt")
   message (WARNING "submodule contrib/librdkafka is missing. to fix try run: \n git submodule update --init --recursive")
   set (USE_INTERNAL_RDKAFKA_LIBRARY 0)
   set (MISSING_INTERNAL_RDKAFKA_LIBRARY 1)
endif ()

if (NOT USE_INTERNAL_RDKAFKA_LIBRARY)
    find_library (RDKAFKA_LIB rdkafka)
    find_path (RDKAFKA_INCLUDE_DIR NAMES librdkafka/rdkafka.h PATHS ${RDKAFKA_INCLUDE_PATHS})
    if (USE_STATIC_LIBRARIES AND NOT ARCH_FREEBSD)
       find_library (SASL2_LIBRARY sasl2)
    endif ()
endif ()

if (RDKAFKA_LIB AND RDKAFKA_INCLUDE_DIR)
    set (USE_RDKAFKA 1)
    set (RDKAFKA_LIBRARY ${RDKAFKA_LIB} ${OPENSSL_LIBRARIES})
    if (SASL2_LIBRARY)
       list (APPEND RDKAFKA_LIBRARY ${SASL2_LIBRARY})
    endif ()
    if (LZ4_LIBRARY)
       list (APPEND RDKAFKA_LIBRARY ${LZ4_LIBRARY})
    endif ()
elseif (NOT MISSING_INTERNAL_RDKAFKA_LIBRARY)
    set (USE_INTERNAL_RDKAFKA_LIBRARY 1)
    set (RDKAFKA_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/librdkafka/src")
    set (RDKAFKA_LIBRARY rdkafka)
    set (USE_RDKAFKA 1)
endif ()

endif ()

message (STATUS "Using librdkafka=${USE_RDKAFKA}: ${RDKAFKA_INCLUDE_DIR} : ${RDKAFKA_LIBRARY}")
