option (ENABLE_ICU "Enable ICU" ON)

if (ENABLE_ICU)
    set (ICU_PATHS "/usr/local/opt/icu4c/lib")
    set (ICU_INCLUDE_PATHS "/usr/local/opt/icu4c/include")
    find_library (ICUI18N icui18n PATHS ${ICU_PATHS})
    find_library (ICUUC icuuc PATHS ${ICU_PATHS})
    find_library (ICUDATA icudata PATHS ${ICU_PATHS})
    set (ICU_LIBS ${ICUI18N} ${ICUUC} ${ICUDATA})

    find_path (ICU_INCLUDE_DIR NAMES unicode/unistr.h PATHS ${ICU_INCLUDE_PATHS})
    include_directories (${ICU_INCLUDE_DIR})

    set(USE_ICU 1)
endif ()

if (USE_ICU)
    message (STATUS "Using icu=${USE_ICU}: ${ICU_INCLUDE_DIR} : ${ICU_LIBS}")
else ()
    message (STATUS "Build without ICU (support for collations and charset conversion functions will be disabled)")
endif ()
