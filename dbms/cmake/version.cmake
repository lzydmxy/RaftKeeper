# This strings autochanged from release_lib.sh:
set(VERSION_REVISION 54425)
set(VERSION_MAJOR 19)
set(VERSION_MINOR 14)
set(VERSION_PATCH 5)
set(VERSION_GITHASH 60cc8fb29f15a58604eec0168ef2dd5bb531f9aa)
set(VERSION_DESCRIBE v19.14.5.1-prestable)
set(VERSION_STRING 19.14.5.1)
# end of autochange

set(VERSION_EXTRA "" CACHE STRING "")
set(VERSION_TWEAK "" CACHE STRING "")

if (VERSION_TWEAK)
    string(CONCAT VERSION_STRING ${VERSION_STRING} "." ${VERSION_TWEAK})
endif ()

if (VERSION_EXTRA)
    string(CONCAT VERSION_STRING ${VERSION_STRING} "." ${VERSION_EXTRA})
endif ()

set (VERSION_NAME "${PROJECT_NAME}")
set (VERSION_FULL "${VERSION_NAME} ${VERSION_STRING}")
set (VERSION_SO "${VERSION_STRING}")

math (EXPR VERSION_INTEGER "${VERSION_PATCH} + ${VERSION_MINOR}*1000 + ${VERSION_MAJOR}*1000000")

if(YANDEX_OFFICIAL_BUILD)
    set(VERSION_OFFICIAL " (official build)")
endif()
