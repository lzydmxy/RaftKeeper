if (NOT USE_INTERNAL_ZLIB_LIBRARY)
	find_package (ZLIB)

	if (ZLIB_FOUND)
		include_directories (${ZLIB_INCLUDE_DIRS})
	endif ()
endif ()

if (NOT ZLIB_FOUND)
	set (ZLIB_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libzlib-ng")
	include_directories (BEFORE ${ZLIB_INCLUDE_DIR})
	if (USE_STATIC_LIBRARIES)
		set (ZLIB_LIBRARIES zlibstatic)
	else ()
		set (ZLIB_LIBRARIES zlib)
	endif ()
endif ()

message (STATUS "Using zlib: ${ZLIB_INCLUDE_DIR} : ${ZLIB_LIBRARIES}")
