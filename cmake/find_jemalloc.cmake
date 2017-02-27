if (ENABLE_JEMALLOC)
	find_package(JeMalloc)

	if (JEMALLOC_INCLUDE_DIR AND JEMALLOC_LIBRARIES)
		include_directories (${JEMALLOC_INCLUDE_DIR})
		set (USE_JEMALLOC 1)
		if (USE_TCMALLOC)
			message (WARNING "Disabling tcmalloc")
			set (USE_TCMALLOC 0)
		endif ()
	endif ()
	message (STATUS "Using jemalloc=${USE_JEMALLOC}: ${JEMALLOC_INCLUDE_DIR} : ${JEMALLOC_LIBRARIES}")
endif ()
