if (NOT READLINE_PATHS)
	set (READLINE_PATHS "/usr/local/opt/readline/lib")
	if (USE_STATIC_LIBRARIES)
		find_library (READLINE_LIB NAMES libreadline.a PATHS ${READLINE_PATHS})
	else ()
		find_library (READLINE_LIB NAMES readline PATHS ${READLINE_PATHS})
	endif ()

	if (USE_STATIC_LIBRARIES)
		find_library (TERMCAP_LIB NAMES libtermcap.a termcap)
	else ()
		find_library (TERMCAP_LIB NAMES termcap)
	endif ()

	if (USE_STATIC_LIBRARIES)
		find_library (EDIT_LIB NAMES libedit.a)
	else ()
		find_library (EDIT_LIB NAMES edit)
	endif ()
	if (USE_STATIC_LIBRARIES)
		find_library (CURSES_LIB NAMES libcurses.a)
	else ()
		find_library (CURSES_LIB NAMES curses)
	endif ()

	if (READLINE_LIB)
		set(READLINE_INCLUDE_PATHS "/usr/local/opt/readline/include")
		find_path (READLINE_INCLUDE_DIR NAMES readline.h PATH_SUFFIXES readline PATHS ${READLINE_INCLUDE_PATHS})
		if (INE_INCLUDE_DIR)
			include_directories (${READLINE_INCLUDE_DIR})
		endif ()
		add_definitions (-D USE_READLINE)
		set (LINE_EDITING_LIBS ${READLINE_LIB} ${TERMCAP_LIB})
		message (STATUS "Using line editing libraries: ${LINE_EDITING_LIBS}")
	elseif (EDIT_LIB)
		add_definitions (-D USE_LIBEDIT)
		set (LINE_EDITING_LIBS ${EDIT_LIB} ${CURSES_LIB} ${TERMCAP_LIB})
		message (STATUS "Using line editing libraries: ${LINE_EDITING_LIBS}")
	else ()
		message (STATUS "Not using any library for line editing.")
	endif ()
	
endif ()
