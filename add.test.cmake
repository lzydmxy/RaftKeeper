# добавляем вывод программы при ошибке теста
enable_testing()
if (CMAKE_CONFIGURATION_TYPES)
	add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND}
		--force-new-ctest-process --output-on-failure
		--build-config "$<CONFIGURATION>")
else()
	add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND}
		--force-new-ctest-process --output-on-failure)
endif()

macro (add_check target)
	add_test(NAME test_${target} COMMAND ${target} WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
	add_dependencies(check ${target})
endmacro (add_check)