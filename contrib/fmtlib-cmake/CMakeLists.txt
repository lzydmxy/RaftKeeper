set (SRCS
        ../fmtlib/src/fmt.cc
    ../fmtlib/src/format.cc
    ../fmtlib/src/os.cc

    ../fmtlib/include/fmt/chrono.h
    ../fmtlib/include/fmt/color.h
    ../fmtlib/include/fmt/compile.h
    ../fmtlib/include/fmt/core.h
    ../fmtlib/include/fmt/format.h
    ../fmtlib/include/fmt/format-inl.h
    ../fmtlib/include/fmt/os.h
    ../fmtlib/include/fmt/ostream.h
    ../fmtlib/include/fmt/printf.h
    ../fmtlib/include/fmt/ranges.h
)

add_library(fmt ${SRCS})
target_include_directories(fmt SYSTEM PUBLIC ../fmtlib/include)
