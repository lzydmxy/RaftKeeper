# wget https://raw.githubusercontent.com/openssh/openssh-portable/master/openbsd-compat/readpassphrase.c
# wget https://raw.githubusercontent.com/openssh/openssh-portable/master/openbsd-compat/readpassphrase.h

add_library(readpassphrase readpassphrase.c)

set_target_properties(readpassphrase PROPERTIES LINKER_LANGUAGE C)
target_compile_options(readpassphrase PRIVATE -Wno-unused-result -Wno-reserved-id-macro -Wno-reserved-identifier -Wno-disabled-macro-expansion -Wno-sign-conversion -Wno-switch-default)
target_include_directories(readpassphrase PUBLIC .)
