#!/usr/bin/env bash

# Parse src/Common/ErrorCodes.cpp
# And generate src/Functions/errorCodes.generated.cpp
# For errorCode() function.
#
# Later it may contain some description of the error.

set -e
set -o pipefail

CUR_DIR="$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")"
ERROR_CODES_IN_FILE=${ERROR_CODES_IN_FILE=$CUR_DIR/../Common/ErrorCodes.cpp}
ERROR_CODES_OUT_FILE=${ERROR_CODES_OUT_FILE=$CUR_DIR/errorCodes.generated.cpp}
CXX=${CXX=g++}

trap 'rm -f $TMP_FILE' EXIT
TMP_FILE="$(mktemp clichouse_generate_errorCodes_XXXXXXXX.cpp)"

function parse_for_errorCodeToName()
{
    # This is the simplest command that can be written to parse the file
    # And it does not requires any extra tools and works everywhere where you have g++/clang++
    $CXX -E "$ERROR_CODES_IN_FILE" | {
        awk -F '[ =;]*' '/extern const int / { printf("        case %s: return std::string_view(\"%s\");\n", $(NF-1), $(NF-2)); }'
    }
}

function generate_errorCodeToName()
{
    cat <<EOL
// autogenerated by ${BASH_SOURCE[0]}

#include <string_view>

std::string_view errorCodeToName(int code)
{
    switch (code)
    {
        case 0: return std::string_view("OK");
$(parse_for_errorCodeToName)
        default: return std::string_view("");
    }
};

EOL
}

function main()
{
    generate_errorCodeToName > "$TMP_FILE"

    if [[ ! -e $ERROR_CODES_OUT_FILE ]]; then
        cp -a "$TMP_FILE" "$ERROR_CODES_OUT_FILE"
    fi
    # update it only if it differs, to avoid costly recompilation
    if ! diff -q "$TMP_FILE" "$ERROR_CODES_OUT_FILE"; then
        cp -a "$TMP_FILE" "$ERROR_CODES_OUT_FILE"
    fi
}
main "$@"
