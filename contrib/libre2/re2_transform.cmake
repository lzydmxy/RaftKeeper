file (READ ${SOURCE_FILENAME} CONTENT)
string (REGEX REPLACE "using re2::RE2;" "" CONTENT "${CONTENT}")
string (REGEX REPLACE "using re2::LazyRE2;" "" CONTENT "${CONTENT}")
string (REGEX REPLACE "namespace re2" "namespace re2_st" CONTENT "${CONTENT}")
string (REGEX REPLACE "re2::" "re2_st::" CONTENT "${CONTENT}")
string (REGEX REPLACE "\"re2/" "\"re2_st/" CONTENT "${CONTENT}")
string (REGEX REPLACE "(.\\*?_H)" "\\1_ST" CONTENT "${CONTENT}")
file (WRITE ${TARGET_FILENAME} "${CONTENT}")
