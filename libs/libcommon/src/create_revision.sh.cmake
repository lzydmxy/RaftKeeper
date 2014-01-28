#!/bin/bash
mkdir -p ${CMAKE_CURRENT_BINARY_DIR}/src
echo "#ifndef SVN_REVISION" > ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h
echo -n "#define SVN_REVISION " >> ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h

cd ${CMAKE_CURRENT_SOURCE_DIR};

echo && (LC_ALL=C svn info ${PROJECT_SOURCE_DIR}/ 2>/dev/null || echo Revision 1) | grep Revision | cut -d " " -f 2 >> ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h;

echo -n "#define SVN_PATH " >> ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h
SVN_PATH="$((LC_ALL=C svn info ./ 2>/dev/null || echo URL: unknown) | grep URL | cut -d " " -f 2 | sed "s/https:\/\/███████████.yandex-team.ru\/conv\///g")"
echo '"'$SVN_PATH'"' >> ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h;

echo "#endif" >> ${CMAKE_CURRENT_BINARY_DIR}/src/revision.h
