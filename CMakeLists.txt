cmake_minimum_required(VERSION 3.5.0)
set(TARGET_NAME odbc_scanner)
project(${TARGET_NAME})

# Find standard ODBC libraries
find_path(ODBC_INCLUDE_DIR sql.h)
find_library(ODBC_LIBRARY NAMES odbc odbc32)

if(NOT ODBC_INCLUDE_DIR OR NOT ODBC_LIBRARY)
    message(FATAL_ERROR "ODBC development libraries not found. Please install unixODBC or equivalent.")
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)
include_directories(${ODBC_INCLUDE_DIR})

add_subdirectory(src)

set(PARAMETERS "-no-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ALL_OBJECT_FILES})

# Loadable binary
target_include_directories(${TARGET_NAME}_loadable_extension
                           PRIVATE include ${ODBC_INCLUDE_DIR})
target_link_libraries(${TARGET_NAME}_loadable_extension ${ODBC_LIBRARY})
