cmake_minimum_required(VERSION 3.5.0)
set(TARGET_NAME odbc_catalog_scanner)
project(${TARGET_NAME})

# Platform-specific dependency handling (Windows and Linux)
if(WIN32)
    # Windows: Use system ODBC SDK (odbc32.lib)
    set(ODBC_LIBRARY odbc32)
    set(ODBC_INCLUDE_DIR "")
    message(STATUS "ODBC: Using Windows ODBC SDK")
else()
    # Linux: unixODBC (libodbc.so); vcpkg or system package provides headers and -lodbc
    set(ODBC_LIBRARY odbc)
    if(NOT ODBC_INCLUDE_DIR)
        set(ODBC_INCLUDE_DIR "")
    endif()
    message(STATUS "ODBC: Using unixODBC (Linux)")
endif()

set(EXTENSION_NAME ${TARGET_NAME}_extension)

# Include directories
if(ODBC_INCLUDE_DIR)
    include_directories(BEFORE ${ODBC_INCLUDE_DIR})
endif()

add_subdirectory(src)

set(PARAMETERS "-no-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${ALL_OBJECT_FILES})

# Loadable binary - include directories
target_include_directories(${TARGET_NAME}_loadable_extension
                           PRIVATE include)

if(ODBC_INCLUDE_DIR)
    target_include_directories(${TARGET_NAME}_loadable_extension
                               PRIVATE ${ODBC_INCLUDE_DIR})
endif()

# Link libraries
target_link_libraries(${TARGET_NAME}_loadable_extension
                      ${ODBC_LIBRARY})

option(ODBC_CATALOG_BUILD_RESOURCE_TESTS "构建无数据库资源生命周期测试" OFF)
if(ODBC_CATALOG_BUILD_RESOURCE_TESTS)
    add_executable(odbc_relational_reader_test
                   test/odbc_relational_reader_test.cpp src/odbc_relational_reader.cpp)
    target_include_directories(odbc_relational_reader_test PRIVATE src/include)
    target_link_libraries(odbc_relational_reader_test duckdb)
    add_executable(odbc_connection_resource_test
                   test/odbc_connection_resource_test.cpp src/odbc_utils.cpp)
    target_include_directories(odbc_connection_resource_test PRIVATE src/include)
    target_link_libraries(odbc_connection_resource_test duckdb ${ODBC_LIBRARY})
    add_executable(odbc_result_resource_test
                   test/odbc_result_resource_test.cpp src/odbc_result.cpp src/odbc_types.cpp)
    target_include_directories(odbc_result_resource_test PRIVATE src/include)
    target_link_libraries(odbc_result_resource_test duckdb ${ODBC_LIBRARY})
endif()
