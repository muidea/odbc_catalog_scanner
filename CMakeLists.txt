cmake_minimum_required(VERSION 3.5.0)
set(TARGET_NAME odbc_scanner)
project(${TARGET_NAME})

# Platform-specific dependency handling
if(WIN32)
    # Windows: Use system ODBC SDK
    set(ODBC_LIBRARY odbc32)
    set(ODBC_INCLUDE_DIR "")
    message(STATUS "Using Windows ODBC SDK")
    message(STATUS "Note: ODBC drivers (like FreeTDS) can be configured at runtime")
else()
    # Linux: vcpkg's unixodbc will be configured automatically via toolchain
    # Note: find_package(UnixODBC) is provided by vcpkg
    # For now, we rely on vcpkg setting up the paths correctly
    message(STATUS "Using vcpkg unixodbc")
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
