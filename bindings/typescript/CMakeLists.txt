cmake_minimum_required(VERSION 3.15)
cmake_policy(SET CMP0091 NEW)
cmake_policy(SET CMP0042 NEW)

project(lattice_native)

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
    pkg_check_modules(LATTICE_PC QUIET lattice)
endif()

set(LATTICE_HINT_INCLUDE_DIRS
    ${CMAKE_SOURCE_DIR}/../../include
)
set(LATTICE_HINT_LIBRARY_DIRS
    ${CMAKE_SOURCE_DIR}/../../zig-out/lib
)

if(DEFINED ENV{LATTICE_PREFIX})
    list(PREPEND LATTICE_HINT_INCLUDE_DIRS "$ENV{LATTICE_PREFIX}/include")
    list(PREPEND LATTICE_HINT_LIBRARY_DIRS "$ENV{LATTICE_PREFIX}/lib")
endif()

if(LATTICE_PC_FOUND)
    list(APPEND LATTICE_HINT_INCLUDE_DIRS ${LATTICE_PC_INCLUDE_DIRS})
    list(APPEND LATTICE_HINT_LIBRARY_DIRS ${LATTICE_PC_LIBRARY_DIRS})
endif()

include_directories(${CMAKE_JS_INC})
include_directories(${CMAKE_SOURCE_DIR}/node_modules/node-addon-api)

file(GLOB SOURCE_FILES "src/native/addon.cpp")

add_library(${PROJECT_NAME} SHARED ${SOURCE_FILES} ${CMAKE_JS_SRC})
set_target_properties(${PROJECT_NAME} PROPERTIES PREFIX "" SUFFIX ".node")

target_compile_definitions(${PROJECT_NAME} PRIVATE NAPI_DISABLE_CPP_EXCEPTIONS)

# Disable C++ features that cause ___dso_handle symbol conflicts with Zig-built libraries
target_compile_options(${PROJECT_NAME} PRIVATE
    -fno-exceptions
    -fno-rtti
    -fno-use-cxa-atexit
)

# Locate the public header and shared library
find_path(LATTICE_INCLUDE_DIR lattice.h HINTS ${LATTICE_HINT_INCLUDE_DIRS})
find_library(LATTICE_LIB lattice HINTS ${LATTICE_HINT_LIBRARY_DIRS})

if(NOT LATTICE_INCLUDE_DIR OR NOT LATTICE_LIB)
    message(FATAL_ERROR "Could not find Lattice headers/library. Set LATTICE_PREFIX, configure PKG_CONFIG_PATH, or build with zig build shared.")
endif()

include_directories(${LATTICE_INCLUDE_DIR})
target_link_libraries(${PROJECT_NAME} ${CMAKE_JS_LIB} ${LATTICE_LIB})

get_filename_component(LATTICE_LIB_DIR "${LATTICE_LIB}" DIRECTORY)

# macOS specific settings
if(APPLE)
    set(CMAKE_OSX_DEPLOYMENT_TARGET "11.0")
    set_target_properties(${PROJECT_NAME} PROPERTIES
        BUILD_RPATH "${LATTICE_LIB_DIR}"
        INSTALL_RPATH "${LATTICE_LIB_DIR}"
    )
endif()
