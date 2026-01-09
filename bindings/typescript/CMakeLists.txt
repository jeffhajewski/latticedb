cmake_minimum_required(VERSION 3.15)
cmake_policy(SET CMP0091 NEW)
cmake_policy(SET CMP0042 NEW)

project(lattice_native)

include_directories(${CMAKE_JS_INC})
include_directories(../../include)
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

# Link against the Zig-built shared library
find_library(LATTICE_LIB lattice PATHS ${CMAKE_SOURCE_DIR}/../../zig-out/lib)
target_link_libraries(${PROJECT_NAME} ${CMAKE_JS_LIB} ${LATTICE_LIB})

# macOS specific settings
if(APPLE)
    set(CMAKE_OSX_DEPLOYMENT_TARGET "11.0")
    set_target_properties(${PROJECT_NAME} PROPERTIES
        BUILD_RPATH "@loader_path/../../zig-out/lib"
        INSTALL_RPATH "@loader_path/../../zig-out/lib"
    )
endif()
