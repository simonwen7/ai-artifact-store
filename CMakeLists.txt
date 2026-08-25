cmake_minimum_required(VERSION 3.25)

project(
    AIArtifactStore
    VERSION 0.1.0
    LANGUAGES CXX
)

add_executable(
    aistore
    apps/aistore-cli/main.cpp
)

target_compile_features(
    aistore
    PRIVATE
    cxx_std_20
)

set_target_properties(
    aistore
    PROPERTIES
    CXX_EXTENSIONS OFF
)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(
        aistore
        PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )
endif()
