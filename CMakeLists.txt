cmake_minimum_required(VERSION 3.25)

project(
    AIArtifactStore
    VERSION 0.1.0
    LANGUAGES CXX
)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

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

include(CTest)

if(BUILD_TESTING)
    find_package(GTest CONFIG REQUIRED)

    add_executable(
        aistore_unit_tests
        tests/unit/smoke_test.cpp
    )

    target_compile_features(
        aistore_unit_tests
        PRIVATE
        cxx_std_20
    )

    set_target_properties(
        aistore_unit_tests
        PROPERTIES
        CXX_EXTENSIONS OFF
    )

    target_link_libraries(
        aistore_unit_tests
        PRIVATE
        GTest::gtest_main
    )

    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(
            aistore_unit_tests
            PRIVATE
            -Wall
            -Wextra
            -Wpedantic
        )
    endif()

    include(GoogleTest)

    gtest_discover_tests(
        aistore_unit_tests
    )
endif()
