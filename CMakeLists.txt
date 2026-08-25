cmake_minimum_required(VERSION 3.25)

project(
    AIArtifactStore
    VERSION 0.1.0
    LANGUAGES CXX
)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(OpenSSL REQUIRED)

add_library(
    aistore_hashing
    src/hashing/sha256.cpp
)

target_include_directories(
    aistore_hashing
    PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_compile_features(
    aistore_hashing
    PUBLIC
    cxx_std_20
)

set_target_properties(
    aistore_hashing
    PROPERTIES
    CXX_EXTENSIONS OFF
)

target_link_libraries(
    aistore_hashing
    PRIVATE
    OpenSSL::Crypto
)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(
        aistore_hashing
        PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )
endif()

add_library(
    aistore_chunking
    src/chunking/fixed_size_chunker.cpp
)

target_include_directories(
    aistore_chunking
    PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_compile_features(
    aistore_chunking
    PUBLIC
    cxx_std_20
)

set_target_properties(
    aistore_chunking
    PROPERTIES
    CXX_EXTENSIONS OFF
)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(
        aistore_chunking
        PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )
endif()

add_library(
    aistore_storage
    src/storage/local_chunk_store.cpp
)

target_include_directories(
    aistore_storage
    PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_compile_features(
    aistore_storage
    PUBLIC
    cxx_std_20
)

set_target_properties(
    aistore_storage
    PROPERTIES
    CXX_EXTENSIONS OFF
)

target_link_libraries(
    aistore_storage
    PRIVATE
    aistore_hashing
)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(
        aistore_storage
        PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )
endif()

add_library(
    aistore_metadata
    src/metadata/object_layout.cpp
)

target_include_directories(
    aistore_metadata
    PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)

target_compile_features(
    aistore_metadata
    PUBLIC
    cxx_std_20
)

set_target_properties(
    aistore_metadata
    PROPERTIES
    CXX_EXTENSIONS OFF
)

if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(
        aistore_metadata
        PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )
endif()

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
    include(GoogleTest)

    function(aistore_add_unit_test target_name source_file)
        add_executable(
            ${target_name}
            ${source_file}
        )

        target_compile_features(
            ${target_name}
            PRIVATE
            cxx_std_20
        )

        set_target_properties(
            ${target_name}
            PROPERTIES
            CXX_EXTENSIONS OFF
        )

        target_link_libraries(
            ${target_name}
            PRIVATE
            GTest::gtest_main
        )

        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(
                ${target_name}
                PRIVATE
                -Wall
                -Wextra
                -Wpedantic
            )
        endif()

        gtest_discover_tests(
            ${target_name}
        )
    endfunction()

    aistore_add_unit_test(
        aistore_smoke_tests
        tests/unit/smoke_test.cpp
    )

    aistore_add_unit_test(
        aistore_hashing_tests
        tests/unit/hashing/sha256_test.cpp
    )

    target_link_libraries(
        aistore_hashing_tests
        PRIVATE
        aistore_hashing
    )

    aistore_add_unit_test(
        aistore_chunking_tests
        tests/unit/chunking/fixed_size_chunker_test.cpp
    )

    target_link_libraries(
        aistore_chunking_tests
        PRIVATE
        aistore_chunking
    )

    aistore_add_unit_test(
        aistore_storage_tests
        tests/unit/storage/local_chunk_store_test.cpp
    )

    target_link_libraries(
        aistore_storage_tests
        PRIVATE
        aistore_storage
    )

    aistore_add_unit_test(
        aistore_metadata_tests
        tests/unit/metadata/object_layout_test.cpp
    )

    target_link_libraries(
        aistore_metadata_tests
        PRIVATE
        aistore_metadata
    )

    aistore_add_unit_test(
        aistore_local_pipeline_tests
        tests/integration/local_chunk_pipeline_test.cpp
    )

    target_link_libraries(
        aistore_local_pipeline_tests
        PRIVATE
        aistore_chunking
        aistore_storage
    )
endif()
