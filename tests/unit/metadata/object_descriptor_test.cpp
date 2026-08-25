#include "aistore/metadata/object_descriptor.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <string>
#include <vector>

namespace {

using aistore::metadata::ChunkRef;
using aistore::metadata::ObjectDescriptor;
using aistore::metadata::ObjectLayout;

const std::string kChunkA(64, 'a');

const std::string kChunkB(64, 'b');

std::string bytes_to_hex(const std::vector<std::byte>& bytes) {
    constexpr std::array<char, 16> kHexDigits{
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    };

    std::string result;
    result.reserve(bytes.size() * 2);

    for (const std::byte byte : bytes) {
        const auto value = std::to_integer<unsigned int>(byte);

        result.push_back(kHexDigits[(value >> 4U) & 0x0FU]);

        result.push_back(kHexDigits[value & 0x0FU]);
    }

    return result;
}

TEST(ObjectDescriptorTest, CanonicalizesEmptyLayout) {
    ObjectDescriptor descriptor{
        ObjectLayout{{}},
    };

    EXPECT_EQ(bytes_to_hex(descriptor.canonical_bytes()),
              "414953544f524531"
              "00000001"
              "0000000000000000");

    EXPECT_EQ(descriptor.object_id(),
              "42c833d3e81c76064f5de52eff6cef2b"
              "b7cb323618b38728f39109a89e25fc40");
}

TEST(ObjectDescriptorTest, CanonicalizesKnownLayout) {
    ObjectDescriptor descriptor{
        ObjectLayout{
            {
                ChunkRef{
                    .chunk_id = kChunkA,
                    .offset = 0,
                    .size = 4,
                },
            },
        },
    };

    EXPECT_EQ(bytes_to_hex(descriptor.canonical_bytes()),
              "414953544f524531"
              "00000001"
              "0000000000000001"
              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
              "0000000000000000"
              "0000000000000004");

    EXPECT_EQ(descriptor.object_id(),
              "0d45c4d4dcfca01c4e432261e7a2fc38"
              "28dedc195f3bf7f08cb43c0d04c26b41");
}

TEST(ObjectDescriptorTest, SameLayoutProducesSameObjectId) {
    ObjectDescriptor first{
        ObjectLayout{
            {
                ChunkRef{
                    .chunk_id = kChunkA,
                    .offset = 0,
                    .size = 4,
                },
                ChunkRef{
                    .chunk_id = kChunkB,
                    .offset = 4,
                    .size = 2,
                },
            },
        },
    };

    ObjectDescriptor second{
        ObjectLayout{
            {
                ChunkRef{
                    .chunk_id = kChunkA,
                    .offset = 0,
                    .size = 4,
                },
                ChunkRef{
                    .chunk_id = kChunkB,
                    .offset = 4,
                    .size = 2,
                },
            },
        },
    };

    EXPECT_EQ(first.canonical_bytes(), second.canonical_bytes());

    EXPECT_EQ(first.object_id(), second.object_id());
}

TEST(ObjectDescriptorTest, DifferentChunkIdChangesObjectId) {
    ObjectDescriptor first{
        ObjectLayout{
            {
                ChunkRef{
                    .chunk_id = kChunkA,
                    .offset = 0,
                    .size = 4,
                },
            },
        },
    };

    ObjectDescriptor second{
        ObjectLayout{
            {
                ChunkRef{
                    .chunk_id = kChunkB,
                    .offset = 0,
                    .size = 4,
                },
            },
        },
    };

    EXPECT_NE(first.object_id(), second.object_id());
}

TEST(ObjectDescriptorTest, DifferentChunkSizeChangesObjectId) {
    ObjectDescriptor first{
        ObjectLayout{
            {
                ChunkRef{
                    .chunk_id = kChunkA,
                    .offset = 0,
                    .size = 4,
                },
            },
        },
    };

    ObjectDescriptor second{
        ObjectLayout{
            {
                ChunkRef{
                    .chunk_id = kChunkA,
                    .offset = 0,
                    .size = 5,
                },
            },
        },
    };

    EXPECT_NE(first.object_id(), second.object_id());
}

}  // namespace
