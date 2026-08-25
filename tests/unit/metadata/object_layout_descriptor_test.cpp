#include "aistore/metadata/object_layout_descriptor.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <string>
#include <vector>

namespace {

using aistore::metadata::ChunkingStrategy;
using aistore::metadata::ChunkRef;
using aistore::metadata::Object;
using aistore::metadata::ObjectLayout;
using aistore::metadata::ObjectLayoutDescriptor;

const std::string kObjectA(64, '1');

const std::string kObjectB(64, '2');

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

TEST(ObjectLayoutDescriptorTest, CanonicalizesKnownFixedSizeLayout) {
    const ObjectLayoutDescriptor descriptor{
        Object{
            kObjectA,
            4,
        },
        ChunkingStrategy::FixedSize,
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
              "414953544f52455f4c41594f5554"
              "00000001"
              "11111111111111111111111111111111"
              "11111111111111111111111111111111"
              "01"
              "0000000000000004"
              "0000000000000001"
              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
              "0000000000000000"
              "0000000000000004");

    EXPECT_EQ(descriptor.layout_id(),
              "c369806353f848a91d93e88f63fc99ef"
              "190265ccd7eaba94187aa7d783786be8");
}

TEST(ObjectLayoutDescriptorTest, SameInputsProduceSameLayoutId) {
    const ObjectLayoutDescriptor first{
        Object{
            kObjectA,
            6,
        },
        ChunkingStrategy::FixedSize,
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

    const ObjectLayoutDescriptor second{
        Object{
            kObjectA,
            6,
        },
        ChunkingStrategy::FixedSize,
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

    EXPECT_EQ(first.layout_id(), second.layout_id());
}

TEST(ObjectLayoutDescriptorTest, DifferentLayoutsForSameObjectHaveDifferentLayoutIds) {
    const Object object{
        kObjectA,
        6,
    };

    const ObjectLayoutDescriptor first{
        object,
        ChunkingStrategy::FixedSize,
        ObjectLayout{
            {
                ChunkRef{
                    .chunk_id = kChunkA,
                    .offset = 0,
                    .size = 6,
                },
            },
        },
    };

    const ObjectLayoutDescriptor second{
        object,
        ChunkingStrategy::FixedSize,
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

    EXPECT_EQ(first.object_id(), second.object_id());

    EXPECT_NE(first.layout_id(), second.layout_id());
}

TEST(ObjectLayoutDescriptorTest, RejectsObjectLayoutSizeMismatch) {
    EXPECT_THROW(static_cast<void>(ObjectLayoutDescriptor{
                     Object{
                         kObjectA,
                         5,
                     },
                     ChunkingStrategy::FixedSize,
                     ObjectLayout{
                         {
                             ChunkRef{
                                 .chunk_id = kChunkA,
                                 .offset = 0,
                                 .size = 4,
                             },
                         },
                     },
                 }),
                 std::invalid_argument);
}

TEST(ObjectLayoutDescriptorTest, DifferentObjectIdentityChangesLayoutId) {
    const ObjectLayout layout{
        {
            ChunkRef{
                .chunk_id = kChunkA,
                .offset = 0,
                .size = 4,
            },
        },
    };

    const ObjectLayoutDescriptor first{
        Object{
            kObjectA,
            4,
        },
        ChunkingStrategy::FixedSize,
        layout,
    };

    const ObjectLayoutDescriptor second{
        Object{
            kObjectB,
            4,
        },
        ChunkingStrategy::FixedSize,
        layout,
    };

    EXPECT_NE(first.layout_id(), second.layout_id());
}

}  // namespace
