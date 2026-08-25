#include "aistore/metadata/object_layout.hpp"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace {

using aistore::metadata::ChunkRef;
using aistore::metadata::ObjectLayout;

const std::string kChunkA(64, 'a');

const std::string kChunkB(64, 'b');

const std::string kChunkC(64, 'c');

TEST(ObjectLayoutTest, AcceptsEmptyLayout) {
    ObjectLayout layout{{}};

    EXPECT_TRUE(layout.empty());
    EXPECT_EQ(layout.total_size(), 0U);
    EXPECT_TRUE(layout.chunks().empty());
}

TEST(ObjectLayoutTest, AcceptsContiguousOrderedChunks) {
    ObjectLayout layout{
        {
            ChunkRef{
                .chunk_id = kChunkA,
                .offset = 0,
                .size = 4,
            },
            ChunkRef{
                .chunk_id = kChunkB,
                .offset = 4,
                .size = 4,
            },
            ChunkRef{
                .chunk_id = kChunkC,
                .offset = 8,
                .size = 2,
            },
        },
    };

    EXPECT_FALSE(layout.empty());
    EXPECT_EQ(layout.total_size(), 10U);

    ASSERT_EQ(layout.chunks().size(), 3U);

    EXPECT_EQ(layout.chunks()[0].offset, 0U);
    EXPECT_EQ(layout.chunks()[1].offset, 4U);
    EXPECT_EQ(layout.chunks()[2].offset, 8U);
}

TEST(ObjectLayoutTest, RejectsInvalidChunkId) {
    EXPECT_THROW(static_cast<void>(ObjectLayout{
                     {
                         ChunkRef{
                             .chunk_id = "invalid",
                             .offset = 0,
                             .size = 4,
                         },
                     },
                 }),
                 std::invalid_argument);
}

TEST(ObjectLayoutTest, RejectsUppercaseChunkId) {
    EXPECT_THROW(static_cast<void>(ObjectLayout{
                     {
                         ChunkRef{
                             .chunk_id = std::string(64, 'A'),
                             .offset = 0,
                             .size = 4,
                         },
                     },
                 }),
                 std::invalid_argument);
}

TEST(ObjectLayoutTest, RejectsZeroSizedChunk) {
    EXPECT_THROW(static_cast<void>(ObjectLayout{
                     {
                         ChunkRef{
                             .chunk_id = kChunkA,
                             .offset = 0,
                             .size = 0,
                         },
                     },
                 }),
                 std::invalid_argument);
}

TEST(ObjectLayoutTest, RejectsGapBetweenChunks) {
    EXPECT_THROW(static_cast<void>(ObjectLayout{
                     {
                         ChunkRef{
                             .chunk_id = kChunkA,
                             .offset = 0,
                             .size = 4,
                         },
                         ChunkRef{
                             .chunk_id = kChunkB,
                             .offset = 8,
                             .size = 4,
                         },
                     },
                 }),
                 std::invalid_argument);
}

TEST(ObjectLayoutTest, RejectsOverlappingChunks) {
    EXPECT_THROW(static_cast<void>(ObjectLayout{
                     {
                         ChunkRef{
                             .chunk_id = kChunkA,
                             .offset = 0,
                             .size = 4,
                         },
                         ChunkRef{
                             .chunk_id = kChunkB,
                             .offset = 2,
                             .size = 4,
                         },
                     },
                 }),
                 std::invalid_argument);
}

TEST(ObjectLayoutTest, RejectsNonZeroFirstOffset) {
    EXPECT_THROW(static_cast<void>(ObjectLayout{
                     {
                         ChunkRef{
                             .chunk_id = kChunkA,
                             .offset = 4,
                             .size = 4,
                         },
                     },
                 }),
                 std::invalid_argument);
}

TEST(ObjectLayoutTest, RejectsSizeOverflow) {
    EXPECT_THROW(static_cast<void>(ObjectLayout{
                     {
                         ChunkRef{
                             .chunk_id = kChunkA,
                             .offset = 0,
                             .size = std::numeric_limits<std::uint64_t>::max(),
                         },
                         ChunkRef{
                             .chunk_id = kChunkB,
                             .offset = std::numeric_limits<std::uint64_t>::max(),
                             .size = 1,
                         },
                     },
                 }),
                 std::overflow_error);
}

}  // namespace
