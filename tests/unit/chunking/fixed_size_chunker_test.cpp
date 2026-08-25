#include "aistore/chunking/fixed_size_chunker.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using aistore::chunking::ChunkBuffer;
using aistore::chunking::FixedSizeChunker;

std::span<const std::byte> as_bytes(std::string_view text) {
    return std::as_bytes(std::span{text.data(), text.size()});
}

std::string bytes_to_string(const std::vector<std::byte>& bytes) {
    std::string result;
    result.reserve(bytes.size());

    for (const std::byte byte : bytes) {
        result.push_back(static_cast<char>(std::to_integer<unsigned char>(byte)));
    }

    return result;
}

TEST(FixedSizeChunkerTest, UsesFourMiBDefaultChunkSize) {
    FixedSizeChunker chunker;

    EXPECT_EQ(chunker.chunk_size(), 4U * 1024U * 1024U);
}

TEST(FixedSizeChunkerTest, RejectsZeroChunkSize) {
    EXPECT_THROW(static_cast<void>(FixedSizeChunker{0}), std::invalid_argument);
}

TEST(FixedSizeChunkerTest, EmptyInputProducesNoChunks) {
    FixedSizeChunker chunker{4};
    std::vector<ChunkBuffer> chunks;

    chunker.finalize([&chunks](ChunkBuffer chunk) { chunks.push_back(std::move(chunk)); });

    EXPECT_TRUE(chunks.empty());
    EXPECT_EQ(chunker.bytes_received(), 0U);
    EXPECT_TRUE(chunker.finalized());
}

TEST(FixedSizeChunkerTest, HoldsPartialChunkUntilFinalize) {
    FixedSizeChunker chunker{4};
    std::vector<ChunkBuffer> chunks;

    const auto collect = [&chunks](ChunkBuffer chunk) { chunks.push_back(std::move(chunk)); };

    chunker.update(as_bytes("abc"), collect);

    EXPECT_TRUE(chunks.empty());
    EXPECT_EQ(chunker.bytes_received(), 3U);

    chunker.finalize(collect);

    ASSERT_EQ(chunks.size(), 1U);
    EXPECT_EQ(chunks[0].offset, 0U);
    EXPECT_EQ(bytes_to_string(chunks[0].bytes), "abc");
}

TEST(FixedSizeChunkerTest, EmitsExactBoundaryWithoutEmptyFinalChunk) {
    FixedSizeChunker chunker{4};
    std::vector<ChunkBuffer> chunks;

    const auto collect = [&chunks](ChunkBuffer chunk) { chunks.push_back(std::move(chunk)); };

    chunker.update(as_bytes("abcd"), collect);

    ASSERT_EQ(chunks.size(), 1U);
    EXPECT_EQ(chunks[0].offset, 0U);
    EXPECT_EQ(bytes_to_string(chunks[0].bytes), "abcd");

    chunker.finalize(collect);

    EXPECT_EQ(chunks.size(), 1U);
    EXPECT_EQ(chunker.bytes_received(), 4U);
}

TEST(FixedSizeChunkerTest, SplitsMultipleChunksFromSingleUpdate) {
    FixedSizeChunker chunker{4};
    std::vector<ChunkBuffer> chunks;

    const auto collect = [&chunks](ChunkBuffer chunk) { chunks.push_back(std::move(chunk)); };

    chunker.update(as_bytes("abcdefghij"), collect);
    chunker.finalize(collect);

    ASSERT_EQ(chunks.size(), 3U);

    EXPECT_EQ(chunks[0].offset, 0U);
    EXPECT_EQ(bytes_to_string(chunks[0].bytes), "abcd");

    EXPECT_EQ(chunks[1].offset, 4U);
    EXPECT_EQ(bytes_to_string(chunks[1].bytes), "efgh");

    EXPECT_EQ(chunks[2].offset, 8U);
    EXPECT_EQ(bytes_to_string(chunks[2].bytes), "ij");

    EXPECT_EQ(chunker.bytes_received(), 10U);
}

TEST(FixedSizeChunkerTest, PreservesBoundariesAcrossIncrementalUpdates) {
    FixedSizeChunker chunker{4};
    std::vector<ChunkBuffer> chunks;

    const auto collect = [&chunks](ChunkBuffer chunk) { chunks.push_back(std::move(chunk)); };

    chunker.update(as_bytes("ab"), collect);
    chunker.update(as_bytes("cde"), collect);
    chunker.update(as_bytes("fghij"), collect);
    chunker.finalize(collect);

    ASSERT_EQ(chunks.size(), 3U);

    EXPECT_EQ(chunks[0].offset, 0U);
    EXPECT_EQ(bytes_to_string(chunks[0].bytes), "abcd");

    EXPECT_EQ(chunks[1].offset, 4U);
    EXPECT_EQ(bytes_to_string(chunks[1].bytes), "efgh");

    EXPECT_EQ(chunks[2].offset, 8U);
    EXPECT_EQ(bytes_to_string(chunks[2].bytes), "ij");
}

TEST(FixedSizeChunkerTest, RejectsOperationsAfterFinalize) {
    FixedSizeChunker chunker{4};
    std::vector<ChunkBuffer> chunks;

    const auto collect = [&chunks](ChunkBuffer chunk) { chunks.push_back(std::move(chunk)); };

    chunker.update(as_bytes("abc"), collect);
    chunker.finalize(collect);

    EXPECT_THROW(chunker.update(as_bytes("more"), collect), std::logic_error);

    EXPECT_THROW(chunker.finalize(collect), std::logic_error);
}

}  // namespace
