#include "aistore/chunking/fastcdc_chunker.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <set>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "aistore/chunking/fixed_size_chunker.hpp"
#include "aistore/hashing/sha256.hpp"

namespace {

using aistore::chunking::ChunkBuffer;
using aistore::chunking::FastCdcChunker;
using aistore::chunking::FixedSizeChunker;
using aistore::hashing::Sha256;

struct ChunkBoundary {
    std::uint64_t offset;
    std::size_t size;
};

std::vector<ChunkBuffer> chunk_all(FastCdcChunker& chunker, std::span<const std::byte> data) {
    std::vector<ChunkBuffer> chunks;

    const auto collect = [&chunks](ChunkBuffer chunk) { chunks.push_back(std::move(chunk)); };

    chunker.update(data, collect);
    chunker.finalize(collect);

    return chunks;
}

std::vector<std::byte> make_golden_fixture() {
    std::vector<std::byte> fixture;
    fixture.reserve(4096U);

    for (std::size_t index = 0; index < 4096U; ++index) {
        fixture.push_back(static_cast<std::byte>(index % 251U));
    }

    return fixture;
}

std::vector<std::byte> make_xorshift_payload() {
    std::vector<std::byte> bytes;
    bytes.reserve(64U * 1024U);

    std::uint64_t state = 0xDEADBEEFCAFEBABEULL;

    for (std::size_t index = 0; index < 64U * 1024U; ++index) {
        state ^= state << 13U;
        state ^= state >> 7U;
        state ^= state << 17U;
        bytes.push_back(static_cast<std::byte>(state & 0xFFU));
    }

    return bytes;
}

std::vector<std::byte> insert_prefix(const std::vector<std::byte>& base) {
    std::vector<std::byte> prefixed;
    prefixed.reserve(base.size() + 16U);

    for (std::size_t index = 0; index < 16U; ++index) {
        prefixed.push_back(static_cast<std::byte>(0xA0U + index));
    }

    prefixed.insert(prefixed.end(), base.begin(), base.end());
    return prefixed;
}

std::string hash_chunk_bytes(const std::vector<std::byte>& bytes) {
    Sha256 hasher;
    hasher.update(bytes);
    const Sha256::Digest digest = hasher.finalize();
    return std::string{reinterpret_cast<const char*>(digest.data()), digest.size()};
}

std::set<std::string> collect_chunk_hashes(std::span<const std::byte> data, bool use_fastcdc) {
    std::set<std::string> hashes;

    if (use_fastcdc) {
        FastCdcChunker chunker{512U, 1024U, 2048U};
        const auto collect = [&hashes](ChunkBuffer chunk) { hashes.insert(hash_chunk_bytes(chunk.bytes)); };

        chunker.update(data, collect);
        chunker.finalize(collect);
        return hashes;
    }

    FixedSizeChunker chunker{1024U};
    const auto collect = [&hashes](ChunkBuffer chunk) { hashes.insert(hash_chunk_bytes(chunk.bytes)); };

    chunker.update(data, collect);
    chunker.finalize(collect);
    return hashes;
}

std::size_t count_shared_hashes(const std::set<std::string>& left, const std::set<std::string>& right) {
    std::size_t shared = 0;

    for (const std::string& hash : left) {
        if (right.contains(hash)) {
            ++shared;
        }
    }

    return shared;
}

}  // namespace

TEST(FastCdcChunkerTest, UsesFrozenDefaultParameters) {
    FastCdcChunker chunker;

    EXPECT_EQ(chunker.min_chunk_size(), 2U * 1024U * 1024U);
    EXPECT_EQ(chunker.avg_chunk_size(), 4U * 1024U * 1024U);
    EXPECT_EQ(chunker.max_chunk_size(), 8U * 1024U * 1024U);
}

TEST(FastCdcChunkerTest, RejectsInvalidParameters) {
    EXPECT_THROW(static_cast<void>(FastCdcChunker{32U, 128U, 256U}), std::invalid_argument);
    EXPECT_THROW(static_cast<void>(FastCdcChunker{64U, 100U, 256U}), std::invalid_argument);
    EXPECT_THROW(static_cast<void>(FastCdcChunker{256U, 128U, 256U}), std::invalid_argument);
    EXPECT_THROW(static_cast<void>(FastCdcChunker{64U, 256U, 128U}), std::invalid_argument);
    EXPECT_THROW(static_cast<void>(FastCdcChunker{64U, 128U, 9U * 1024U * 1024U}), std::invalid_argument);
}

TEST(FastCdcChunkerTest, EmptyInputProducesNoChunks) {
    FastCdcChunker chunker{64U, 128U, 256U};
    std::vector<ChunkBuffer> chunks;

    chunker.finalize([&chunks](ChunkBuffer chunk) { chunks.push_back(std::move(chunk)); });

    EXPECT_TRUE(chunks.empty());
    EXPECT_EQ(chunker.bytes_received(), 0U);
    EXPECT_TRUE(chunker.finalized());
}

TEST(FastCdcChunkerTest, EmitsFinalChunkBelowMinimumAtEof) {
    FastCdcChunker chunker{64U, 128U, 256U};
    std::vector<ChunkBuffer> chunks;

    const auto collect = [&chunks](ChunkBuffer chunk) { chunks.push_back(std::move(chunk)); };

    chunker.update(std::span<const std::byte>{}, collect);
    chunker.update(std::span<const std::byte>{reinterpret_cast<const std::byte*>("short"), 5U}, collect);
    chunker.finalize(collect);

    ASSERT_EQ(chunks.size(), 1U);
    EXPECT_EQ(chunks[0].offset, 0U);
    EXPECT_EQ(chunks[0].bytes.size(), 5U);
}

TEST(FastCdcChunkerTest, PreservesBoundariesAcrossIncrementalUpdates) {
    const std::vector<std::byte> fixture = make_golden_fixture();

    FastCdcChunker bulk_chunker{64U, 128U, 256U};
    const std::vector<ChunkBuffer> bulk_chunks = chunk_all(bulk_chunker, fixture);

    FastCdcChunker incremental_chunker{64U, 128U, 256U};
    std::vector<ChunkBuffer> incremental_chunks;

    const auto collect = [&incremental_chunks](ChunkBuffer chunk) { incremental_chunks.push_back(std::move(chunk)); };

    for (std::size_t index = 0; index < fixture.size(); index += 17U) {
        const std::size_t piece_size = std::min<std::size_t>(17U, fixture.size() - index);
        incremental_chunker.update(std::span<const std::byte>{fixture}.subspan(index, piece_size), collect);
    }

    incremental_chunker.finalize(collect);

    ASSERT_EQ(incremental_chunks.size(), bulk_chunks.size());

    for (std::size_t index = 0; index < bulk_chunks.size(); ++index) {
        EXPECT_EQ(incremental_chunks[index].offset, bulk_chunks[index].offset);
        EXPECT_EQ(incremental_chunks[index].bytes, bulk_chunks[index].bytes);
    }
}

TEST(FastCdcChunkerTest, NonFinalChunksRespectMinAndMaxBounds) {
    const std::vector<std::byte> fixture = make_golden_fixture();
    FastCdcChunker chunker{64U, 128U, 256U};
    const std::vector<ChunkBuffer> chunks = chunk_all(chunker, fixture);

    ASSERT_GT(chunks.size(), 1U);

    for (std::size_t index = 0; index + 1U < chunks.size(); ++index) {
        EXPECT_GE(chunks[index].bytes.size(), 64U);
        EXPECT_LE(chunks[index].bytes.size(), 256U);
    }
}

TEST(FastCdcChunkerTest, ForcesBoundaryAtMaximumSize) {
    std::vector<std::byte> fixture;
    fixture.reserve(300U);

    for (std::size_t index = 0; index < 300U; ++index) {
        fixture.push_back(static_cast<std::byte>(0x5AU));
    }

    FastCdcChunker chunker{64U, 128U, 256U};
    const std::vector<ChunkBuffer> chunks = chunk_all(chunker, fixture);

    ASSERT_GE(chunks.size(), 2U);
    EXPECT_EQ(chunks[0].bytes.size(), 256U);
    EXPECT_EQ(chunks[1].offset, 256U);
}

TEST(FastCdcChunkerTest, ProducesDeterministicGoldenBoundaries) {
    const std::vector<std::byte> fixture = make_golden_fixture();
    FastCdcChunker chunker{64U, 128U, 256U};
    const std::vector<ChunkBuffer> chunks = chunk_all(chunker, fixture);

    const std::array<ChunkBoundary, 17> expected = {{
        {0U, 185U},
        {185U, 163U},
        {348U, 251U},
        {599U, 251U},
        {850U, 251U},
        {1101U, 251U},
        {1352U, 251U},
        {1603U, 251U},
        {1854U, 251U},
        {2105U, 251U},
        {2356U, 251U},
        {2607U, 251U},
        {2858U, 251U},
        {3109U, 251U},
        {3360U, 251U},
        {3611U, 251U},
        {3862U, 234U},
    }};

    ASSERT_EQ(chunks.size(), expected.size());

    for (std::size_t index = 0; index < expected.size(); ++index) {
        EXPECT_EQ(chunks[index].offset, expected[index].offset);
        EXPECT_EQ(chunks[index].bytes.size(), expected[index].size);
    }
}

TEST(FastCdcChunkerTest, RejectsOperationsAfterFinalize) {
    FastCdcChunker chunker{64U, 128U, 256U};
    std::vector<ChunkBuffer> chunks;

    const auto collect = [&chunks](ChunkBuffer chunk) { chunks.push_back(std::move(chunk)); };

    chunker.update(std::span<const std::byte>{reinterpret_cast<const std::byte*>("abc"), 3U}, collect);
    chunker.finalize(collect);

    EXPECT_THROW(chunker.update(std::span<const std::byte>{reinterpret_cast<const std::byte*>("more"), 4U}, collect),
                 std::logic_error);
    EXPECT_THROW(chunker.finalize(collect), std::logic_error);
}

TEST(FastCdcChunkerTest, PreservesSubstantiallyMoreChunksAfterPrefixInsertionThanFixedSize) {
    const std::vector<std::byte> base_payload = make_xorshift_payload();
    const std::vector<std::byte> prefixed_payload = insert_prefix(base_payload);

    const std::set<std::string> base_fast_hashes = collect_chunk_hashes(base_payload, true);
    const std::set<std::string> prefixed_fast_hashes = collect_chunk_hashes(prefixed_payload, true);
    const std::set<std::string> base_fixed_hashes = collect_chunk_hashes(base_payload, false);
    const std::set<std::string> prefixed_fixed_hashes = collect_chunk_hashes(prefixed_payload, false);

    const std::size_t fast_shared = count_shared_hashes(base_fast_hashes, prefixed_fast_hashes);
    const std::size_t fixed_shared = count_shared_hashes(base_fixed_hashes, prefixed_fixed_hashes);

    constexpr double kMinimumFastSharedFraction = 0.40;
    constexpr std::size_t kFixedSizeMultiplier = 2U;

    EXPECT_GE(fast_shared, static_cast<std::size_t>(kMinimumFastSharedFraction * base_fast_hashes.size()));
    EXPECT_GT(fast_shared, fixed_shared * kFixedSizeMultiplier);
    EXPECT_GT(fast_shared, fixed_shared);
}
