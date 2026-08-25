#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "aistore/chunking/fixed_size_chunker.hpp"
#include "aistore/hashing/sha256.hpp"
#include "aistore/storage/local_chunk_store.hpp"

namespace {

using aistore::chunking::ChunkBuffer;
using aistore::chunking::FixedSizeChunker;
using aistore::hashing::Sha256;
using aistore::storage::LocalChunkStore;

std::span<const std::byte> as_bytes(std::string_view text) {
    return std::as_bytes(std::span{text.data(), text.size()});
}

std::string digest_to_hex(const Sha256::Digest& digest) {
    constexpr std::array<char, 16> kHexDigits{
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    };

    std::string result;
    result.reserve(digest.size() * 2);

    for (const std::byte byte : digest) {
        const auto value = std::to_integer<unsigned int>(byte);

        result.push_back(kHexDigits[(value >> 4U) & 0x0FU]);
        result.push_back(kHexDigits[value & 0x0FU]);
    }

    return result;
}

std::string hash_bytes(std::span<const std::byte> data) {
    Sha256 hasher;
    hasher.update(data);

    return digest_to_hex(hasher.finalize());
}

std::string bytes_to_string(const std::vector<std::byte>& bytes) {
    std::string result;
    result.reserve(bytes.size());

    for (const std::byte byte : bytes) {
        result.push_back(static_cast<char>(std::to_integer<unsigned char>(byte)));
    }

    return result;
}

class TemporaryDirectory {
   public:
    TemporaryDirectory() {
        static std::atomic<std::uint64_t> counter{0};

        const auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();

        path_ = std::filesystem::temp_directory_path() /
                ("aistore-pipeline-test-" + std::to_string(timestamp) + "-" + std::to_string(counter.fetch_add(1)));

        std::filesystem::create_directories(path_);
    }

    ~TemporaryDirectory() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

   private:
    std::filesystem::path path_;
};

TEST(LocalChunkPipelineTest, ChunksHashesStoresAndRestoresByteStream) {
    TemporaryDirectory temporary_directory;
    LocalChunkStore store{temporary_directory.path()};
    FixedSizeChunker chunker{4};

    std::vector<std::string> chunk_ids;
    std::vector<std::uint64_t> offsets;

    const auto consume = [&store, &chunk_ids, &offsets](ChunkBuffer chunk) {
        const std::string chunk_id = hash_bytes(chunk.bytes);

        store.put(chunk_id, chunk.bytes);

        offsets.push_back(chunk.offset);
        chunk_ids.push_back(chunk_id);
    };

    chunker.update(as_bytes("ab"), consume);
    chunker.update(as_bytes("cdefg"), consume);
    chunker.update(as_bytes("hij"), consume);
    chunker.finalize(consume);

    ASSERT_EQ(chunk_ids.size(), 3U);
    ASSERT_EQ(offsets.size(), 3U);

    EXPECT_EQ(offsets[0], 0U);
    EXPECT_EQ(offsets[1], 4U);
    EXPECT_EQ(offsets[2], 8U);

    std::string restored;

    for (const std::string& chunk_id : chunk_ids) {
        EXPECT_TRUE(store.contains(chunk_id));

        const auto chunk = store.get(chunk_id);
        restored += bytes_to_string(chunk);
    }

    EXPECT_EQ(restored, "abcdefghij");
}

}  // namespace
