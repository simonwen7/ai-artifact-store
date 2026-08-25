#include "aistore/storage/local_chunk_store.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using aistore::storage::LocalChunkStore;

constexpr std::string_view kAbcChunkId =
    "ba7816bf8f01cfea414140de5dae2223"
    "b00361a396177a9cb410ff61f20015ad";

constexpr std::string_view kEmptyChunkId =
    "e3b0c44298fc1c149afbf4c8996fb924"
    "27ae41e4649b934ca495991b7852b855";

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

class TemporaryDirectory {
   public:
    TemporaryDirectory() {
        static std::atomic<std::uint64_t> counter{0};

        const auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();

        path_ = std::filesystem::temp_directory_path() /
                ("aistore-test-" + std::to_string(timestamp) + "-" + std::to_string(counter.fetch_add(1)));

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

TEST(LocalChunkStoreTest, StoresAndReadsVerifiedChunk) {
    TemporaryDirectory temporary_directory;
    LocalChunkStore store{temporary_directory.path()};

    store.put(kAbcChunkId, as_bytes("abc"));

    EXPECT_TRUE(store.contains(kAbcChunkId));

    const auto restored = store.get(kAbcChunkId);

    EXPECT_EQ(bytes_to_string(restored), "abc");
}

TEST(LocalChunkStoreTest, RejectsMismatchedChunkContent) {
    TemporaryDirectory temporary_directory;
    LocalChunkStore store{temporary_directory.path()};

    EXPECT_THROW(store.put(kEmptyChunkId, as_bytes("abc")), std::runtime_error);

    EXPECT_FALSE(store.contains(kEmptyChunkId));
}

TEST(LocalChunkStoreTest, DuplicatePutIsIdempotent) {
    TemporaryDirectory temporary_directory;
    LocalChunkStore store{temporary_directory.path()};

    store.put(kAbcChunkId, as_bytes("abc"));

    store.put(kAbcChunkId, as_bytes("abc"));

    const auto restored = store.get(kAbcChunkId);

    EXPECT_EQ(bytes_to_string(restored), "abc");
}

TEST(LocalChunkStoreTest, RejectsMalformedChunkId) {
    TemporaryDirectory temporary_directory;
    LocalChunkStore store{temporary_directory.path()};

    EXPECT_THROW(static_cast<void>(store.contains("not-a-valid-chunk-id")), std::invalid_argument);
}

TEST(LocalChunkStoreTest, DetectsCorruptionDuringRead) {
    TemporaryDirectory temporary_directory;
    LocalChunkStore store{temporary_directory.path()};

    store.put(kAbcChunkId, as_bytes("abc"));

    const std::filesystem::path stored_path = temporary_directory.path() / "chunks" / "ba" / std::string{kAbcChunkId};

    {
        std::ofstream output(stored_path, std::ios::binary | std::ios::trunc);

        ASSERT_TRUE(output);

        output << "corrupt";
    }

    EXPECT_THROW(static_cast<void>(store.get(kAbcChunkId)), std::runtime_error);
}

}  // namespace
