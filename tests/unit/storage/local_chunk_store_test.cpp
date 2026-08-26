#include <gtest/gtest.h>

#include <aistore/storage/local_chunk_store.hpp>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using aistore::storage::LocalChunkStore;
using aistore::storage::StoredChunkPage;

constexpr std::string_view kAbcChunkId =
    "ba7816bf8f01cfea414140de5dae2223"
    "b00361a396177a9cb410ff61f20015ad";

constexpr std::string_view kEmptyChunkId =
    "e3b0c44298fc1c149afbf4c8996fb924"
    "27ae41e4649b934ca495991b7852b855";

constexpr std::string_view kDefChunkId =
    "cb8379ac2098aa165029e3938a51da0b"
    "cecfc008fd6795f401178647f96c5b34";

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

TEST(LocalChunkStoreTest, RemoveDeletesExistingChunk) {
    TemporaryDirectory temporary_directory;
    LocalChunkStore store{temporary_directory.path()};

    store.put(kAbcChunkId, as_bytes("abc"));

    EXPECT_TRUE(store.contains(kAbcChunkId));

    EXPECT_TRUE(store.remove(kAbcChunkId));

    EXPECT_FALSE(store.contains(kAbcChunkId));

    EXPECT_THROW(static_cast<void>(store.get(kAbcChunkId)), std::runtime_error);
}

TEST(LocalChunkStoreTest, RemoveMissingChunkIsIdempotent) {
    TemporaryDirectory temporary_directory;
    LocalChunkStore store{temporary_directory.path()};

    EXPECT_FALSE(store.remove(kAbcChunkId));
}

TEST(LocalChunkStoreTest, ListsOnlyCanonicalStoredChunksInLexicalOrder) {
    TemporaryDirectory temporary_directory;
    LocalChunkStore store{temporary_directory.path()};

    store.put(kEmptyChunkId, as_bytes(""));
    store.put(kAbcChunkId, as_bytes("abc"));

    const std::filesystem::path chunks_directory = temporary_directory.path() / "chunks";
    const std::filesystem::path ba_fanout = chunks_directory / "ba";
    const std::filesystem::path cb_fanout = chunks_directory / "cb";
    std::filesystem::create_directories(cb_fanout);

    std::filesystem::create_directories(ba_fanout / "nested-dir");
    std::filesystem::create_directories(chunks_directory / "ZZ");
    std::ofstream(ba_fanout / ".aistore-chunk-temp") << "temp";
    std::ofstream(ba_fanout / "not-a-valid-chunk-filename") << "junk";
    std::ofstream(ba_fanout / "BA00000000000000000000000000000000000000000000000000000000000000") << "uppercase";
    std::ofstream(ba_fanout / "aa00000000000000000000000000000000000000000000000000000000000000") << "wrong-prefix";
    std::ofstream(cb_fanout / "aa00000000000000000000000000000000000000000000000000000000000000") << "wrong-prefix";

    std::filesystem::create_symlink(ba_fanout / std::string{kAbcChunkId}, ba_fanout / "symlink-chunk");

    const StoredChunkPage page = store.list_chunks(std::nullopt, 256);

    ASSERT_EQ(page.chunks.size(), 2U);
    EXPECT_EQ(page.chunks[0].chunk_id, kAbcChunkId);
    EXPECT_EQ(page.chunks[1].chunk_id, kEmptyChunkId);
    EXPECT_EQ(page.chunks[0].size_bytes, 3U);
    EXPECT_EQ(page.chunks[1].size_bytes, 0U);
    EXPECT_FALSE(page.next_after.has_value());
}

TEST(LocalChunkStoreTest, InventoryPaginationIsExclusiveAndBounded) {
    TemporaryDirectory temporary_directory;
    LocalChunkStore store{temporary_directory.path()};

    store.put(kAbcChunkId, as_bytes("abc"));
    store.put(kDefChunkId, as_bytes("def"));
    store.put(kEmptyChunkId, as_bytes(""));

    const StoredChunkPage first_page = store.list_chunks(std::nullopt, 2);

    ASSERT_EQ(first_page.chunks.size(), 2U);
    EXPECT_EQ(first_page.chunks[0].chunk_id, kAbcChunkId);
    EXPECT_EQ(first_page.chunks[1].chunk_id, kDefChunkId);
    ASSERT_TRUE(first_page.next_after.has_value());
    EXPECT_EQ(*first_page.next_after, kDefChunkId);

    const StoredChunkPage second_page = store.list_chunks(kDefChunkId, 2);

    ASSERT_EQ(second_page.chunks.size(), 1U);
    EXPECT_EQ(second_page.chunks[0].chunk_id, kEmptyChunkId);
    EXPECT_FALSE(second_page.next_after.has_value());

    const StoredChunkPage third_page = store.list_chunks(kEmptyChunkId, 2);

    EXPECT_TRUE(third_page.chunks.empty());
    EXPECT_FALSE(third_page.next_after.has_value());

    EXPECT_THROW(static_cast<void>(store.list_chunks(std::nullopt, 0)), std::invalid_argument);
    EXPECT_THROW(static_cast<void>(store.list_chunks(std::nullopt, 257)), std::invalid_argument);
}

}  // namespace
