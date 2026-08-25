#ifndef AISTORE_STORAGE_LOCAL_CHUNK_STORE_HPP
#define AISTORE_STORAGE_LOCAL_CHUNK_STORE_HPP

#include <cstddef>
#include <filesystem>
#include <span>
#include <string_view>
#include <vector>

namespace aistore::storage {

class LocalChunkStore {
   public:
    explicit LocalChunkStore(std::filesystem::path root_directory);

    void put(std::string_view expected_chunk_id, std::span<const std::byte> data);

    [[nodiscard]] std::vector<std::byte> get(std::string_view chunk_id) const;

    [[nodiscard]] bool contains(std::string_view chunk_id) const;

    [[nodiscard]] const std::filesystem::path& root_directory() const noexcept;

   private:
    static void validate_chunk_id(std::string_view chunk_id);

    [[nodiscard]] std::filesystem::path chunk_path(std::string_view chunk_id) const;

    std::filesystem::path root_directory_;
};

}  // namespace aistore::storage

#endif  // AISTORE_STORAGE_LOCAL_CHUNK_STORE_HPP
