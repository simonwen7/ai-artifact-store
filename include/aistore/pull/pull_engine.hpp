#ifndef AISTORE_PULL_PULL_ENGINE_HPP
#define AISTORE_PULL_PULL_ENGINE_HPP

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>

#include "aistore/client/metadata_client.hpp"
#include "aistore/client/storage_node_client.hpp"
#include "aistore/metadata/uuid_v7.hpp"

namespace aistore::pull {

struct PullRequest {
    std::string version_id;
    std::filesystem::path destination_path;
    bool overwrite{false};
};

struct PullStats {
    std::uint64_t bytes_restored;
    std::size_t total_chunks;
    std::size_t chunks_downloaded;
    std::size_t chunks_reused_from_partial;
    std::uint64_t bytes_received_from_storage;
};

struct PullResult {
    std::string version_id;
    metadata::UuidV7 artifact_id;
    std::string source_node_id;
    std::string object_id;
    std::string layout_id;
    std::filesystem::path destination_path;
    PullStats stats;
};

class PullEngine {
   public:
    static constexpr std::size_t kWorkerCount = 4;
    static constexpr std::size_t kWindowCapacity = 8;
    static constexpr std::uint64_t kMaxM5ChunkSize = 8ULL * 1024ULL * 1024ULL;

    PullEngine(client::MetadataClient& metadata_client, client::StorageNodeClient& storage_client,
               std::string source_node_id);

    [[nodiscard]] PullResult pull(const PullRequest& request) const;

   private:
    client::MetadataClient& metadata_client_;
    client::StorageNodeClient& storage_client_;
    std::string source_node_id_;
};

}  // namespace aistore::pull

#endif  // AISTORE_PULL_PULL_ENGINE_HPP
