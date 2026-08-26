#ifndef AISTORE_PUSH_PUSH_ENGINE_HPP
#define AISTORE_PUSH_PUSH_ENGINE_HPP

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>

#include "aistore/client/metadata_client.hpp"
#include "aistore/client/storage_node_client.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/uuid_v7.hpp"

namespace aistore::push {

struct PushRequest {
    std::filesystem::path source_path;
    metadata::UuidV7 session_id;
};

struct PushStats {
    std::uint64_t bytes_read;
    std::size_t total_chunks;
    std::size_t unique_chunks;
    std::size_t put_requests;
    std::size_t verified_target_chunks;
    std::size_t repaired_target_chunks;
    std::uint64_t bytes_sent_to_storage;
};

struct PreparedPush {
    metadata::UuidV7 session_id;
    metadata::ObjectLayoutDescriptor layout_descriptor;
    PushStats stats;
};

class PushEngine {
   public:
    static constexpr std::size_t kReadBufferSize = static_cast<std::size_t>(1U) * 1024U * 1024U;

    static constexpr std::size_t kWorkerCount = 4U;

    static constexpr std::size_t kQueueCapacity = 8U;

    static constexpr std::size_t kNegotiationBatchSize = 4U;

    static constexpr std::uint64_t kMaxM4ChunkSize = 8ULL * 1024ULL * 1024ULL;

    PushEngine(client::MetadataClient& metadata_client, client::StorageNodeClient& storage_client,
               std::string storage_node_id);

    [[nodiscard]] PreparedPush push(const PushRequest& request) const;

   private:
    client::MetadataClient& metadata_client_;
    client::StorageNodeClient& storage_client_;
    std::string storage_node_id_;
};

}  // namespace aistore::push

#endif  // AISTORE_PUSH_PUSH_ENGINE_HPP
