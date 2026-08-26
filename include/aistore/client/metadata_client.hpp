#ifndef AISTORE_CLIENT_METADATA_CLIENT_HPP
#define AISTORE_CLIENT_METADATA_CLIENT_HPP

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "aistore/http/http_client.hpp"
#include "aistore/metadata/chunk_metadata.hpp"
#include "aistore/metadata/finalize_upload.hpp"
#include "aistore/metadata/gc.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/restore_plan.hpp"
#include "aistore/metadata/storage_location.hpp"
#include "aistore/metadata/upload_session.hpp"
#include "aistore/metadata/uuid_v7.hpp"

namespace aistore::client {

enum class ChunkAvailability : std::uint8_t {
    AvailableOnTarget,
    AvailableElsewhere,
    NoAvailableLocation,
};

struct NegotiatedChunk {
    metadata::ChunkMetadata chunk;
    bool metadata_was_known;
    ChunkAvailability availability;
    std::vector<std::string> available_node_ids;
};

struct ChunkNegotiationResult {
    metadata::UuidV7 session_id;
    std::string target_node_id;
    std::vector<NegotiatedChunk> chunks;
};

class MetadataClient {
   public:
    explicit MetadataClient(http::HttpClientConfig config);

    [[nodiscard]] metadata::UploadSession create_upload_session(const metadata::UploadSession& session) const;

    [[nodiscard]] std::optional<metadata::UploadSession> get_upload_session(const metadata::UuidV7& session_id) const;

    [[nodiscard]] metadata::UploadSession abort_upload_session(const metadata::UuidV7& session_id) const;

    [[nodiscard]] metadata::FinalizeUploadResult finalize_upload(
        const metadata::UuidV7& session_id, const metadata::ObjectLayoutDescriptor& descriptor) const;

    [[nodiscard]] metadata::RestorePlan get_restore_plan(std::string_view version_id,
                                                         std::string_view source_node_id) const;

    [[nodiscard]] ChunkNegotiationResult negotiate_chunks(const metadata::UuidV7& session_id,
                                                          const std::vector<metadata::ChunkMetadata>& chunks) const;

    void register_storage_location(const metadata::StorageLocation& location) const;

    [[nodiscard]] metadata::GcRun start_gc_run(const metadata::UuidV7& gc_run_id, std::string_view target_node_id,
                                               bool dry_run) const;

    [[nodiscard]] std::optional<metadata::GcRun> get_gc_run(const metadata::UuidV7& gc_run_id) const;

    [[nodiscard]] std::vector<metadata::GcChunkDecision> classify_gc_chunks(
        const metadata::UuidV7& gc_run_id, const std::vector<std::string>& chunk_ids) const;

    [[nodiscard]] metadata::GcRun complete_gc_run(const metadata::UuidV7& gc_run_id,
                                                  const metadata::GcPhysicalStats& physical_stats) const;

   private:
    http::HttpClient http_client_;
};

}  // namespace aistore::client

#endif  // AISTORE_CLIENT_METADATA_CLIENT_HPP
