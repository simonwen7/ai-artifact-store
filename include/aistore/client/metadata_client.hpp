#ifndef AISTORE_CLIENT_METADATA_CLIENT_HPP
#define AISTORE_CLIENT_METADATA_CLIENT_HPP

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "aistore/http/http_client.hpp"
#include "aistore/metadata/chunk_metadata.hpp"
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

    [[nodiscard]] ChunkNegotiationResult negotiate_chunks(const metadata::UuidV7& session_id,
                                                          const std::vector<metadata::ChunkMetadata>& chunks) const;

   private:
    http::HttpClient http_client_;
};

}  // namespace aistore::client

#endif  // AISTORE_CLIENT_METADATA_CLIENT_HPP
