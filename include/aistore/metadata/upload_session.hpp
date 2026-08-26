#ifndef AISTORE_METADATA_UPLOAD_SESSION_HPP
#define AISTORE_METADATA_UPLOAD_SESSION_HPP

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>

#include "aistore/metadata/artifact_model.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/uuid_v7.hpp"

namespace aistore::metadata {

enum class UploadSessionState : std::uint8_t {
    Open,
    Committed,
    Aborted,
};

class UploadSession {
   public:
    using ImmutableMetadata = ArtifactVersion::ImmutableMetadata;

    UploadSession(UuidV7 session_id, UuidV7 artifact_id, std::string target_node_id, ChunkingStrategy chunking_strategy,
                  std::uint64_t chunk_size_bytes, std::optional<std::string> parent_version_id,
                  ImmutableMetadata immutable_metadata, UploadSessionState state,
                  std::optional<std::string> finalized_version_id);

    [[nodiscard]] const UuidV7& session_id() const noexcept;

    [[nodiscard]] const UuidV7& artifact_id() const noexcept;

    [[nodiscard]] const std::string& target_node_id() const noexcept;

    [[nodiscard]] ChunkingStrategy chunking_strategy() const noexcept;

    [[nodiscard]] std::uint64_t chunk_size_bytes() const noexcept;

    [[nodiscard]] const std::optional<std::string>& parent_version_id() const noexcept;

    [[nodiscard]] const ImmutableMetadata& immutable_metadata() const noexcept;

    [[nodiscard]] UploadSessionState state() const noexcept;

    [[nodiscard]] const std::optional<std::string>& finalized_version_id() const noexcept;

   private:
    static void validate_node_id(std::string_view node_id);

    static void validate_content_id(const std::string& content_id, const char* field_name);

    static void validate_metadata(const ImmutableMetadata& immutable_metadata);

    UuidV7 session_id_;
    UuidV7 artifact_id_;
    std::string target_node_id_;
    ChunkingStrategy chunking_strategy_;
    std::uint64_t chunk_size_bytes_;
    std::optional<std::string> parent_version_id_;
    ImmutableMetadata immutable_metadata_;
    UploadSessionState state_;
    std::optional<std::string> finalized_version_id_;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_UPLOAD_SESSION_HPP
