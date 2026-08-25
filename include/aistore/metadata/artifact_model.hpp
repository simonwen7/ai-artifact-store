#ifndef AISTORE_METADATA_ARTIFACT_MODEL_HPP
#define AISTORE_METADATA_ARTIFACT_MODEL_HPP

#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "aistore/metadata/uuid_v7.hpp"

namespace aistore::metadata {

enum class VersionState : std::uint8_t {
    Staging,
    Committed,
    Failed,
};

class Artifact {
   public:
    Artifact(UuidV7 artifact_id, std::string name, std::string project);

    [[nodiscard]]
    const UuidV7& artifact_id() const noexcept;

    [[nodiscard]]
    const std::string& name() const noexcept;

    [[nodiscard]]
    const std::string& project() const noexcept;

   private:
    UuidV7 artifact_id_;
    std::string name_;
    std::string project_;
};

class ArtifactVersion {
   public:
    using ImmutableMetadata = std::map<std::string, std::string>;

    static constexpr std::uint32_t kFormatVersion = 1;

    ArtifactVersion(UuidV7 artifact_id, std::string root_object_id, std::optional<std::string> parent_version_id,
                    ImmutableMetadata immutable_metadata, VersionState state);

    [[nodiscard]]
    const std::string& version_id() const noexcept;

    [[nodiscard]]
    const UuidV7& artifact_id() const noexcept;

    [[nodiscard]]
    const std::string& root_object_id() const noexcept;

    [[nodiscard]]
    const std::optional<std::string>& parent_version_id() const noexcept;

    [[nodiscard]]
    const ImmutableMetadata& immutable_metadata() const noexcept;

    [[nodiscard]]
    const std::vector<std::byte>& canonical_bytes() const noexcept;

    [[nodiscard]]
    VersionState state() const noexcept;

   private:
    static void validate_content_id(const std::string& content_id, const char* field_name);

    static void validate_metadata(const ImmutableMetadata& immutable_metadata);

    static std::vector<std::byte> serialize(const UuidV7& artifact_id, const std::string& root_object_id,
                                            const std::optional<std::string>& parent_version_id,
                                            const ImmutableMetadata& immutable_metadata);

    static std::string hash_canonical_bytes(const std::vector<std::byte>& canonical_bytes);

    UuidV7 artifact_id_;
    std::string root_object_id_;
    std::optional<std::string> parent_version_id_;
    ImmutableMetadata immutable_metadata_;
    VersionState state_;
    std::vector<std::byte> canonical_bytes_;
    std::string version_id_;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_ARTIFACT_MODEL_HPP
