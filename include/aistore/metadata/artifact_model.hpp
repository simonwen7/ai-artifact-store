#ifndef AISTORE_METADATA_ARTIFACT_MODEL_HPP
#define AISTORE_METADATA_ARTIFACT_MODEL_HPP

#include <cstdint>
#include <string>

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

    [[nodiscard]] const UuidV7& artifact_id() const noexcept;

    [[nodiscard]] const std::string& name() const noexcept;

    [[nodiscard]] const std::string& project() const noexcept;

   private:
    UuidV7 artifact_id_;
    std::string name_;
    std::string project_;
};

class ArtifactVersion {
   public:
    ArtifactVersion(UuidV7 version_id, UuidV7 artifact_id, std::string root_object_id, VersionState state);

    [[nodiscard]] const UuidV7& version_id() const noexcept;

    [[nodiscard]] const UuidV7& artifact_id() const noexcept;

    [[nodiscard]] const std::string& root_object_id() const noexcept;

    [[nodiscard]] VersionState state() const noexcept;

   private:
    static void validate_object_id(const std::string& object_id);

    UuidV7 version_id_;
    UuidV7 artifact_id_;
    std::string root_object_id_;
    VersionState state_;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_ARTIFACT_MODEL_HPP
