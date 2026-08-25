#include "aistore/metadata/artifact_model.hpp"

#include <stdexcept>
#include <string>
#include <utility>

namespace aistore::metadata {

Artifact::Artifact(UuidV7 artifact_id, std::string name, std::string project)
    : artifact_id_(std::move(artifact_id)), name_(std::move(name)), project_(std::move(project)) {
    if (name_.empty()) {
        throw std::invalid_argument("artifact name must not be empty");
    }

    if (project_.empty()) {
        throw std::invalid_argument("artifact project must not be empty");
    }
}

const UuidV7& Artifact::artifact_id() const noexcept { return artifact_id_; }

const std::string& Artifact::name() const noexcept { return name_; }

const std::string& Artifact::project() const noexcept { return project_; }

ArtifactVersion::ArtifactVersion(UuidV7 version_id, UuidV7 artifact_id, std::string root_object_id, VersionState state)
    : version_id_(std::move(version_id)),
      artifact_id_(std::move(artifact_id)),
      root_object_id_(std::move(root_object_id)),
      state_(state) {
    validate_object_id(root_object_id_);
}

const UuidV7& ArtifactVersion::version_id() const noexcept { return version_id_; }

const UuidV7& ArtifactVersion::artifact_id() const noexcept { return artifact_id_; }

const std::string& ArtifactVersion::root_object_id() const noexcept { return root_object_id_; }

VersionState ArtifactVersion::state() const noexcept { return state_; }

void ArtifactVersion::validate_object_id(const std::string& object_id) {
    if (object_id.size() != 64) {
        throw std::invalid_argument("root object ID must contain exactly 64 hexadecimal characters");
    }

    for (const char character : object_id) {
        const bool is_digit = character >= '0' && character <= '9';

        const bool is_lower_hex = character >= 'a' && character <= 'f';

        if (!is_digit && !is_lower_hex) {
            throw std::invalid_argument("root object ID must use lowercase hexadecimal characters");
        }
    }
}

}  // namespace aistore::metadata
