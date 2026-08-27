#ifndef AISTORE_METADATA_RESTORE_PLAN_HPP
#define AISTORE_METADATA_RESTORE_PLAN_HPP

#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/uuid_v7.hpp"

namespace aistore::metadata {

enum class RestorePlanErrorKind : std::uint8_t {
    VersionNotFound,
    VersionNotCommitted,
    SourceUnavailable,
    VersionRetired,
};

class RestorePlanError : public std::runtime_error {
   public:
    RestorePlanError(RestorePlanErrorKind kind, std::string message);

    [[nodiscard]] RestorePlanErrorKind kind() const noexcept;

   private:
    RestorePlanErrorKind kind_;
};

struct RestorePlan {
    UuidV7 artifact_id;
    std::string version_id;
    std::string source_node_id;
    ObjectLayoutDescriptor layout_descriptor;
};

struct RestoreNodeEndpoint {
    std::string node_id;
    std::string address;
    std::uint16_t port = 0;
};

struct RestoreChunkSources {
    std::string chunk_id;
    std::uint64_t offset = 0;
    std::uint64_t size_bytes = 0;
    std::vector<RestoreNodeEndpoint> sources;
};

struct MultiNodeRestorePlan {
    UuidV7 artifact_id;
    std::string version_id;
    std::string object_id;
    std::string layout_id;
    ObjectLayoutDescriptor layout_descriptor;
    std::vector<RestoreChunkSources> chunks;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_RESTORE_PLAN_HPP
