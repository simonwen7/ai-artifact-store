#ifndef AISTORE_METADATA_RESTORE_PLAN_HPP
#define AISTORE_METADATA_RESTORE_PLAN_HPP

#include <cstdint>
#include <stdexcept>
#include <string>

#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/uuid_v7.hpp"

namespace aistore::metadata {

enum class RestorePlanErrorKind : std::uint8_t {
    VersionNotFound,
    VersionNotCommitted,
    SourceUnavailable,
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

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_RESTORE_PLAN_HPP
