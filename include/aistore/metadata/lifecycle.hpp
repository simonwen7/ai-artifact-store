#ifndef AISTORE_METADATA_LIFECYCLE_HPP
#define AISTORE_METADATA_LIFECYCLE_HPP

#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

#include "aistore/metadata/uuid_v7.hpp"

namespace aistore::metadata {

inline constexpr std::string_view kArtifactKindMetadataKey = "aistore.artifact_kind";

enum class ArtifactKind : std::uint8_t {
    Generic,
    ModelCheckpoint,
    DatasetSnapshot,
    EmbeddingIndex,
    EvaluationOutput,
};

enum class LifecycleRunMode : std::uint8_t {
    DryRun,
    Apply,
};

enum class LifecycleDecisionReason : std::uint8_t {
    Pinned,
    Tagged,
    ManifestReferenced,
    KeepLastN,
    AgeNotReached,
    PolicyRetire,
};

enum class LifecycleErrorKind : std::uint8_t {
    InvalidRequest,
    PolicyNotFound,
    RunNotFound,
    VersionNotFound,
    VersionRetired,
    RunConflict,
    BlockedByOpenUploadSessions,
    BlockedByGc,
    BlockedByReplication,
    InvalidArtifactKind,
    PolicyConflict,
};

class LifecycleError : public std::runtime_error {
   public:
    LifecycleError(LifecycleErrorKind kind, const std::string& message);

    [[nodiscard]] LifecycleErrorKind kind() const noexcept;

   private:
    LifecycleErrorKind kind_;
};

struct LifecycleRule {
    ArtifactKind artifact_kind = ArtifactKind::Generic;
    std::uint32_t keep_last_n = 0;
    std::optional<std::uint64_t> max_age_seconds;

    bool operator==(const LifecycleRule&) const = default;
};

struct LifecyclePolicy {
    UuidV7 policy_id;
    std::string name;
    std::map<ArtifactKind, LifecycleRule> rules;
};

struct LifecycleStats {
    std::uint64_t versions_scanned = 0;
    std::uint64_t versions_protected = 0;
    std::uint64_t versions_retained_by_policy = 0;
    std::uint64_t versions_candidates = 0;
    std::uint64_t versions_retired = 0;
    std::uint64_t logical_bytes_candidates = 0;
    std::uint64_t logical_bytes_retired = 0;

    bool operator==(const LifecycleStats&) const = default;
};

struct LifecycleRun {
    UuidV7 run_id;
    UuidV7 policy_id;
    LifecycleRunMode mode = LifecycleRunMode::DryRun;
    std::int64_t evaluated_at_unix_ms = 0;
    LifecycleStats stats{};
};

struct LifecycleDecision {
    std::string version_id;
    UuidV7 artifact_id;
    ArtifactKind artifact_kind = ArtifactKind::Generic;
    bool retire = false;
    LifecycleDecisionReason reason = LifecycleDecisionReason::PolicyRetire;
    std::uint64_t logical_size_bytes = 0;
};

[[nodiscard]] ArtifactKind resolve_artifact_kind(std::optional<std::string_view> metadata_value);

void validate_lifecycle_policy(const LifecyclePolicy& policy);

[[nodiscard]] std::string_view artifact_kind_to_string(ArtifactKind kind);

[[nodiscard]] ArtifactKind artifact_kind_from_string(std::string_view kind);

[[nodiscard]] std::string_view lifecycle_run_mode_to_string(LifecycleRunMode mode);

[[nodiscard]] LifecycleRunMode lifecycle_run_mode_from_string(std::string_view mode);

[[nodiscard]] std::string_view lifecycle_decision_reason_to_string(LifecycleDecisionReason reason);

[[nodiscard]] LifecycleDecisionReason lifecycle_decision_reason_from_string(std::string_view reason);

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_LIFECYCLE_HPP
