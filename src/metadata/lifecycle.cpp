#include "aistore/metadata/lifecycle.hpp"

#include <array>

namespace aistore::metadata {

namespace {

constexpr std::uint32_t kMaxKeepLastN = 1000U;
constexpr std::uint64_t kMaxAgeSeconds = 315360000ULL;
constexpr std::size_t kMaxPolicyNameBytes = 128U;
constexpr std::size_t kExpectedRuleCount = 5U;

constexpr std::array<ArtifactKind, kExpectedRuleCount> kAllArtifactKinds = {
    ArtifactKind::Generic,        ArtifactKind::ModelCheckpoint,  ArtifactKind::DatasetSnapshot,
    ArtifactKind::EmbeddingIndex, ArtifactKind::EvaluationOutput,
};

}  // namespace

LifecycleError::LifecycleError(LifecycleErrorKind kind, const std::string& message)
    : std::runtime_error(message), kind_(kind) {}

LifecycleErrorKind LifecycleError::kind() const noexcept { return kind_; }

ArtifactKind resolve_artifact_kind(std::optional<std::string_view> metadata_value) {
    if (!metadata_value.has_value()) {
        return ArtifactKind::Generic;
    }

    try {
        return artifact_kind_from_string(*metadata_value);
    } catch (const std::runtime_error&) {
        throw LifecycleError(LifecycleErrorKind::InvalidArtifactKind, "invalid reserved aistore.artifact_kind value");
    }
}

void validate_lifecycle_policy(const LifecyclePolicy& policy) {
    if (policy.name.empty() || policy.name.size() > kMaxPolicyNameBytes) {
        throw LifecycleError(LifecycleErrorKind::InvalidRequest, "lifecycle policy name is invalid");
    }

    if (policy.rules.size() != kExpectedRuleCount) {
        throw LifecycleError(LifecycleErrorKind::InvalidRequest,
                             "lifecycle policy must contain exactly one rule per artifact kind");
    }

    for (const ArtifactKind kind : kAllArtifactKinds) {
        const auto rule_it = policy.rules.find(kind);
        if (rule_it == policy.rules.end()) {
            throw LifecycleError(LifecycleErrorKind::InvalidRequest,
                                 "lifecycle policy must contain exactly one rule per artifact kind");
        }

        const LifecycleRule& rule = rule_it->second;
        if (rule.artifact_kind != kind) {
            throw LifecycleError(LifecycleErrorKind::InvalidRequest,
                                 "lifecycle policy rule artifact kind does not match map key");
        }

        if (rule.keep_last_n > kMaxKeepLastN) {
            throw LifecycleError(LifecycleErrorKind::InvalidRequest, "lifecycle policy keep_last_n is out of range");
        }

        if (rule.max_age_seconds.has_value() && *rule.max_age_seconds > kMaxAgeSeconds) {
            throw LifecycleError(LifecycleErrorKind::InvalidRequest,
                                 "lifecycle policy max_age_seconds is out of range");
        }
    }
}

std::string_view artifact_kind_to_string(ArtifactKind kind) {
    switch (kind) {
        case ArtifactKind::Generic:
            return "generic";

        case ArtifactKind::ModelCheckpoint:
            return "model-checkpoint";

        case ArtifactKind::DatasetSnapshot:
            return "dataset-snapshot";

        case ArtifactKind::EmbeddingIndex:
            return "embedding-index";

        case ArtifactKind::EvaluationOutput:
            return "evaluation-output";
    }

    throw std::logic_error("unsupported artifact kind");
}

ArtifactKind artifact_kind_from_string(std::string_view kind) {
    if (kind == "generic") {
        return ArtifactKind::Generic;
    }

    if (kind == "model-checkpoint") {
        return ArtifactKind::ModelCheckpoint;
    }

    if (kind == "dataset-snapshot") {
        return ArtifactKind::DatasetSnapshot;
    }

    if (kind == "embedding-index") {
        return ArtifactKind::EmbeddingIndex;
    }

    if (kind == "evaluation-output") {
        return ArtifactKind::EvaluationOutput;
    }

    throw std::runtime_error("unsupported artifact kind string");
}

std::string_view lifecycle_run_mode_to_string(LifecycleRunMode mode) {
    switch (mode) {
        case LifecycleRunMode::DryRun:
            return "dry-run";

        case LifecycleRunMode::Apply:
            return "apply";
    }

    throw std::logic_error("unsupported lifecycle run mode");
}

LifecycleRunMode lifecycle_run_mode_from_string(std::string_view mode) {
    if (mode == "dry-run") {
        return LifecycleRunMode::DryRun;
    }

    if (mode == "apply") {
        return LifecycleRunMode::Apply;
    }

    throw std::runtime_error("unsupported lifecycle run mode string");
}

std::string_view lifecycle_decision_reason_to_string(LifecycleDecisionReason reason) {
    switch (reason) {
        case LifecycleDecisionReason::Pinned:
            return "pinned";

        case LifecycleDecisionReason::Tagged:
            return "tagged";

        case LifecycleDecisionReason::ManifestReferenced:
            return "manifest-referenced";

        case LifecycleDecisionReason::KeepLastN:
            return "keep-last-n";

        case LifecycleDecisionReason::AgeNotReached:
            return "age-not-reached";

        case LifecycleDecisionReason::PolicyRetire:
            return "policy-retire";
    }

    throw std::logic_error("unsupported lifecycle decision reason");
}

LifecycleDecisionReason lifecycle_decision_reason_from_string(std::string_view reason) {
    if (reason == "pinned") {
        return LifecycleDecisionReason::Pinned;
    }

    if (reason == "tagged") {
        return LifecycleDecisionReason::Tagged;
    }

    if (reason == "manifest-referenced") {
        return LifecycleDecisionReason::ManifestReferenced;
    }

    if (reason == "keep-last-n") {
        return LifecycleDecisionReason::KeepLastN;
    }

    if (reason == "age-not-reached") {
        return LifecycleDecisionReason::AgeNotReached;
    }

    if (reason == "policy-retire") {
        return LifecycleDecisionReason::PolicyRetire;
    }

    throw std::runtime_error("unsupported lifecycle decision reason string");
}

}  // namespace aistore::metadata
