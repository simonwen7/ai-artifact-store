#include "aistore/metadata/lifecycle.hpp"

#include <gtest/gtest.h>

#include <map>
#include <optional>
#include <string>

namespace {

using aistore::metadata::artifact_kind_from_string;
using aistore::metadata::artifact_kind_to_string;
using aistore::metadata::ArtifactKind;
using aistore::metadata::lifecycle_decision_reason_from_string;
using aistore::metadata::lifecycle_decision_reason_to_string;
using aistore::metadata::LifecycleDecisionReason;
using aistore::metadata::LifecycleError;
using aistore::metadata::LifecycleErrorKind;
using aistore::metadata::LifecyclePolicy;
using aistore::metadata::LifecycleRule;
using aistore::metadata::resolve_artifact_kind;
using aistore::metadata::UuidV7;
using aistore::metadata::validate_lifecycle_policy;

LifecyclePolicy make_valid_policy(const UuidV7& policy_id) {
    LifecyclePolicy policy{
        .policy_id = policy_id,
        .name = "default-retention",
        .rules = {},
    };

    for (const ArtifactKind kind : {ArtifactKind::Generic, ArtifactKind::ModelCheckpoint, ArtifactKind::DatasetSnapshot,
                                    ArtifactKind::EmbeddingIndex, ArtifactKind::EvaluationOutput}) {
        policy.rules.emplace(kind, LifecycleRule{
                                       .artifact_kind = kind,
                                       .keep_last_n = 1U,
                                       .max_age_seconds = 3600U,
                                   });
    }

    return policy;
}

TEST(LifecycleTest, ArtifactKindStringRoundTrip) {
    for (const ArtifactKind kind : {ArtifactKind::Generic, ArtifactKind::ModelCheckpoint, ArtifactKind::DatasetSnapshot,
                                    ArtifactKind::EmbeddingIndex, ArtifactKind::EvaluationOutput}) {
        const std::string_view serialized = artifact_kind_to_string(kind);
        EXPECT_EQ(artifact_kind_from_string(serialized), kind);
    }
}

TEST(LifecycleTest, LifecyclePolicyRequiresExactlyOneRulePerKind) {
    const UuidV7 policy_id = UuidV7::generate();

    {
        LifecyclePolicy policy{
            .policy_id = policy_id,
            .name = "incomplete",
            .rules = {},
        };

        try {
            validate_lifecycle_policy(policy);
            FAIL() << "expected LifecycleError";
        } catch (const LifecycleError& error) {
            EXPECT_EQ(error.kind(), LifecycleErrorKind::InvalidRequest);
        }
    }

    {
        LifecyclePolicy policy = make_valid_policy(policy_id);
        policy.rules.erase(ArtifactKind::Generic);

        try {
            validate_lifecycle_policy(policy);
            FAIL() << "expected LifecycleError";
        } catch (const LifecycleError& error) {
            EXPECT_EQ(error.kind(), LifecycleErrorKind::InvalidRequest);
        }
    }

    {
        LifecyclePolicy policy = make_valid_policy(policy_id);
        policy.rules[ArtifactKind::Generic].artifact_kind = ArtifactKind::ModelCheckpoint;

        try {
            validate_lifecycle_policy(policy);
            FAIL() << "expected LifecycleError";
        } catch (const LifecycleError& error) {
            EXPECT_EQ(error.kind(), LifecycleErrorKind::InvalidRequest);
        }
    }

    EXPECT_NO_THROW(validate_lifecycle_policy(make_valid_policy(policy_id)));
}

TEST(LifecycleTest, LifecyclePolicyValidatesRuleBounds) {
    const UuidV7 policy_id = UuidV7::generate();

    {
        LifecyclePolicy policy = make_valid_policy(policy_id);
        policy.name = "";

        try {
            validate_lifecycle_policy(policy);
            FAIL() << "expected LifecycleError";
        } catch (const LifecycleError& error) {
            EXPECT_EQ(error.kind(), LifecycleErrorKind::InvalidRequest);
        }
    }

    {
        LifecyclePolicy policy = make_valid_policy(policy_id);
        policy.name = std::string(129U, 'x');

        try {
            validate_lifecycle_policy(policy);
            FAIL() << "expected LifecycleError";
        } catch (const LifecycleError& error) {
            EXPECT_EQ(error.kind(), LifecycleErrorKind::InvalidRequest);
        }
    }

    {
        LifecyclePolicy policy = make_valid_policy(policy_id);
        policy.rules[ArtifactKind::Generic].keep_last_n = 1001U;

        try {
            validate_lifecycle_policy(policy);
            FAIL() << "expected LifecycleError";
        } catch (const LifecycleError& error) {
            EXPECT_EQ(error.kind(), LifecycleErrorKind::InvalidRequest);
        }
    }

    {
        LifecyclePolicy policy = make_valid_policy(policy_id);
        policy.rules[ArtifactKind::Generic].max_age_seconds = 315360001ULL;

        try {
            validate_lifecycle_policy(policy);
            FAIL() << "expected LifecycleError";
        } catch (const LifecycleError& error) {
            EXPECT_EQ(error.kind(), LifecycleErrorKind::InvalidRequest);
        }
    }
}

TEST(LifecycleTest, MissingArtifactKindResolvesToGeneric) {
    EXPECT_EQ(resolve_artifact_kind(std::nullopt), ArtifactKind::Generic);
}

TEST(LifecycleTest, UnknownReservedArtifactKindIsRejected) {
    try {
        (void)resolve_artifact_kind(std::optional<std::string_view>{"not-a-reserved-kind"});
        FAIL() << "expected LifecycleError";
    } catch (const LifecycleError& error) {
        EXPECT_EQ(error.kind(), LifecycleErrorKind::InvalidArtifactKind);
    }
}

TEST(LifecycleTest, LifecycleDecisionReasonStringRoundTrip) {
    for (const LifecycleDecisionReason reason :
         {LifecycleDecisionReason::Pinned, LifecycleDecisionReason::Tagged, LifecycleDecisionReason::ManifestReferenced,
          LifecycleDecisionReason::KeepLastN, LifecycleDecisionReason::AgeNotReached,
          LifecycleDecisionReason::PolicyRetire}) {
        const std::string_view serialized = lifecycle_decision_reason_to_string(reason);
        EXPECT_EQ(lifecycle_decision_reason_from_string(serialized), reason);
    }
}

}  // namespace
