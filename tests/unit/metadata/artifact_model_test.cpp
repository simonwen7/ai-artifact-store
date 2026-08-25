#include "aistore/metadata/artifact_model.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using aistore::metadata::Artifact;
using aistore::metadata::ArtifactVersion;
using aistore::metadata::UuidV7;
using aistore::metadata::VersionState;

const std::string kObjectId(64, 'a');

const std::string kOtherObjectId(64, 'b');

const std::string kParentVersionId(64, 'c');

const UuidV7 kArtifactId{
    "01890f3e-9c8a-7cc2-bc63-7f0c2e67a1d1",
};

std::string bytes_to_hex(const std::vector<std::byte>& bytes) {
    constexpr std::array<char, 16> kHexDigits{
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    };

    std::string result;
    result.reserve(bytes.size() * 2);

    for (const std::byte byte : bytes) {
        const auto value = std::to_integer<unsigned int>(byte);

        result.push_back(kHexDigits[(value >> 4U) & 0x0FU]);

        result.push_back(kHexDigits[value & 0x0FU]);
    }

    return result;
}

TEST(ArtifactModelTest, PreservesArtifactLogicalIdentity) {
    Artifact artifact{
        kArtifactId,
        "training-checkpoint",
        "recommendation-model",
    };

    EXPECT_EQ(artifact.artifact_id(), kArtifactId);

    EXPECT_EQ(artifact.name(), "training-checkpoint");

    EXPECT_EQ(artifact.project(), "recommendation-model");
}

TEST(ArtifactModelTest, RejectsEmptyArtifactName) {
    EXPECT_THROW(static_cast<void>(Artifact{
                     kArtifactId,
                     "",
                     "recommendation-model",
                 }),
                 std::invalid_argument);
}

TEST(ArtifactModelTest, RejectsEmptyProjectName) {
    EXPECT_THROW(static_cast<void>(Artifact{
                     kArtifactId,
                     "training-checkpoint",
                     "",
                 }),
                 std::invalid_argument);
}

TEST(ArtifactModelTest, CanonicalizesKnownArtifactVersion) {
    const ArtifactVersion version{
        kArtifactId,
        kObjectId,
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{
            {"framework", "pytorch"},
            {"precision", "bf16"},
        },
        VersionState::Committed,
    };

    EXPECT_EQ(bytes_to_hex(version.canonical_bytes()),
              "414953544f52455f56455253494f4e"
              "00000001"
              "01890f3e9c8a7cc2bc637f0c2e67a1d1"
              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
              "00"
              "0000000000000002"
              "0000000000000009"
              "6672616d65776f726b"
              "0000000000000007"
              "7079746f726368"
              "0000000000000009"
              "707265636973696f6e"
              "0000000000000004"
              "62663136");

    EXPECT_EQ(version.version_id(), "10c2ab368f0fc99e8926ec1bea038eb24f5e882e62e7d9547b27088d8236e868");
}

TEST(ArtifactModelTest, SameSemanticPayloadProducesSameVersionId) {
    const ArtifactVersion first{
        kArtifactId,
        kObjectId,
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{
            {"framework", "pytorch"},
        },
        VersionState::Committed,
    };

    const ArtifactVersion second{
        kArtifactId,
        kObjectId,
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{
            {"framework", "pytorch"},
        },
        VersionState::Committed,
    };

    EXPECT_EQ(first.version_id(), second.version_id());

    EXPECT_EQ(first.canonical_bytes(), second.canonical_bytes());
}

TEST(ArtifactModelTest, VersionStateDoesNotAffectVersionId) {
    const ArtifactVersion::ImmutableMetadata metadata{
        {"framework", "pytorch"},
    };

    const ArtifactVersion staging{
        kArtifactId, kObjectId, std::nullopt, metadata, VersionState::Staging,
    };

    const ArtifactVersion committed{
        kArtifactId, kObjectId, std::nullopt, metadata, VersionState::Committed,
    };

    const ArtifactVersion failed{
        kArtifactId, kObjectId, std::nullopt, metadata, VersionState::Failed,
    };

    EXPECT_EQ(staging.version_id(), committed.version_id());

    EXPECT_EQ(committed.version_id(), failed.version_id());
}

TEST(ArtifactModelTest, DifferentRootObjectChangesVersionId) {
    const ArtifactVersion first{
        kArtifactId, kObjectId, std::nullopt, ArtifactVersion::ImmutableMetadata{}, VersionState::Committed,
    };

    const ArtifactVersion second{
        kArtifactId, kOtherObjectId, std::nullopt, ArtifactVersion::ImmutableMetadata{}, VersionState::Committed,
    };

    EXPECT_NE(first.version_id(), second.version_id());
}

TEST(ArtifactModelTest, ParentVersionChangesVersionId) {
    const ArtifactVersion without_parent{
        kArtifactId, kObjectId, std::nullopt, ArtifactVersion::ImmutableMetadata{}, VersionState::Committed,
    };

    const ArtifactVersion with_parent{
        kArtifactId, kObjectId, kParentVersionId, ArtifactVersion::ImmutableMetadata{}, VersionState::Committed,
    };

    EXPECT_NE(without_parent.version_id(), with_parent.version_id());
}

TEST(ArtifactModelTest, ImmutableMetadataChangesVersionId) {
    const ArtifactVersion without_metadata{
        kArtifactId, kObjectId, std::nullopt, ArtifactVersion::ImmutableMetadata{}, VersionState::Committed,
    };

    const ArtifactVersion with_metadata{
        kArtifactId,
        kObjectId,
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{
            {"epoch", "1"},
        },
        VersionState::Committed,
    };

    EXPECT_NE(without_metadata.version_id(), with_metadata.version_id());
}

TEST(ArtifactModelTest, MetadataOrderingIsDeterministic) {
    ArtifactVersion::ImmutableMetadata first_order;
    first_order.emplace("precision", "bf16");
    first_order.emplace("framework", "pytorch");

    ArtifactVersion::ImmutableMetadata second_order;
    second_order.emplace("framework", "pytorch");
    second_order.emplace("precision", "bf16");

    const ArtifactVersion first{
        kArtifactId, kObjectId, std::nullopt, std::move(first_order), VersionState::Committed,
    };

    const ArtifactVersion second{
        kArtifactId, kObjectId, std::nullopt, std::move(second_order), VersionState::Committed,
    };

    EXPECT_EQ(first.canonical_bytes(), second.canonical_bytes());

    EXPECT_EQ(first.version_id(), second.version_id());
}

TEST(ArtifactModelTest, RejectsMalformedRootObjectId) {
    EXPECT_THROW(static_cast<void>(ArtifactVersion{
                     kArtifactId,
                     "invalid-object-id",
                     std::nullopt,
                     ArtifactVersion::ImmutableMetadata{},
                     VersionState::Staging,
                 }),
                 std::invalid_argument);
}

TEST(ArtifactModelTest, RejectsMalformedParentVersionId) {
    EXPECT_THROW(static_cast<void>(ArtifactVersion{
                     kArtifactId,
                     kObjectId,
                     std::string{"not-a-version-id"},
                     ArtifactVersion::ImmutableMetadata{},
                     VersionState::Staging,
                 }),
                 std::invalid_argument);
}

TEST(ArtifactModelTest, RejectsEmptyImmutableMetadataKey) {
    EXPECT_THROW(static_cast<void>(ArtifactVersion{
                     kArtifactId,
                     kObjectId,
                     std::nullopt,
                     ArtifactVersion::ImmutableMetadata{
                         {"", "value"},
                     },
                     VersionState::Staging,
                 }),
                 std::invalid_argument);
}

}  // namespace
