#include "aistore/metadata/artifact_model.hpp"

#include <gtest/gtest.h>

#include <string>

namespace {

using aistore::metadata::Artifact;
using aistore::metadata::ArtifactVersion;
using aistore::metadata::UuidV7;
using aistore::metadata::VersionState;

const std::string kObjectId(64, 'a');

const UuidV7 kArtifactId{
    "01890f3e-9c8a-7cc2-bc63-7f0c2e67a1d1",
};

const UuidV7 kVersionIdA{
    "01890f3e-9c8b-7123-8abc-1234567890ab",
};

const UuidV7 kVersionIdB{
    "01890f3e-9c8c-7456-9abc-abcdef123456",
};

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

TEST(ArtifactModelTest, PreservesVersionLogicalIdentity) {
    ArtifactVersion version{
        kVersionIdA,
        kArtifactId,
        kObjectId,
        VersionState::Committed,
    };

    EXPECT_EQ(version.version_id(), kVersionIdA);

    EXPECT_EQ(version.artifact_id(), kArtifactId);

    EXPECT_EQ(version.root_object_id(), kObjectId);

    EXPECT_EQ(version.state(), VersionState::Committed);
}

TEST(ArtifactModelTest, SameContentCanBackDifferentLogicalVersions) {
    ArtifactVersion first{
        kVersionIdA,
        kArtifactId,
        kObjectId,
        VersionState::Committed,
    };

    ArtifactVersion second{
        kVersionIdB,
        kArtifactId,
        kObjectId,
        VersionState::Committed,
    };

    EXPECT_NE(first.version_id(), second.version_id());

    EXPECT_EQ(first.root_object_id(), second.root_object_id());
}

TEST(ArtifactModelTest, RejectsMalformedRootObjectId) {
    EXPECT_THROW(static_cast<void>(ArtifactVersion{
                     kVersionIdA,
                     kArtifactId,
                     "invalid-object-id",
                     VersionState::Staging,
                 }),
                 std::invalid_argument);
}

}  // namespace
