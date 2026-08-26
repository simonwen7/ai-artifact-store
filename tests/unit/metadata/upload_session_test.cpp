#include "aistore/metadata/upload_session.hpp"

#include <gtest/gtest.h>

#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include "aistore/metadata/chunking.hpp"

namespace {

using aistore::metadata::ChunkingStrategy;
using aistore::metadata::FastCdcParameters;
using aistore::metadata::upload_session_identity_matches;
using aistore::metadata::UploadSession;
using aistore::metadata::UploadSessionState;
using aistore::metadata::UuidV7;

constexpr std::uint64_t kFourMiB = 4ULL * 1024ULL * 1024ULL;

TEST(UploadSessionTest, OpenSessionExposesImmutableConfiguration) {
    const UuidV7 session_id = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();

    const UploadSession session{
        session_id,
        artifact_id,
        "node-a",
        ChunkingStrategy::FixedSize,
        kFourMiB,
        std::nullopt,
        UploadSession::ImmutableMetadata{
            {"framework", "pytorch"},
            {"kind", "checkpoint"},
        },
        UploadSessionState::Open,
        std::nullopt,
    };

    EXPECT_EQ(session.session_id(), session_id);
    EXPECT_EQ(session.artifact_id(), artifact_id);
    EXPECT_EQ(session.target_node_id(), "node-a");
    EXPECT_EQ(session.chunking_strategy(), ChunkingStrategy::FixedSize);
    EXPECT_EQ(session.chunk_size_bytes(), kFourMiB);
    EXPECT_FALSE(session.parent_version_id().has_value());
    EXPECT_EQ(session.immutable_metadata().at("framework"), "pytorch");
    EXPECT_EQ(session.immutable_metadata().at("kind"), "checkpoint");
    EXPECT_EQ(session.state(), UploadSessionState::Open);
    EXPECT_FALSE(session.finalized_version_id().has_value());
}

TEST(UploadSessionTest, RejectsEmptyTargetNodeId) {
    EXPECT_THROW((UploadSession{
                     UuidV7::generate(),
                     UuidV7::generate(),
                     "",
                     ChunkingStrategy::FixedSize,
                     kFourMiB,
                     std::nullopt,
                     UploadSession::ImmutableMetadata{},
                     UploadSessionState::Open,
                     std::nullopt,
                 }),
                 std::invalid_argument);
}

TEST(UploadSessionTest, RejectsMalformedTargetNodeId) {
    EXPECT_THROW((UploadSession{
                     UuidV7::generate(),
                     UuidV7::generate(),
                     "node/a",
                     ChunkingStrategy::FixedSize,
                     kFourMiB,
                     std::nullopt,
                     UploadSession::ImmutableMetadata{},
                     UploadSessionState::Open,
                     std::nullopt,
                 }),
                 std::invalid_argument);

    EXPECT_THROW((UploadSession{
                     UuidV7::generate(),
                     UuidV7::generate(),
                     "node space",
                     ChunkingStrategy::FixedSize,
                     kFourMiB,
                     std::nullopt,
                     UploadSession::ImmutableMetadata{},
                     UploadSessionState::Open,
                     std::nullopt,
                 }),
                 std::invalid_argument);
}

TEST(UploadSessionTest, RejectsZeroChunkSize) {
    EXPECT_THROW((UploadSession{
                     UuidV7::generate(),
                     UuidV7::generate(),
                     "node-a",
                     ChunkingStrategy::FixedSize,
                     0,
                     std::nullopt,
                     UploadSession::ImmutableMetadata{},
                     UploadSessionState::Open,
                     std::nullopt,
                 }),
                 std::invalid_argument);
}

TEST(UploadSessionTest, RejectsMalformedParentVersionId) {
    EXPECT_THROW((UploadSession{
                     UuidV7::generate(),
                     UuidV7::generate(),
                     "node-a",
                     ChunkingStrategy::FixedSize,
                     kFourMiB,
                     std::string{"not-a-valid-version-id"},
                     UploadSession::ImmutableMetadata{},
                     UploadSessionState::Open,
                     std::nullopt,
                 }),
                 std::invalid_argument);
}

TEST(UploadSessionTest, RejectsInvalidFinalizedVersionStateCombination) {
    const std::string version_id(64, 'a');
    const std::string other_version_id(64, 'b');

    EXPECT_THROW((UploadSession{
                     UuidV7::generate(),
                     UuidV7::generate(),
                     "node-a",
                     ChunkingStrategy::FixedSize,
                     kFourMiB,
                     std::nullopt,
                     UploadSession::ImmutableMetadata{},
                     UploadSessionState::Open,
                     version_id,
                 }),
                 std::invalid_argument);

    EXPECT_THROW((UploadSession{
                     UuidV7::generate(),
                     UuidV7::generate(),
                     "node-a",
                     ChunkingStrategy::FixedSize,
                     kFourMiB,
                     std::nullopt,
                     UploadSession::ImmutableMetadata{},
                     UploadSessionState::Aborted,
                     version_id,
                 }),
                 std::invalid_argument);

    EXPECT_THROW((UploadSession{
                     UuidV7::generate(),
                     UuidV7::generate(),
                     "node-a",
                     ChunkingStrategy::FixedSize,
                     kFourMiB,
                     std::nullopt,
                     UploadSession::ImmutableMetadata{},
                     UploadSessionState::Committed,
                     std::nullopt,
                 }),
                 std::invalid_argument);

    const UploadSession committed{
        UuidV7::generate(),
        UuidV7::generate(),
        "node-a",
        ChunkingStrategy::FixedSize,
        kFourMiB,
        std::nullopt,
        UploadSession::ImmutableMetadata{},
        UploadSessionState::Committed,
        version_id,
    };

    EXPECT_EQ(committed.state(), UploadSessionState::Committed);
    EXPECT_EQ(*committed.finalized_version_id(), version_id);

    EXPECT_THROW((UploadSession{
                     UuidV7::generate(),
                     UuidV7::generate(),
                     "node-a",
                     ChunkingStrategy::FixedSize,
                     kFourMiB,
                     version_id,
                     UploadSession::ImmutableMetadata{},
                     UploadSessionState::Committed,
                     version_id,
                 }),
                 std::invalid_argument);

    const UploadSession committed_with_parent{
        UuidV7::generate(),
        UuidV7::generate(),
        "node-a",
        ChunkingStrategy::FixedSize,
        kFourMiB,
        other_version_id,
        UploadSession::ImmutableMetadata{},
        UploadSessionState::Committed,
        version_id,
    };

    EXPECT_EQ(*committed_with_parent.parent_version_id(), other_version_id);
    EXPECT_EQ(*committed_with_parent.finalized_version_id(), version_id);
}

TEST(UploadSessionTest, RejectsEmptyImmutableMetadataKey) {
    EXPECT_THROW((UploadSession{
                     UuidV7::generate(),
                     UuidV7::generate(),
                     "node-a",
                     ChunkingStrategy::FixedSize,
                     kFourMiB,
                     std::nullopt,
                     UploadSession::ImmutableMetadata{
                         {"", "value"},
                     },
                     UploadSessionState::Open,
                     std::nullopt,
                 }),
                 std::invalid_argument);
}

TEST(UploadSessionTest, AcceptsValidFastCdcSession) {
    const UuidV7 session_id = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const FastCdcParameters parameters{
        .min_chunk_size_bytes = 64,
        .avg_chunk_size_bytes = 256,
        .max_chunk_size_bytes = 1024,
    };

    const UploadSession session{
        session_id,
        artifact_id,
        "node-a",
        parameters,
        std::nullopt,
        UploadSession::ImmutableMetadata{
            {"framework", "pytorch"},
        },
        UploadSessionState::Open,
        std::nullopt,
    };

    EXPECT_EQ(session.session_id(), session_id);
    EXPECT_EQ(session.chunking_strategy(), ChunkingStrategy::FastCdc);
    EXPECT_EQ(session.fastcdc_parameters(), parameters);
    EXPECT_FALSE(session.fixed_chunk_size_bytes().has_value());
}

TEST(UploadSessionTest, RejectsInvalidFastCdcParameters) {
    EXPECT_THROW((UploadSession{
                     UuidV7::generate(),
                     UuidV7::generate(),
                     "node-a",
                     FastCdcParameters{
                         .min_chunk_size_bytes = 64,
                         .avg_chunk_size_bytes = 300,
                         .max_chunk_size_bytes = 1024,
                     },
                     std::nullopt,
                     UploadSession::ImmutableMetadata{},
                     UploadSessionState::Open,
                     std::nullopt,
                 }),
                 std::invalid_argument);
}

TEST(UploadSessionTest, ExposesStrategySpecificConfiguration) {
    const FastCdcParameters parameters{
        .min_chunk_size_bytes = 64,
        .avg_chunk_size_bytes = 256,
        .max_chunk_size_bytes = 1024,
    };

    const UploadSession fastcdc_session{
        UuidV7::generate(),
        UuidV7::generate(),
        "node-a",
        parameters,
        std::nullopt,
        UploadSession::ImmutableMetadata{},
        UploadSessionState::Open,
        std::nullopt,
    };

    EXPECT_THROW((void)fastcdc_session.chunk_size_bytes(), std::logic_error);
    EXPECT_EQ(fastcdc_session.fastcdc_parameters(), parameters);

    const UploadSession fixed_session{
        UuidV7::generate(),
        UuidV7::generate(),
        "node-a",
        ChunkingStrategy::FixedSize,
        kFourMiB,
        std::nullopt,
        UploadSession::ImmutableMetadata{},
        UploadSessionState::Open,
        std::nullopt,
    };

    EXPECT_EQ(fixed_session.chunk_size_bytes(), kFourMiB);
    EXPECT_FALSE(fixed_session.fastcdc_parameters().has_value());
}

TEST(UploadSessionTest, PreservesFixedSizeSessionSemantics) {
    const UploadSession session{
        UuidV7::generate(),
        UuidV7::generate(),
        "node-a",
        ChunkingStrategy::FixedSize,
        kFourMiB,
        std::nullopt,
        UploadSession::ImmutableMetadata{},
        UploadSessionState::Open,
        std::nullopt,
    };

    EXPECT_EQ(session.chunking_strategy(), ChunkingStrategy::FixedSize);
    EXPECT_EQ(session.fixed_chunk_size_bytes(), kFourMiB);
    EXPECT_EQ(session.chunk_size_bytes(), kFourMiB);
    EXPECT_FALSE(session.fastcdc_parameters().has_value());
}

TEST(UploadSessionTest, AcceptsMultiNodePlacementSnapshot) {
    const UuidV7 session_id = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();

    const UploadSession session{
        session_id,
        artifact_id,
        3U,
        std::vector<std::string>{"node-a", "node-b", "node-c"},
        ChunkingStrategy::FixedSize,
        kFourMiB,
        std::nullopt,
        UploadSession::ImmutableMetadata{{"framework", "pytorch"}},
        UploadSessionState::Open,
        std::nullopt,
    };

    EXPECT_EQ(session.replication_factor(), 3U);
    EXPECT_EQ(session.placement_node_ids(), (std::vector<std::string>{"node-a", "node-b", "node-c"}));
    EXPECT_EQ(session.target_node_id(), "node-a");
}

TEST(UploadSessionTest, RejectsInvalidReplicationFactorOrNodeSet) {
    EXPECT_THROW((UploadSession{
                     UuidV7::generate(),
                     UuidV7::generate(),
                     0U,
                     std::vector<std::string>{"node-a"},
                     ChunkingStrategy::FixedSize,
                     kFourMiB,
                     std::nullopt,
                     UploadSession::ImmutableMetadata{},
                     UploadSessionState::Open,
                     std::nullopt,
                 }),
                 std::invalid_argument);

    EXPECT_THROW((UploadSession{
                     UuidV7::generate(),
                     UuidV7::generate(),
                     2U,
                     std::vector<std::string>{"node-b", "node-a"},
                     ChunkingStrategy::FixedSize,
                     kFourMiB,
                     std::nullopt,
                     UploadSession::ImmutableMetadata{},
                     UploadSessionState::Open,
                     std::nullopt,
                 }),
                 std::invalid_argument);

    EXPECT_THROW((UploadSession{
                     UuidV7::generate(),
                     UuidV7::generate(),
                     2U,
                     std::vector<std::string>{"node-a"},
                     ChunkingStrategy::FixedSize,
                     kFourMiB,
                     std::nullopt,
                     UploadSession::ImmutableMetadata{},
                     UploadSessionState::Open,
                     std::nullopt,
                 }),
                 std::invalid_argument);
}

TEST(UploadSessionTest, PreservesLegacySingleTargetAsRfOneSnapshot) {
    const UploadSession session{
        UuidV7::generate(),
        UuidV7::generate(),
        "node-a",
        ChunkingStrategy::FixedSize,
        kFourMiB,
        std::nullopt,
        UploadSession::ImmutableMetadata{},
        UploadSessionState::Open,
        std::nullopt,
    };

    EXPECT_EQ(session.replication_factor(), 1U);
    EXPECT_EQ(session.placement_node_ids(), (std::vector<std::string>{"node-a"}));
    EXPECT_EQ(session.target_node_id(), "node-a");
}

TEST(UploadSessionTest, SessionIdentityIncludesReplicationConfiguration) {
    const UuidV7 session_id = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();

    const UploadSession baseline{
        session_id,
        artifact_id,
        2U,
        std::vector<std::string>{"node-a", "node-b", "node-c"},
        ChunkingStrategy::FixedSize,
        kFourMiB,
        std::nullopt,
        UploadSession::ImmutableMetadata{{"marker", "same"}},
        UploadSessionState::Open,
        std::nullopt,
    };

    const UploadSession different_factor{
        session_id,
        artifact_id,
        3U,
        std::vector<std::string>{"node-a", "node-b", "node-c"},
        ChunkingStrategy::FixedSize,
        kFourMiB,
        std::nullopt,
        UploadSession::ImmutableMetadata{{"marker", "same"}},
        UploadSessionState::Open,
        std::nullopt,
    };

    const UploadSession different_placement{
        session_id,
        artifact_id,
        2U,
        std::vector<std::string>{"node-a", "node-b", "node-d"},
        ChunkingStrategy::FixedSize,
        kFourMiB,
        std::nullopt,
        UploadSession::ImmutableMetadata{{"marker", "same"}},
        UploadSessionState::Open,
        std::nullopt,
    };

    EXPECT_TRUE(upload_session_identity_matches(baseline, baseline));
    EXPECT_FALSE(upload_session_identity_matches(baseline, different_factor));
    EXPECT_FALSE(upload_session_identity_matches(baseline, different_placement));
}

}  // namespace
