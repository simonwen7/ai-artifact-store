#include "aistore/metadata/postgres_metadata_repository.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <optional>
#include <pqxx/pqxx>
#include <stdexcept>
#include <string>
#include <vector>

#include "aistore/metadata/finalize_upload.hpp"
#include "aistore/metadata/restore_plan.hpp"

namespace {

using aistore::metadata::Artifact;
using aistore::metadata::ArtifactVersion;
using aistore::metadata::ChunkingStrategy;
using aistore::metadata::ChunkMetadata;
using aistore::metadata::ChunkRef;
using aistore::metadata::FastCdcParameters;
using aistore::metadata::FinalizeUploadError;
using aistore::metadata::FinalizeUploadErrorKind;
using aistore::metadata::Manifest;
using aistore::metadata::Object;
using aistore::metadata::ObjectLayout;
using aistore::metadata::ObjectLayoutDescriptor;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::RestorePlan;
using aistore::metadata::RestorePlanError;
using aistore::metadata::RestorePlanErrorKind;
using aistore::metadata::StorageLocation;
using aistore::metadata::StorageLocationState;
using aistore::metadata::UploadSession;
using aistore::metadata::UploadSessionState;
using aistore::metadata::UuidV7;
using aistore::metadata::VersionState;

// GoogleTest's testing::Test::Run() shadows a bare using-declaration for Run.
using RunModel = aistore::metadata::Run;

const std::string kObjectId(64, '1');

const std::string kChunkA(64, 'a');

const std::string kChunkB(64, 'b');

const std::string kChunkC(64, 'c');

const std::string kChunkD(64, 'd');

std::string test_database_connection_string() {
    const char* configured = std::getenv("AISTORE_TEST_DB_URL");

    if (configured != nullptr && configured[0] != '\0') {
        return configured;
    }

    return "dbname=ai_artifact_store_test";
}

void reset_metadata_data() {
    pqxx::connection connection{
        test_database_connection_string(),
    };

    pqxx::work transaction{connection};

    transaction
        .exec(
            "TRUNCATE TABLE "
            "    run_tags, "
            "    runs, "
            "    manifest_entries, "
            "    manifests, "
            "    tags, "
            "    artifact_version_metadata, "
            "    storage_locations, "
            "    upload_session_finalizations, "
            "    upload_session_metadata, "
            "    upload_sessions, "
            "    artifact_versions, "
            "    object_layout_chunks, "
            "    object_layouts, "
            "    artifacts, "
            "    objects, "
            "    chunks "
            "RESTART IDENTITY")
        .no_rows();

    transaction.commit();
}

Object make_object() {
    return Object{
        kObjectId,
        6,
    };
}

ObjectLayoutDescriptor make_object_layout_descriptor() {
    return ObjectLayoutDescriptor{
        make_object(),
        ChunkingStrategy::FixedSize,
        ObjectLayout{
            {
                ChunkRef{
                    .chunk_id = kChunkA,
                    .offset = 0,
                    .size = 4,
                },
                ChunkRef{
                    .chunk_id = kChunkB,
                    .offset = 4,
                    .size = 2,
                },
            },
        },
    };
}

ObjectLayoutDescriptor make_alternate_object_layout_descriptor() {
    return ObjectLayoutDescriptor{
        make_object(),
        ChunkingStrategy::FixedSize,
        ObjectLayout{
            {
                ChunkRef{
                    .chunk_id = kChunkC,
                    .offset = 0,
                    .size = 3,
                },
                ChunkRef{
                    .chunk_id = kChunkD,
                    .offset = 3,
                    .size = 3,
                },
            },
        },
    };
}

void register_test_object(PostgresMetadataRepository& repository) { repository.register_object(make_object()); }

void register_test_object_and_layout(PostgresMetadataRepository& repository) {
    repository.register_object(make_object());

    repository.register_object_layout(make_object_layout_descriptor());
}

ArtifactVersion create_manifest_test_version(PostgresMetadataRepository& repository, const UuidV7& artifact_id,
                                             std::string marker) {
    ArtifactVersion version{
        artifact_id,
        kObjectId,
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{
            {"marker", std::move(marker)},
        },
        VersionState::Committed,
    };

    repository.create_version(version);

    return version;
}

ObjectLayoutDescriptor make_finalize_descriptor(char object_marker, char first_chunk_marker = 'a',
                                                char second_chunk_marker = 'b') {
    return ObjectLayoutDescriptor{
        Object{std::string(64, object_marker), 6},
        ChunkingStrategy::FixedSize,
        ObjectLayout{{
            ChunkRef{.chunk_id = std::string(64, first_chunk_marker), .offset = 0, .size = 4},
            ChunkRef{.chunk_id = std::string(64, second_chunk_marker), .offset = 4, .size = 2},
        }},
    };
}

UploadSession make_finalize_session(const UuidV7& session_id, const UuidV7& artifact_id, std::string target_node,
                                    std::string marker) {
    return UploadSession{
        session_id,
        artifact_id,
        std::move(target_node),
        ChunkingStrategy::FixedSize,
        4,
        std::nullopt,
        UploadSession::ImmutableMetadata{{"marker", std::move(marker)}},
        UploadSessionState::Open,
        std::nullopt,
    };
}

void register_finalize_chunks(PostgresMetadataRepository& repository, const ObjectLayoutDescriptor& descriptor,
                              std::string_view node_id, StorageLocationState state) {
    for (const ChunkRef& chunk : descriptor.layout().chunks()) {
        repository.register_chunks({ChunkMetadata{.chunk_id = chunk.chunk_id, .size_bytes = chunk.size}});
        repository.register_storage_location(StorageLocation{
            .chunk_id = chunk.chunk_id,
            .node_id = std::string{node_id},
            .storage_path = std::string{"/m4s5/"} + chunk.chunk_id,
            .state = state,
        });
    }
}

TEST(PostgresMetadataRepositoryTest, CreatesAndReadsArtifact) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();

    const Artifact artifact{
        artifact_id,
        "training-checkpoint",
        "recommendation-model",
    };

    repository.create_artifact(artifact);

    const auto restored = repository.get_artifact(artifact_id);

    ASSERT_TRUE(restored.has_value());

    EXPECT_EQ(restored->artifact_id(), artifact_id);

    EXPECT_EQ(restored->name(), "training-checkpoint");

    EXPECT_EQ(restored->project(), "recommendation-model");
}

TEST(PostgresMetadataRepositoryTest, MissingArtifactReturnsNullopt) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const auto restored = repository.get_artifact(UuidV7::generate());

    EXPECT_FALSE(restored.has_value());
}

TEST(PostgresMetadataRepositoryTest, DatabaseEnforcesUniqueProjectAndName) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const Artifact first{
        UuidV7::generate(),
        "checkpoint",
        "recommendation-model",
    };

    const Artifact second{
        UuidV7::generate(),
        "checkpoint",
        "recommendation-model",
    };

    repository.create_artifact(first);

    EXPECT_THROW(repository.create_artifact(second), pqxx::sql_error);
}

TEST(PostgresMetadataRepositoryTest, RegistersAndReadsObject) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const Object object = make_object();

    repository.register_object(object);

    const auto restored = repository.get_object(object.object_id());

    ASSERT_TRUE(restored.has_value());

    EXPECT_EQ(restored->object_id(), object.object_id());

    EXPECT_EQ(restored->total_size(), object.total_size());
}

TEST(PostgresMetadataRepositoryTest, RegisterObjectIsIdempotent) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const Object object = make_object();

    repository.register_object(object);

    repository.register_object(object);

    pqxx::connection connection{
        test_database_connection_string(),
    };

    pqxx::work transaction{connection};

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) "
                                                 "FROM objects"),
              1);

    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, RegisterObjectRejectsConflictingSize) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    repository.register_object(make_object());

    EXPECT_THROW(repository.register_object(Object{
                     kObjectId,
                     7,
                 }),
                 std::runtime_error);
}

TEST(PostgresMetadataRepositoryTest, MissingObjectReturnsNullopt) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const auto restored = repository.get_object(std::string(64, 'f'));

    EXPECT_FALSE(restored.has_value());
}

TEST(PostgresMetadataRepositoryTest, RegistersAndReadsObjectLayoutTransactionally) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    register_test_object(repository);

    const auto descriptor = make_object_layout_descriptor();

    repository.register_object_layout(descriptor);

    const auto restored = repository.get_object_layout(descriptor.layout_id());

    ASSERT_TRUE(restored.has_value());

    EXPECT_EQ(restored->layout_id(), descriptor.layout_id());

    EXPECT_EQ(restored->object_id(), descriptor.object_id());

    EXPECT_EQ(restored->canonical_bytes(), descriptor.canonical_bytes());

    EXPECT_EQ(restored->chunking_strategy(), ChunkingStrategy::FixedSize);

    ASSERT_EQ(restored->layout().chunks().size(), 2U);
}

TEST(PostgresMetadataRepositoryTest, RegisterObjectLayoutIsIdempotent) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    register_test_object(repository);

    const auto descriptor = make_object_layout_descriptor();

    repository.register_object_layout(descriptor);

    repository.register_object_layout(descriptor);

    pqxx::connection connection{
        test_database_connection_string(),
    };

    pqxx::work transaction{connection};

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) "
                                                 "FROM object_layouts"),
              1);

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) "
                                                 "FROM chunks"),
              2);

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) "
                                                 "FROM object_layout_chunks"),
              2);

    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, RegisterObjectLayoutRollsBackOnChunkSizeConflict) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    register_test_object(repository);

    {
        pqxx::connection connection{
            test_database_connection_string(),
        };

        pqxx::work transaction{connection};

        transaction
            .exec(
                "INSERT INTO chunks ("
                "    chunk_id, "
                "    size_bytes"
                ") "
                "VALUES ("
                "    $1, "
                "    $2"
                ")",
                pqxx::params{
                    kChunkB,
                    99,
                })
            .no_rows();

        transaction.commit();
    }

    const auto descriptor = make_object_layout_descriptor();

    EXPECT_THROW(repository.register_object_layout(descriptor), std::runtime_error);

    pqxx::connection connection{
        test_database_connection_string(),
    };

    pqxx::work transaction{connection};

    const bool chunk_a_exists = transaction.query_value<bool>(
        "SELECT EXISTS ("
        "    SELECT 1 "
        "    FROM chunks "
        "    WHERE chunk_id = $1"
        ")",
        pqxx::params{
            kChunkA,
        });

    const bool layout_exists = transaction.query_value<bool>(
        "SELECT EXISTS ("
        "    SELECT 1 "
        "    FROM object_layouts "
        "    WHERE layout_id = $1"
        ")",
        pqxx::params{
            descriptor.layout_id(),
        });

    EXPECT_FALSE(chunk_a_exists);

    EXPECT_FALSE(layout_exists);

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) "
                                                 "FROM objects"),
              1);

    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, MissingObjectLayoutReturnsNullopt) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const auto restored = repository.get_object_layout(std::string(64, 'e'));

    EXPECT_FALSE(restored.has_value());
}

TEST(PostgresMetadataRepositoryTest, SupportsMultipleLayoutsForSameObject) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    register_test_object(repository);

    const auto first = make_object_layout_descriptor();

    const auto second = make_alternate_object_layout_descriptor();

    repository.register_object_layout(first);

    repository.register_object_layout(second);

    EXPECT_EQ(first.object_id(), second.object_id());

    EXPECT_NE(first.layout_id(), second.layout_id());

    auto restored = repository.get_object_layouts(kObjectId);

    ASSERT_EQ(restored.size(), 2U);

    std::vector<std::string> restored_layout_ids;

    for (const auto& descriptor : restored) {
        EXPECT_EQ(descriptor.object_id(), kObjectId);

        restored_layout_ids.push_back(descriptor.layout_id());
    }

    std::vector<std::string> expected_layout_ids{
        first.layout_id(),
        second.layout_id(),
    };

    std::sort(restored_layout_ids.begin(), restored_layout_ids.end());

    std::sort(expected_layout_ids.begin(), expected_layout_ids.end());

    EXPECT_EQ(restored_layout_ids, expected_layout_ids);
}

TEST(PostgresMetadataRepositoryTest, RegisterObjectLayoutRequiresRegisteredObject) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const auto descriptor = make_object_layout_descriptor();

    EXPECT_THROW(repository.register_object_layout(descriptor), std::runtime_error);

    pqxx::connection connection{
        test_database_connection_string(),
    };

    pqxx::work transaction{connection};

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) "
                                                 "FROM chunks"),
              0);

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) "
                                                 "FROM object_layouts"),
              0);

    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, DatabaseRejectsLayoutForMissingObject) {
    reset_metadata_data();

    pqxx::connection connection{
        test_database_connection_string(),
    };

    pqxx::work transaction{connection};

    EXPECT_THROW(transaction
                     .exec("INSERT INTO object_layouts ("
                           "    layout_id, "
                           "    object_id, "
                           "    descriptor_version, "
                           "    chunking_strategy, "
                           "    canonical_descriptor, "
                           "    total_size_bytes, "
                           "    chunk_count"
                           ") "
                           "VALUES ("
                           "    $1, "
                           "    $2, "
                           "    1, "
                           "    'fixed-size', "
                           "    decode('00', 'hex'), "
                           "    0, "
                           "    0"
                           ")",
                           pqxx::params{
                               std::string(64, 'e'),
                               std::string(64, 'f'),
                           })
                     .no_rows(),
                 pqxx::foreign_key_violation);
}

TEST(PostgresMetadataRepositoryTest, DatabaseRejectsLayoutWithMismatchedObjectSize) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    repository.register_object(make_object());

    pqxx::connection connection{
        test_database_connection_string(),
    };

    pqxx::work transaction{connection};

    EXPECT_THROW(transaction
                     .exec("INSERT INTO object_layouts ("
                           "    layout_id, "
                           "    object_id, "
                           "    descriptor_version, "
                           "    chunking_strategy, "
                           "    canonical_descriptor, "
                           "    total_size_bytes, "
                           "    chunk_count"
                           ") "
                           "VALUES ("
                           "    $1, "
                           "    $2, "
                           "    1, "
                           "    'fixed-size', "
                           "    decode('00', 'hex'), "
                           "    7, "
                           "    1"
                           ")",
                           pqxx::params{
                               std::string(64, 'e'),
                               kObjectId,
                           })
                     .no_rows(),
                 pqxx::foreign_key_violation);
}

TEST(PostgresMetadataRepositoryTest, CreatesAndReadsArtifactVersion) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();

    const Artifact artifact{
        artifact_id,
        "training-checkpoint",
        "recommendation-model",
    };

    repository.create_artifact(artifact);

    const Object object = make_object();

    repository.register_object(object);

    const ArtifactVersion version{
        artifact_id, object.object_id(), std::nullopt, ArtifactVersion::ImmutableMetadata{}, VersionState::Committed,
    };

    repository.create_version(version);

    const auto restored = repository.get_version(version.version_id());

    ASSERT_TRUE(restored.has_value());

    EXPECT_EQ(restored->version_id(), version.version_id());

    EXPECT_EQ(restored->artifact_id(), artifact_id);

    EXPECT_EQ(restored->root_object_id(), object.object_id());

    EXPECT_FALSE(restored->parent_version_id().has_value());

    EXPECT_TRUE(restored->immutable_metadata().empty());

    EXPECT_EQ(restored->state(), VersionState::Committed);

    EXPECT_EQ(restored->canonical_bytes(), version.canonical_bytes());
}

TEST(PostgresMetadataRepositoryTest, PersistsParentAndImmutableMetadata) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    const Object object = make_object();

    repository.register_object(object);

    const ArtifactVersion parent{
        artifact_id,
        object.object_id(),
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{
            {"stage", "base"},
        },
        VersionState::Committed,
    };

    repository.create_version(parent);

    const ArtifactVersion child{
        artifact_id,
        object.object_id(),
        parent.version_id(),
        ArtifactVersion::ImmutableMetadata{
            {"epoch", "2"},
            {"framework", "pytorch"},
        },
        VersionState::Committed,
    };

    repository.create_version(child);

    const auto restored = repository.get_version(child.version_id());

    ASSERT_TRUE(restored.has_value());

    ASSERT_TRUE(restored->parent_version_id().has_value());

    EXPECT_EQ(*restored->parent_version_id(), parent.version_id());

    EXPECT_EQ(restored->immutable_metadata(), child.immutable_metadata());

    EXPECT_EQ(restored->version_id(), child.version_id());
}

TEST(PostgresMetadataRepositoryTest, UpdatesOperationalVersionStateWithoutChangingIdentity) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    const Object object = make_object();

    repository.register_object(object);

    const ArtifactVersion version{
        artifact_id,
        object.object_id(),
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{
            {"epoch", "1"},
        },
        VersionState::Staging,
    };

    repository.create_version(version);

    const std::string version_id = version.version_id();

    repository.set_version_state(version_id, VersionState::Committed);

    const auto restored = repository.get_version(version_id);

    ASSERT_TRUE(restored.has_value());

    EXPECT_EQ(restored->version_id(), version_id);

    EXPECT_EQ(restored->state(), VersionState::Committed);

    EXPECT_EQ(restored->artifact_id(), version.artifact_id());

    EXPECT_EQ(restored->root_object_id(), version.root_object_id());

    EXPECT_EQ(restored->parent_version_id(), version.parent_version_id());

    EXPECT_EQ(restored->immutable_metadata(), version.immutable_metadata());

    EXPECT_EQ(restored->canonical_bytes(), version.canonical_bytes());
}

TEST(PostgresMetadataRepositoryTest, MissingArtifactVersionReturnsNullopt) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const auto restored = repository.get_version(std::string(64, 'f'));

    EXPECT_FALSE(restored.has_value());
}

TEST(PostgresMetadataRepositoryTest, DatabaseRejectsVersionForMissingArtifact) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const Object object = make_object();

    repository.register_object(object);

    const ArtifactVersion version{
        UuidV7::generate(),      object.object_id(), std::nullopt, ArtifactVersion::ImmutableMetadata{},
        VersionState::Committed,
    };

    EXPECT_THROW(repository.create_version(version), pqxx::foreign_key_violation);
}

TEST(PostgresMetadataRepositoryTest, DatabaseRejectsVersionForMissingRootObject) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    const ArtifactVersion version{
        artifact_id, std::string(64, 'e'), std::nullopt, ArtifactVersion::ImmutableMetadata{}, VersionState::Committed,
    };

    EXPECT_THROW(repository.create_version(version), pqxx::foreign_key_violation);
}

TEST(PostgresMetadataRepositoryTest, DatabaseRejectsParentVersionFromDifferentArtifact) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_a_id = UuidV7::generate();

    const UuidV7 artifact_b_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_a_id,
        "checkpoint-a",
        "training-project",
    });

    repository.create_artifact(Artifact{
        artifact_b_id,
        "checkpoint-b",
        "training-project",
    });

    const Object object = make_object();

    repository.register_object(object);

    const ArtifactVersion parent{
        artifact_a_id, object.object_id(), std::nullopt, ArtifactVersion::ImmutableMetadata{}, VersionState::Committed,
    };

    repository.create_version(parent);

    const ArtifactVersion child{
        artifact_b_id,           object.object_id(), parent.version_id(), ArtifactVersion::ImmutableMetadata{},
        VersionState::Committed,
    };

    EXPECT_THROW(repository.create_version(child), pqxx::foreign_key_violation);
}

TEST(PostgresMetadataRepositoryTest, DuplicateSemanticVersionIsRejected) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    const Object object = make_object();

    repository.register_object(object);

    const ArtifactVersion first{
        artifact_id,
        object.object_id(),
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{
            {"epoch", "1"},
        },
        VersionState::Staging,
    };

    const ArtifactVersion second{
        artifact_id,
        object.object_id(),
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{
            {"epoch", "1"},
        },
        VersionState::Committed,
    };

    EXPECT_EQ(first.version_id(), second.version_id());

    repository.create_version(first);

    EXPECT_THROW(repository.create_version(second), pqxx::unique_violation);
}

TEST(PostgresMetadataRepositoryTest, DetectsCorruptedStoredVersionDescriptor) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    const Object object = make_object();

    repository.register_object(object);

    const ArtifactVersion version{
        artifact_id, object.object_id(), std::nullopt, ArtifactVersion::ImmutableMetadata{}, VersionState::Committed,
    };

    repository.create_version(version);

    {
        pqxx::connection connection{
            test_database_connection_string(),
        };

        pqxx::work transaction{connection};

        transaction
            .exec(
                "UPDATE artifact_versions "
                "SET canonical_descriptor = decode('00', 'hex') "
                "WHERE version_id = $1",
                pqxx::params{
                    version.version_id(),
                })
            .no_rows();

        transaction.commit();
    }

    EXPECT_THROW(static_cast<void>(repository.get_version(version.version_id())), std::runtime_error);
}

TEST(PostgresMetadataRepositoryTest, CreatesAndReadsTag) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    const Object object = make_object();

    repository.register_object(object);

    const ArtifactVersion version{
        artifact_id, object.object_id(), std::nullopt, ArtifactVersion::ImmutableMetadata{}, VersionState::Committed,
    };

    repository.create_version(version);

    repository.set_tag(artifact_id, "latest", version.version_id());

    const auto restored = repository.get_tag(artifact_id, "latest");

    ASSERT_TRUE(restored.has_value());

    EXPECT_EQ(*restored, version.version_id());
}

TEST(PostgresMetadataRepositoryTest, UpdatingTagMovesExistingReference) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    const Object object = make_object();

    repository.register_object(object);

    const ArtifactVersion first_version{
        artifact_id,
        object.object_id(),
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{
            {"epoch", "1"},
        },
        VersionState::Committed,
    };

    const ArtifactVersion second_version{
        artifact_id,
        object.object_id(),
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{
            {"epoch", "2"},
        },
        VersionState::Committed,
    };

    EXPECT_NE(first_version.version_id(), second_version.version_id());

    repository.create_version(first_version);

    repository.create_version(second_version);

    repository.set_tag(artifact_id, "latest", first_version.version_id());

    repository.set_tag(artifact_id, "latest", second_version.version_id());

    const auto restored = repository.get_tag(artifact_id, "latest");

    ASSERT_TRUE(restored.has_value());

    EXPECT_EQ(*restored, second_version.version_id());

    pqxx::connection connection{
        test_database_connection_string(),
    };

    pqxx::work transaction{connection};

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) "
                                                 "FROM tags "
                                                 "WHERE artifact_id = $1::uuid "
                                                 "  AND tag_name = $2",
                                                 pqxx::params{
                                                     artifact_id.str(),
                                                     "latest",
                                                 }),
              1);

    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, MissingTagReturnsNullopt) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const auto restored = repository.get_tag(UuidV7::generate(), "latest");

    EXPECT_FALSE(restored.has_value());
}

TEST(PostgresMetadataRepositoryTest, DatabaseRejectsTagPointingToDifferentArtifactVersion) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_a_id = UuidV7::generate();

    const UuidV7 artifact_b_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_a_id,
        "checkpoint-a",
        "training-project",
    });

    repository.create_artifact(Artifact{
        artifact_b_id,
        "checkpoint-b",
        "training-project",
    });

    const Object object = make_object();

    repository.register_object(object);

    const ArtifactVersion version_a{
        artifact_a_id, object.object_id(), std::nullopt, ArtifactVersion::ImmutableMetadata{}, VersionState::Committed,
    };

    repository.create_version(version_a);

    EXPECT_THROW(repository.set_tag(artifact_b_id, "latest", version_a.version_id()), pqxx::foreign_key_violation);

    const auto restored = repository.get_tag(artifact_b_id, "latest");

    EXPECT_FALSE(restored.has_value());
}

TEST(PostgresMetadataRepositoryTest, RegistersAndReadsStorageLocation) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    register_test_object_and_layout(repository);

    repository.register_storage_location(StorageLocation{
        .chunk_id = kChunkA,
        .node_id = "node-a",
        .storage_path = "/data/chunks/aa/chunk-a",
        .state = StorageLocationState::Available,
    });

    const auto locations = repository.get_storage_locations(kChunkA);

    ASSERT_EQ(locations.size(), 1U);

    EXPECT_EQ(locations[0].chunk_id, kChunkA);

    EXPECT_EQ(locations[0].node_id, "node-a");

    EXPECT_EQ(locations[0].storage_path, "/data/chunks/aa/chunk-a");

    EXPECT_EQ(locations[0].state, StorageLocationState::Available);
}

TEST(PostgresMetadataRepositoryTest, UpdatingStorageLocationReusesChunkNodeIdentity) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    register_test_object_and_layout(repository);

    repository.register_storage_location(StorageLocation{
        .chunk_id = kChunkA,
        .node_id = "node-a",
        .storage_path = "/data/chunks/aa/chunk-a",
        .state = StorageLocationState::Available,
    });

    repository.register_storage_location(StorageLocation{
        .chunk_id = kChunkA,
        .node_id = "node-a",
        .storage_path = "/replacement/chunks/aa/chunk-a",
        .state = StorageLocationState::Missing,
    });

    const auto locations = repository.get_storage_locations(kChunkA);

    ASSERT_EQ(locations.size(), 1U);

    EXPECT_EQ(locations[0].storage_path, "/replacement/chunks/aa/chunk-a");

    EXPECT_EQ(locations[0].state, StorageLocationState::Missing);

    pqxx::connection connection{
        test_database_connection_string(),
    };

    pqxx::work transaction{connection};

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) "
                                                 "FROM storage_locations "
                                                 "WHERE chunk_id = $1 "
                                                 "  AND node_id = $2",
                                                 pqxx::params{
                                                     kChunkA,
                                                     "node-a",
                                                 }),
              1);

    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, PreservesStorageLocationStates) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    register_test_object_and_layout(repository);

    repository.register_storage_location(StorageLocation{
        .chunk_id = kChunkA,
        .node_id = "node-available",
        .storage_path = "/store/available",
        .state = StorageLocationState::Available,
    });

    repository.register_storage_location(StorageLocation{
        .chunk_id = kChunkA,
        .node_id = "node-missing",
        .storage_path = "/store/missing",
        .state = StorageLocationState::Missing,
    });

    repository.register_storage_location(StorageLocation{
        .chunk_id = kChunkA,
        .node_id = "node-corrupt",
        .storage_path = "/store/corrupt",
        .state = StorageLocationState::Corrupt,
    });

    const auto locations = repository.get_storage_locations(kChunkA);

    ASSERT_EQ(locations.size(), 3U);

    EXPECT_EQ(locations[0].state, StorageLocationState::Available);

    EXPECT_EQ(locations[1].state, StorageLocationState::Corrupt);

    EXPECT_EQ(locations[2].state, StorageLocationState::Missing);
}

TEST(PostgresMetadataRepositoryTest, DatabaseRejectsStorageLocationForMissingChunk) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    EXPECT_THROW(repository.register_storage_location(StorageLocation{
                     .chunk_id = std::string(64, 'c'),
                     .node_id = "node-a",
                     .storage_path = "/data/missing",
                     .state = StorageLocationState::Available,
                 }),
                 pqxx::foreign_key_violation);
}

TEST(PostgresMetadataRepositoryTest, DatabaseRejectsSameNodePathForDifferentChunks) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    register_test_object_and_layout(repository);

    repository.register_storage_location(StorageLocation{
        .chunk_id = kChunkA,
        .node_id = "node-a",
        .storage_path = "/data/shared-path",
        .state = StorageLocationState::Available,
    });

    EXPECT_THROW(repository.register_storage_location(StorageLocation{
                     .chunk_id = kChunkB,
                     .node_id = "node-a",
                     .storage_path = "/data/shared-path",
                     .state = StorageLocationState::Available,
                 }),
                 pqxx::unique_violation);
}

TEST(PostgresMetadataRepositoryTest, RegistersAndReadsManifest) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    register_test_object(repository);

    const ArtifactVersion dataset_version = create_manifest_test_version(repository, artifact_id, "dataset");

    const ArtifactVersion model_version = create_manifest_test_version(repository, artifact_id, "model");

    const Manifest manifest{
        Manifest::Entries{
            {"dataset", dataset_version.version_id()},
            {"model", model_version.version_id()},
        },
    };

    repository.register_manifest(manifest);

    const auto restored = repository.get_manifest(manifest.manifest_id());

    ASSERT_TRUE(restored.has_value());

    EXPECT_EQ(restored->manifest_id(), manifest.manifest_id());

    EXPECT_EQ(restored->entries(), manifest.entries());

    EXPECT_EQ(restored->canonical_bytes(), manifest.canonical_bytes());
}

TEST(PostgresMetadataRepositoryTest, RegisterManifestIsIdempotent) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    register_test_object(repository);

    const ArtifactVersion dataset_version = create_manifest_test_version(repository, artifact_id, "dataset");

    const ArtifactVersion model_version = create_manifest_test_version(repository, artifact_id, "model");

    const Manifest manifest{
        Manifest::Entries{
            {"dataset", dataset_version.version_id()},
            {"model", model_version.version_id()},
        },
    };

    repository.register_manifest(manifest);

    repository.register_manifest(manifest);

    pqxx::connection connection{
        test_database_connection_string(),
    };

    pqxx::work transaction{connection};

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) "
                                                 "FROM manifests"),
              1);

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) "
                                                 "FROM manifest_entries"),
              static_cast<long long>(manifest.entries().size()));

    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, DatabaseRejectsManifestEntryForMissingVersion) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const Manifest manifest{
        Manifest::Entries{
            {"model", std::string(64, 'f')},
        },
    };

    EXPECT_THROW(repository.register_manifest(manifest), pqxx::foreign_key_violation);

    pqxx::connection connection{
        test_database_connection_string(),
    };

    pqxx::work transaction{connection};

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) "
                                                 "FROM manifests"),
              0);

    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, DetectsCorruptedStoredManifestDescriptor) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    register_test_object(repository);

    const ArtifactVersion model_version = create_manifest_test_version(repository, artifact_id, "model");

    const Manifest manifest{
        Manifest::Entries{
            {"model", model_version.version_id()},
        },
    };

    repository.register_manifest(manifest);

    {
        pqxx::connection connection{
            test_database_connection_string(),
        };

        pqxx::work transaction{connection};

        transaction
            .exec(
                "UPDATE manifests "
                "SET canonical_descriptor = decode('00', 'hex') "
                "WHERE manifest_id = $1",
                pqxx::params{
                    manifest.manifest_id(),
                })
            .no_rows();

        transaction.commit();
    }

    EXPECT_THROW(static_cast<void>(repository.get_manifest(manifest.manifest_id())), std::runtime_error);
}

TEST(PostgresMetadataRepositoryTest, CreatesAndReadsRun) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    register_test_object(repository);

    const ArtifactVersion model_version = create_manifest_test_version(repository, artifact_id, "model");

    const Manifest manifest{
        Manifest::Entries{
            {"model", model_version.version_id()},
        },
    };

    repository.register_manifest(manifest);

    const UuidV7 run_id = UuidV7::generate();

    const RunModel run{
        run_id,
        manifest.manifest_id(),
        "training-run",
        std::string{"abc123"},
        RunModel::Tags{
            {"environment", "dev"},
            {"owner", "ml-team"},
        },
        RunModel::Timestamp{
            std::chrono::milliseconds{
                1700000000000LL,
            },
        },
        RunModel::Timestamp{
            std::chrono::milliseconds{
                1700000060000LL,
            },
        },
    };

    repository.create_run(run);

    const auto restored = repository.get_run(run_id);

    ASSERT_TRUE(restored.has_value());

    EXPECT_EQ(restored->run_id(), run_id);

    EXPECT_EQ(restored->manifest_id(), manifest.manifest_id());

    EXPECT_EQ(restored->name(), "training-run");

    ASSERT_TRUE(restored->source_commit().has_value());

    EXPECT_EQ(*restored->source_commit(), "abc123");

    EXPECT_EQ(restored->tags(), (RunModel::Tags{
                                    {"environment", "dev"},
                                    {"owner", "ml-team"},
                                }));

    EXPECT_EQ(restored->started_at(), RunModel::Timestamp{std::chrono::milliseconds{1700000000000LL}});

    ASSERT_TRUE(restored->completed_at().has_value());

    EXPECT_EQ(*restored->completed_at(), RunModel::Timestamp{std::chrono::milliseconds{1700000060000LL}});
}

TEST(PostgresMetadataRepositoryTest, DifferentRunsCanReferenceSameManifest) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    register_test_object(repository);

    const ArtifactVersion model_version = create_manifest_test_version(repository, artifact_id, "model");

    const Manifest manifest{
        Manifest::Entries{
            {"model", model_version.version_id()},
        },
    };

    repository.register_manifest(manifest);

    const RunModel::Timestamp started_at{
        std::chrono::milliseconds{
            1700000000000LL,
        },
    };

    const RunModel first{
        UuidV7::generate(), manifest.manifest_id(), "training-run-a", std::nullopt, RunModel::Tags{},
        started_at,         std::nullopt,
    };

    const RunModel second{
        UuidV7::generate(), manifest.manifest_id(), "training-run-b", std::nullopt, RunModel::Tags{},
        started_at,         std::nullopt,
    };

    repository.create_run(first);

    repository.create_run(second);

    EXPECT_NE(first.run_id(), second.run_id());

    EXPECT_EQ(first.manifest_id(), second.manifest_id());

    pqxx::connection connection{
        test_database_connection_string(),
    };

    pqxx::work transaction{connection};

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) "
                                                 "FROM runs "
                                                 "WHERE manifest_id = $1",
                                                 pqxx::params{
                                                     manifest.manifest_id(),
                                                 }),
              2);

    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, DatabaseRejectsRunForMissingManifest) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const RunModel run{
        UuidV7::generate(),
        std::string(64, 'e'),
        "training-run",
        std::nullopt,
        RunModel::Tags{},
        RunModel::Timestamp{
            std::chrono::milliseconds{
                1700000000000LL,
            },
        },
        std::nullopt,
    };

    EXPECT_THROW(repository.create_run(run), pqxx::foreign_key_violation);
}

TEST(PostgresMetadataRepositoryTest, DuplicateRunIdIsRejected) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    register_test_object(repository);

    const ArtifactVersion model_version = create_manifest_test_version(repository, artifact_id, "model");

    const Manifest manifest{
        Manifest::Entries{
            {"model", model_version.version_id()},
        },
    };

    repository.register_manifest(manifest);

    const UuidV7 run_id = UuidV7::generate();

    const RunModel first{
        run_id,
        manifest.manifest_id(),
        "training-run",
        std::nullopt,
        RunModel::Tags{},
        RunModel::Timestamp{
            std::chrono::milliseconds{
                1700000000000LL,
            },
        },
        std::nullopt,
    };

    const RunModel second{
        run_id,
        manifest.manifest_id(),
        "training-run",
        std::nullopt,
        RunModel::Tags{},
        RunModel::Timestamp{
            std::chrono::milliseconds{
                1700000000000LL,
            },
        },
        std::nullopt,
    };

    repository.create_run(first);

    EXPECT_THROW(repository.create_run(second), pqxx::unique_violation);
}

TEST(PostgresMetadataRepositoryTest, RegistersAndReadsIndependentChunkMetadata) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    repository.register_chunks({
        ChunkMetadata{
            .chunk_id = kChunkA,
            .size_bytes = 4,
        },
        ChunkMetadata{
            .chunk_id = kChunkB,
            .size_bytes = 2,
        },
    });

    const auto size_a = repository.get_chunk_size(kChunkA);
    const auto size_b = repository.get_chunk_size(kChunkB);

    ASSERT_TRUE(size_a.has_value());
    ASSERT_TRUE(size_b.has_value());

    EXPECT_EQ(*size_a, 4U);
    EXPECT_EQ(*size_b, 2U);
}

TEST(PostgresMetadataRepositoryTest, ChunkRegistrationIsIdempotent) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const ChunkMetadata chunk{
        .chunk_id = kChunkA,
        .size_bytes = 4,
    };

    repository.register_chunks({chunk});
    repository.register_chunks({chunk});

    const auto size = repository.get_chunk_size(kChunkA);

    ASSERT_TRUE(size.has_value());
    EXPECT_EQ(*size, 4U);

    pqxx::connection connection{
        test_database_connection_string(),
    };

    pqxx::work transaction{connection};

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) "
                                                 "FROM chunks "
                                                 "WHERE chunk_id = $1",
                                                 pqxx::params{
                                                     kChunkA,
                                                 }),
              1);

    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, ChunkRegistrationRejectsConflictingSize) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    repository.register_chunks({
        ChunkMetadata{
            .chunk_id = kChunkA,
            .size_bytes = 4,
        },
    });

    EXPECT_THROW(repository.register_chunks({
                     ChunkMetadata{
                         .chunk_id = kChunkA,
                         .size_bytes = 8,
                     },
                 }),
                 std::runtime_error);

    const auto size = repository.get_chunk_size(kChunkA);

    ASSERT_TRUE(size.has_value());
    EXPECT_EQ(*size, 4U);
}

TEST(PostgresMetadataRepositoryTest, CreatesAndReadsUploadSession) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    constexpr std::uint64_t kFourMiB = 4ULL * 1024ULL * 1024ULL;

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

    repository.create_upload_session(session);

    const auto restored = repository.get_upload_session(session_id);

    ASSERT_TRUE(restored.has_value());

    EXPECT_EQ(restored->session_id(), session_id);
    EXPECT_EQ(restored->artifact_id(), artifact_id);
    EXPECT_EQ(restored->target_node_id(), "node-a");
    EXPECT_EQ(restored->chunking_strategy(), ChunkingStrategy::FixedSize);
    EXPECT_EQ(restored->chunk_size_bytes(), kFourMiB);
    EXPECT_FALSE(restored->parent_version_id().has_value());
    EXPECT_EQ(restored->immutable_metadata().at("framework"), "pytorch");
    EXPECT_EQ(restored->immutable_metadata().at("kind"), "checkpoint");
    EXPECT_EQ(restored->state(), UploadSessionState::Open);
    EXPECT_FALSE(restored->finalized_version_id().has_value());
}

TEST(PostgresMetadataRepositoryTest, UploadSessionCreationIsIdempotent) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    constexpr std::uint64_t kFourMiB = 4ULL * 1024ULL * 1024ULL;

    const UploadSession session{
        session_id,
        artifact_id,
        "node-a",
        ChunkingStrategy::FixedSize,
        kFourMiB,
        std::nullopt,
        UploadSession::ImmutableMetadata{
            {"framework", "pytorch"},
        },
        UploadSessionState::Open,
        std::nullopt,
    };

    repository.create_upload_session(session);
    repository.create_upload_session(session);

    const auto restored = repository.get_upload_session(session_id);

    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->immutable_metadata().at("framework"), "pytorch");
    EXPECT_EQ(restored->state(), UploadSessionState::Open);

    pqxx::connection connection{
        test_database_connection_string(),
    };

    pqxx::work transaction{connection};

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) "
                                                 "FROM upload_sessions "
                                                 "WHERE session_id = $1::uuid",
                                                 pqxx::params{
                                                     session_id.str(),
                                                 }),
              1);

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) "
                                                 "FROM upload_session_metadata "
                                                 "WHERE session_id = $1::uuid",
                                                 pqxx::params{
                                                     session_id.str(),
                                                 }),
              1);

    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, ReusedSessionIdWithDifferentPayloadIsRejected) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    constexpr std::uint64_t kFourMiB = 4ULL * 1024ULL * 1024ULL;

    const UploadSession original{
        session_id,
        artifact_id,
        "node-a",
        ChunkingStrategy::FixedSize,
        kFourMiB,
        std::nullopt,
        UploadSession::ImmutableMetadata{
            {"framework", "pytorch"},
        },
        UploadSessionState::Open,
        std::nullopt,
    };

    repository.create_upload_session(original);

    const UploadSession conflicting{
        session_id,
        artifact_id,
        "node-b",
        ChunkingStrategy::FixedSize,
        kFourMiB,
        std::nullopt,
        UploadSession::ImmutableMetadata{
            {"framework", "pytorch"},
        },
        UploadSessionState::Open,
        std::nullopt,
    };

    EXPECT_THROW(repository.create_upload_session(conflicting), std::runtime_error);

    const auto restored = repository.get_upload_session(session_id);

    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->target_node_id(), "node-a");
}

TEST(PostgresMetadataRepositoryTest, AbortsOpenUploadSessionIdempotently) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "checkpoint",
        "training-project",
    });

    constexpr std::uint64_t kFourMiB = 4ULL * 1024ULL * 1024ULL;

    repository.create_upload_session(UploadSession{
        session_id,
        artifact_id,
        "node-a",
        ChunkingStrategy::FixedSize,
        kFourMiB,
        std::nullopt,
        UploadSession::ImmutableMetadata{
            {"framework", "pytorch"},
        },
        UploadSessionState::Open,
        std::nullopt,
    });

    repository.abort_upload_session(session_id);

    auto restored = repository.get_upload_session(session_id);

    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->state(), UploadSessionState::Aborted);
    EXPECT_EQ(restored->immutable_metadata().at("framework"), "pytorch");
    EXPECT_FALSE(restored->finalized_version_id().has_value());

    repository.abort_upload_session(session_id);

    restored = repository.get_upload_session(session_id);

    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->state(), UploadSessionState::Aborted);
    EXPECT_EQ(restored->immutable_metadata().at("framework"), "pytorch");
}

TEST(PostgresMetadataRepositoryTest, DatabaseRejectsUploadSessionForMissingArtifact) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    constexpr std::uint64_t kFourMiB = 4ULL * 1024ULL * 1024ULL;

    EXPECT_THROW(repository.create_upload_session(UploadSession{
                     UuidV7::generate(),
                     UuidV7::generate(),
                     "node-a",
                     ChunkingStrategy::FixedSize,
                     kFourMiB,
                     std::nullopt,
                     UploadSession::ImmutableMetadata{},
                     UploadSessionState::Open,
                     std::nullopt,
                 }),
                 pqxx::foreign_key_violation);
}

TEST(PostgresMetadataRepositoryTest, DatabaseRejectsUploadSessionParentVersionFromDifferentArtifact) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_a = UuidV7::generate();
    const UuidV7 artifact_b = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_a,
        "checkpoint-a",
        "training-project",
    });

    repository.create_artifact(Artifact{
        artifact_b,
        "checkpoint-b",
        "training-project",
    });

    register_test_object_and_layout(repository);

    const ArtifactVersion version_b{
        artifact_b,
        kObjectId,
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{
            {"epoch", "1"},
        },
        VersionState::Committed,
    };

    repository.create_version(version_b);

    constexpr std::uint64_t kFourMiB = 4ULL * 1024ULL * 1024ULL;

    EXPECT_THROW(repository.create_upload_session(UploadSession{
                     UuidV7::generate(),
                     artifact_a,
                     "node-a",
                     ChunkingStrategy::FixedSize,
                     kFourMiB,
                     version_b.version_id(),
                     UploadSession::ImmutableMetadata{},
                     UploadSessionState::Open,
                     std::nullopt,
                 }),
                 pqxx::foreign_key_violation);
}

TEST(PostgresMetadataRepositoryTest, FinalizesUploadAtomically) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const auto descriptor = make_finalize_descriptor('2', '3', '4');

    repository.create_artifact(Artifact{artifact_id, "finalize-" + marker.str(), "m4s5"});
    repository.create_upload_session(make_finalize_session(session_id, artifact_id, "m4s5-target", marker.str()));
    register_finalize_chunks(repository, descriptor, "m4s5-target", StorageLocationState::Available);

    const auto result = repository.finalize_upload(session_id, descriptor);
    const auto session = repository.get_upload_session(session_id);
    const auto version = repository.get_version(result.version_id);

    ASSERT_TRUE(session.has_value());
    ASSERT_TRUE(version.has_value());
    EXPECT_EQ(result.session_id, session_id);
    EXPECT_EQ(result.object_id, descriptor.object_id());
    EXPECT_EQ(result.layout_id, descriptor.layout_id());
    EXPECT_EQ(session->state(), UploadSessionState::Committed);
    EXPECT_EQ(session->finalized_version_id(), result.version_id);
    EXPECT_EQ(version->root_object_id(), descriptor.object_id());
    EXPECT_EQ(version->immutable_metadata().at("marker"), marker.str());
    EXPECT_EQ(version->state(), VersionState::Committed);
    ASSERT_TRUE(repository.get_object(descriptor.object_id()).has_value());
    ASSERT_TRUE(repository.get_object_layout(descriptor.layout_id()).has_value());
}

TEST(PostgresMetadataRepositoryTest, FinalizeUploadIsIdempotent) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const auto descriptor = make_finalize_descriptor('5', '6', '7');

    repository.create_artifact(Artifact{artifact_id, "idempotent-" + marker.str(), "m4s5"});
    repository.create_upload_session(make_finalize_session(session_id, artifact_id, "m4s5-target", marker.str()));
    register_finalize_chunks(repository, descriptor, "m4s5-target", StorageLocationState::Available);

    const auto first = repository.finalize_upload(session_id, descriptor);
    const auto second = repository.finalize_upload(session_id, descriptor);

    EXPECT_EQ(second.version_id, first.version_id);
    EXPECT_EQ(second.object_id, first.object_id);
    EXPECT_EQ(second.layout_id, first.layout_id);

    pqxx::connection connection{test_database_connection_string()};
    pqxx::work transaction{connection};
    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) FROM artifact_versions WHERE version_id = $1",
                                                 pqxx::params{first.version_id}),
              1);
    EXPECT_EQ(transaction.query_value<long long>(
                  "SELECT COUNT(*) FROM upload_session_finalizations WHERE session_id = $1::uuid",
                  pqxx::params{session_id.str()}),
              1);
    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, CommittedSessionDifferentLayoutConflicts) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const auto first = make_finalize_descriptor('8', '9', 'a');
    const auto different = make_finalize_descriptor('8', 'b', 'c');

    repository.create_artifact(Artifact{artifact_id, "layout-conflict-" + marker.str(), "m4s5"});
    repository.create_upload_session(make_finalize_session(session_id, artifact_id, "m4s5-target", marker.str()));
    register_finalize_chunks(repository, first, "m4s5-target", StorageLocationState::Available);
    register_finalize_chunks(repository, different, "m4s5-target", StorageLocationState::Available);
    (void)repository.finalize_upload(session_id, first);

    try {
        (void)repository.finalize_upload(session_id, different);
        FAIL() << "expected FinalizeUploadError";
    } catch (const FinalizeUploadError& error) {
        EXPECT_EQ(error.kind(), FinalizeUploadErrorKind::Conflict);
    }
}

TEST(PostgresMetadataRepositoryTest, AbortedSessionCannotFinalize) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const auto descriptor = make_finalize_descriptor('d', 'e', 'f');

    repository.create_artifact(Artifact{artifact_id, "aborted-" + marker.str(), "m4s5"});
    repository.create_upload_session(make_finalize_session(session_id, artifact_id, "m4s5-target", marker.str()));
    repository.abort_upload_session(session_id);

    try {
        (void)repository.finalize_upload(session_id, descriptor);
        FAIL() << "expected FinalizeUploadError";
    } catch (const FinalizeUploadError& error) {
        EXPECT_EQ(error.kind(), FinalizeUploadErrorKind::SessionNotOpen);
    }
}

TEST(PostgresMetadataRepositoryTest, FinalizeRequiresAvailableLocationOnTarget) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const auto descriptor = make_finalize_descriptor('1', '2', '3');

    repository.create_artifact(Artifact{artifact_id, "missing-target-" + marker.str(), "m4s5"});
    repository.create_upload_session(make_finalize_session(session_id, artifact_id, "m4s5-target", marker.str()));
    register_finalize_chunks(repository, descriptor, "m4s5-target", StorageLocationState::Missing);

    try {
        (void)repository.finalize_upload(session_id, descriptor);
        FAIL() << "expected FinalizeUploadError";
    } catch (const FinalizeUploadError& error) {
        EXPECT_EQ(error.kind(), FinalizeUploadErrorKind::ChunkNotAvailableOnTarget);
        ASSERT_TRUE(error.chunk_id().has_value());
    }
}

TEST(PostgresMetadataRepositoryTest, AvailableElsewhereDoesNotSatisfyFinalize) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const auto descriptor = make_finalize_descriptor('4', '5', '6');

    repository.create_artifact(Artifact{artifact_id, "elsewhere-" + marker.str(), "m4s5"});
    repository.create_upload_session(make_finalize_session(session_id, artifact_id, "m4s5-target", marker.str()));
    register_finalize_chunks(repository, descriptor, "m4s5-other", StorageLocationState::Available);

    try {
        (void)repository.finalize_upload(session_id, descriptor);
        FAIL() << "expected FinalizeUploadError";
    } catch (const FinalizeUploadError& error) {
        EXPECT_EQ(error.kind(), FinalizeUploadErrorKind::ChunkNotAvailableOnTarget);
    }
}

TEST(PostgresMetadataRepositoryTest, LateVersionConflictRollsBackObjectAndLayout) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const auto descriptor = make_finalize_descriptor('7', '8', '9');
    const UploadSession session = make_finalize_session(session_id, artifact_id, "m4s5-target", marker.str());
    const ArtifactVersion expected{artifact_id, descriptor.object_id(), std::nullopt, session.immutable_metadata(),
                                   VersionState::Committed};
    const Object conflicting_object{std::string(64, 'a'), 1};
    const ArtifactVersion conflicting{
        artifact_id, conflicting_object.object_id(), std::nullopt, {}, VersionState::Committed};

    repository.create_artifact(Artifact{artifact_id, "late-conflict-" + marker.str(), "m4s5"});
    repository.create_upload_session(session);
    register_finalize_chunks(repository, descriptor, "m4s5-target", StorageLocationState::Available);
    repository.register_object(conflicting_object);

    {
        pqxx::connection connection{test_database_connection_string()};
        pqxx::work transaction{connection};
        transaction
            .exec(
                "INSERT INTO artifact_versions (version_id, artifact_id, root_object_id, parent_version_id, "
                "descriptor_version, canonical_descriptor, state) "
                "VALUES ($1, $2::uuid, $3, NULL, $4, $5::bytea, 'committed')",
                pqxx::params{expected.version_id(), artifact_id.str(), conflicting_object.object_id(),
                             static_cast<int>(ArtifactVersion::kFormatVersion), conflicting.canonical_bytes()})
            .no_rows();
        transaction.commit();
    }

    try {
        (void)repository.finalize_upload(session_id, descriptor);
        FAIL() << "expected FinalizeUploadError";
    } catch (const FinalizeUploadError& error) {
        EXPECT_EQ(error.kind(), FinalizeUploadErrorKind::Conflict);
    }

    EXPECT_FALSE(repository.get_object(descriptor.object_id()).has_value());
    EXPECT_FALSE(repository.get_object_layout(descriptor.layout_id()).has_value());
    const auto stored_session = repository.get_upload_session(session_id);
    ASSERT_TRUE(stored_session.has_value());
    EXPECT_EQ(stored_session->state(), UploadSessionState::Open);

    pqxx::connection connection{test_database_connection_string()};
    pqxx::work transaction{connection};
    EXPECT_EQ(transaction.query_value<long long>(
                  "SELECT COUNT(*) FROM upload_session_finalizations WHERE session_id = $1::uuid",
                  pqxx::params{session_id.str()}),
              0);
    transaction.exec("DELETE FROM artifact_versions WHERE version_id = $1", pqxx::params{expected.version_id()})
        .no_rows();
    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, FinalizesEmptyObject) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const ObjectLayoutDescriptor descriptor{
        Object{std::string(64, 'b'), 0},
        ChunkingStrategy::FixedSize,
        ObjectLayout{{}},
    };

    repository.create_artifact(Artifact{artifact_id, "empty-" + marker.str(), "m4s5"});
    repository.create_upload_session(make_finalize_session(session_id, artifact_id, "m4s5-target", marker.str()));

    const auto result = repository.finalize_upload(session_id, descriptor);
    EXPECT_EQ(result.object_id, descriptor.object_id());
    EXPECT_EQ(result.layout_id, descriptor.layout_id());
    ASSERT_TRUE(repository.get_version(result.version_id).has_value());
}

TEST(PostgresMetadataRepositoryTest, DifferentSessionsReuseIdenticalArtifactVersion) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 first_session_id = UuidV7::generate();
    const UuidV7 second_session_id = UuidV7::generate();
    const auto descriptor = make_finalize_descriptor('c', 'd', 'e');

    repository.create_artifact(Artifact{artifact_id, "version-reuse-" + marker.str(), "m4s5"});
    repository.create_upload_session(make_finalize_session(first_session_id, artifact_id, "m4s5-target", marker.str()));
    repository.create_upload_session(
        make_finalize_session(second_session_id, artifact_id, "m4s5-target", marker.str()));
    register_finalize_chunks(repository, descriptor, "m4s5-target", StorageLocationState::Available);

    const auto first = repository.finalize_upload(first_session_id, descriptor);
    const auto second = repository.finalize_upload(second_session_id, descriptor);
    EXPECT_EQ(second.version_id, first.version_id);

    pqxx::connection connection{test_database_connection_string()};
    pqxx::work transaction{connection};
    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) FROM artifact_versions WHERE version_id = $1",
                                                 pqxx::params{first.version_id}),
              1);
    EXPECT_EQ(transaction.query_value<long long>(
                  "SELECT COUNT(*) FROM upload_session_finalizations WHERE session_id IN ($1::uuid, $2::uuid)",
                  pqxx::params{first_session_id.str(), second_session_id.str()}),
              2);
    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, ResolveRestorePlanReturnsLexicographicallyFirstFullyAvailableLayout) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const std::string source_node = "m5s5-source";

    repository.create_artifact(Artifact{artifact_id, "restore-lex-" + marker.str(), "m5s5"});
    repository.register_object(make_object());

    const auto primary = make_object_layout_descriptor();
    const auto alternate = make_alternate_object_layout_descriptor();
    repository.register_object_layout(primary);
    repository.register_object_layout(alternate);
    register_finalize_chunks(repository, primary, source_node, StorageLocationState::Available);
    register_finalize_chunks(repository, alternate, source_node, StorageLocationState::Available);

    const ArtifactVersion version{
        artifact_id,
        kObjectId,
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{{"marker", marker.str()}},
        VersionState::Committed,
    };
    repository.create_version(version);

    const std::string expected_layout_id = std::min(primary.layout_id(), alternate.layout_id());
    const RestorePlan plan = repository.resolve_restore_plan(version.version_id(), source_node);

    EXPECT_EQ(plan.artifact_id, artifact_id);
    EXPECT_EQ(plan.version_id, version.version_id());
    EXPECT_EQ(plan.source_node_id, source_node);
    EXPECT_EQ(plan.layout_descriptor.layout_id(), expected_layout_id);
    EXPECT_EQ(plan.layout_descriptor.object_id(), kObjectId);
}

TEST(PostgresMetadataRepositoryTest, ResolveRestorePlanSkipsUnavailableLayoutAndSelectsNext) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const std::string source_node = "m5s5-source";

    repository.create_artifact(Artifact{artifact_id, "restore-skip-" + marker.str(), "m5s5"});
    repository.register_object(make_object());

    const auto primary = make_object_layout_descriptor();
    const auto alternate = make_alternate_object_layout_descriptor();
    repository.register_object_layout(primary);
    repository.register_object_layout(alternate);

    const auto& smaller = primary.layout_id() < alternate.layout_id() ? primary : alternate;
    const auto& larger = primary.layout_id() < alternate.layout_id() ? alternate : primary;
    register_finalize_chunks(repository, smaller, source_node, StorageLocationState::Missing);
    register_finalize_chunks(repository, larger, source_node, StorageLocationState::Available);

    const ArtifactVersion version{
        artifact_id,
        kObjectId,
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{{"marker", marker.str()}},
        VersionState::Committed,
    };
    repository.create_version(version);

    const RestorePlan plan = repository.resolve_restore_plan(version.version_id(), source_node);
    EXPECT_EQ(plan.layout_descriptor.layout_id(), larger.layout_id());
}

TEST(PostgresMetadataRepositoryTest, ResolveRestorePlanRejectsMissingVersion) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};

    try {
        (void)repository.resolve_restore_plan(std::string(64, 'f'), "m5s5-source");
        FAIL() << "expected RestorePlanError";
    } catch (const RestorePlanError& error) {
        EXPECT_EQ(error.kind(), RestorePlanErrorKind::VersionNotFound);
    }
}

TEST(PostgresMetadataRepositoryTest, ResolveRestorePlanRejectsNonCommittedVersion) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();

    repository.create_artifact(Artifact{artifact_id, "restore-staging-" + marker.str(), "m5s5"});
    register_test_object_and_layout(repository);

    const ArtifactVersion version{
        artifact_id,           kObjectId, std::nullopt, ArtifactVersion::ImmutableMetadata{{"marker", marker.str()}},
        VersionState::Staging,
    };
    repository.create_version(version);

    try {
        (void)repository.resolve_restore_plan(version.version_id(), "m5s5-source");
        FAIL() << "expected RestorePlanError";
    } catch (const RestorePlanError& error) {
        EXPECT_EQ(error.kind(), RestorePlanErrorKind::VersionNotCommitted);
    }
}

TEST(PostgresMetadataRepositoryTest, ResolveRestorePlanRejectsWhenNoLayoutIsAvailableOnSource) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const std::string source_node = "m5s5-source";

    repository.create_artifact(Artifact{artifact_id, "restore-unavail-" + marker.str(), "m5s5"});
    register_test_object_and_layout(repository);
    const auto descriptor = make_object_layout_descriptor();
    register_finalize_chunks(repository, descriptor, "m5s5-other", StorageLocationState::Available);

    const ArtifactVersion version{
        artifact_id,
        kObjectId,
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{{"marker", marker.str()}},
        VersionState::Committed,
    };
    repository.create_version(version);

    try {
        (void)repository.resolve_restore_plan(version.version_id(), source_node);
        FAIL() << "expected RestorePlanError";
    } catch (const RestorePlanError& error) {
        EXPECT_EQ(error.kind(), RestorePlanErrorKind::SourceUnavailable);
    }
}

TEST(PostgresMetadataRepositoryTest, ResolveRestorePlanSupportsEmptyObject) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const std::string source_node = "m5s5-source";
    constexpr std::string_view kEmptyObjectId = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    repository.create_artifact(Artifact{artifact_id, "restore-empty-" + marker.str(), "m5s5"});

    const ObjectLayoutDescriptor empty_descriptor{
        Object{std::string{kEmptyObjectId}, 0},
        ChunkingStrategy::FixedSize,
        ObjectLayout{{}},
    };
    repository.register_object(empty_descriptor.object());
    repository.register_object_layout(empty_descriptor);

    const ArtifactVersion version{
        artifact_id,
        std::string{kEmptyObjectId},
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{{"marker", marker.str()}},
        VersionState::Committed,
    };
    repository.create_version(version);

    const RestorePlan plan = repository.resolve_restore_plan(version.version_id(), source_node);
    EXPECT_EQ(plan.layout_descriptor.object_id(), kEmptyObjectId);
    EXPECT_EQ(plan.layout_descriptor.object().total_size(), 0U);
    EXPECT_TRUE(plan.layout_descriptor.layout().chunks().empty());
    EXPECT_EQ(plan.layout_descriptor.layout_id(), empty_descriptor.layout_id());
}

UploadSession make_fastcdc_finalize_session(const UuidV7& session_id, const UuidV7& artifact_id,
                                            std::string target_node, std::string marker,
                                            const FastCdcParameters& parameters) {
    return UploadSession{
        session_id,
        artifact_id,
        std::move(target_node),
        parameters,
        std::nullopt,
        UploadSession::ImmutableMetadata{{"marker", std::move(marker)}},
        UploadSessionState::Open,
        std::nullopt,
    };
}

ObjectLayoutDescriptor make_fastcdc_finalize_descriptor(char object_marker, const FastCdcParameters& parameters,
                                                        char first_chunk_marker = 'a', char second_chunk_marker = 'b') {
    return ObjectLayoutDescriptor{
        Object{std::string(64, object_marker), 192},
        parameters,
        ObjectLayout{{
            ChunkRef{.chunk_id = std::string(64, first_chunk_marker), .offset = 0, .size = 128},
            ChunkRef{.chunk_id = std::string(64, second_chunk_marker), .offset = 128, .size = 64},
        }},
    };
}

constexpr FastCdcParameters kTestFastCdcParameters{
    .min_chunk_size_bytes = 64,
    .avg_chunk_size_bytes = 256,
    .max_chunk_size_bytes = 1024,
};

constexpr FastCdcParameters kAlternateFastCdcParameters{
    .min_chunk_size_bytes = 128,
    .avg_chunk_size_bytes = 512,
    .max_chunk_size_bytes = 2048,
};

TEST(PostgresMetadataRepositoryTest, RegistersAndLoadsFastCdcLayoutWithParameters) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const Object object{
        kObjectId,
        192,
    };

    repository.register_object(object);

    const ObjectLayoutDescriptor descriptor{
        object,
        kTestFastCdcParameters,
        ObjectLayout{
            {
                ChunkRef{
                    .chunk_id = kChunkA,
                    .offset = 0,
                    .size = 128,
                },
                ChunkRef{
                    .chunk_id = kChunkB,
                    .offset = 128,
                    .size = 64,
                },
            },
        },
    };

    repository.register_object_layout(descriptor);

    const auto restored = repository.get_object_layout(descriptor.layout_id());

    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->chunking_strategy(), ChunkingStrategy::FastCdc);
    ASSERT_TRUE(restored->fastcdc_parameters().has_value());
    EXPECT_EQ(*restored->fastcdc_parameters(), kTestFastCdcParameters);
    EXPECT_EQ(restored->canonical_bytes(), descriptor.canonical_bytes());
}

TEST(PostgresMetadataRepositoryTest, CreatesAndLoadsFastCdcUploadSession) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "fastcdc-checkpoint",
        "training-project",
    });

    const UploadSession session{
        session_id,
        artifact_id,
        "node-a",
        kTestFastCdcParameters,
        std::nullopt,
        UploadSession::ImmutableMetadata{
            {"framework", "pytorch"},
        },
        UploadSessionState::Open,
        std::nullopt,
    };

    repository.create_upload_session(session);

    const auto restored = repository.get_upload_session(session_id);

    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->chunking_strategy(), ChunkingStrategy::FastCdc);
    EXPECT_EQ(restored->fastcdc_parameters(), kTestFastCdcParameters);
    EXPECT_FALSE(restored->fixed_chunk_size_bytes().has_value());
    EXPECT_EQ(restored->immutable_metadata().at("framework"), "pytorch");
}

TEST(PostgresMetadataRepositoryTest, RejectsUploadSessionConflictWhenFastCdcParametersDiffer) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();

    repository.create_artifact(Artifact{
        artifact_id,
        "fastcdc-checkpoint",
        "training-project",
    });

    const UploadSession original{
        session_id,
        artifact_id,
        "node-a",
        kTestFastCdcParameters,
        std::nullopt,
        UploadSession::ImmutableMetadata{
            {"framework", "pytorch"},
        },
        UploadSessionState::Open,
        std::nullopt,
    };

    repository.create_upload_session(original);

    const UploadSession conflicting{
        session_id,
        artifact_id,
        "node-a",
        kAlternateFastCdcParameters,
        std::nullopt,
        UploadSession::ImmutableMetadata{
            {"framework", "pytorch"},
        },
        UploadSessionState::Open,
        std::nullopt,
    };

    EXPECT_THROW(repository.create_upload_session(conflicting), std::runtime_error);
}

TEST(PostgresMetadataRepositoryTest, FinalizesFastCdcUpload) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const auto descriptor = make_fastcdc_finalize_descriptor('f', kTestFastCdcParameters, 'c', 'd');

    repository.create_artifact(Artifact{artifact_id, "fastcdc-finalize-" + marker.str(), "m6-fastcdc"});
    repository.create_upload_session(make_fastcdc_finalize_session(session_id, artifact_id, "m6-fastcdc-target",
                                                                   marker.str(), kTestFastCdcParameters));
    register_finalize_chunks(repository, descriptor, "m6-fastcdc-target", StorageLocationState::Available);

    const auto result = repository.finalize_upload(session_id, descriptor);
    const auto session = repository.get_upload_session(session_id);
    const auto layout = repository.get_object_layout(descriptor.layout_id());

    ASSERT_TRUE(session.has_value());
    ASSERT_TRUE(layout.has_value());
    EXPECT_EQ(result.layout_id, descriptor.layout_id());
    EXPECT_EQ(session->state(), UploadSessionState::Committed);
    EXPECT_EQ(layout->fastcdc_parameters(), kTestFastCdcParameters);
}

TEST(PostgresMetadataRepositoryTest, FinalizeRejectsFastCdcDescriptorThatDoesNotMatchSessionConfiguration) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const auto descriptor = make_fastcdc_finalize_descriptor('e', kAlternateFastCdcParameters, '1', '2');

    repository.create_artifact(Artifact{artifact_id, "fastcdc-mismatch-" + marker.str(), "m6-fastcdc"});
    repository.create_upload_session(make_fastcdc_finalize_session(session_id, artifact_id, "m6-fastcdc-target",
                                                                   marker.str(), kTestFastCdcParameters));
    register_finalize_chunks(repository, descriptor, "m6-fastcdc-target", StorageLocationState::Available);

    EXPECT_THROW((void)repository.finalize_upload(session_id, descriptor), std::invalid_argument);
}

}  // namespace
