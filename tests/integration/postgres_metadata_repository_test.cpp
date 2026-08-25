#include "aistore/metadata/postgres_metadata_repository.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <pqxx/pqxx>
#include <string>
#include <vector>

namespace {

using aistore::metadata::Artifact;
using aistore::metadata::ArtifactVersion;
using aistore::metadata::ChunkingStrategy;
using aistore::metadata::ChunkRef;
using aistore::metadata::Object;
using aistore::metadata::ObjectLayout;
using aistore::metadata::ObjectLayoutDescriptor;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::StorageLocation;
using aistore::metadata::StorageLocationState;
using aistore::metadata::UuidV7;
using aistore::metadata::VersionState;

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
            "    tags, "
            "    storage_locations, "
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

    const UuidV7 version_id = UuidV7::generate();

    const ArtifactVersion version{
        version_id,
        artifact_id,
        object.object_id(),
        VersionState::Committed,
    };

    repository.create_version(version);

    const auto restored = repository.get_version(version_id);

    ASSERT_TRUE(restored.has_value());

    EXPECT_EQ(restored->version_id(), version_id);

    EXPECT_EQ(restored->artifact_id(), artifact_id);

    EXPECT_EQ(restored->root_object_id(), object.object_id());

    EXPECT_EQ(restored->state(), VersionState::Committed);
}

TEST(PostgresMetadataRepositoryTest, PreservesAllArtifactVersionStates) {
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

    const ArtifactVersion staging{
        UuidV7::generate(),
        artifact_id,
        object.object_id(),
        VersionState::Staging,
    };

    const ArtifactVersion committed{
        UuidV7::generate(),
        artifact_id,
        object.object_id(),
        VersionState::Committed,
    };

    const ArtifactVersion failed{
        UuidV7::generate(),
        artifact_id,
        object.object_id(),
        VersionState::Failed,
    };

    repository.create_version(staging);
    repository.create_version(committed);
    repository.create_version(failed);

    const auto restored_staging = repository.get_version(staging.version_id());

    const auto restored_committed = repository.get_version(committed.version_id());

    const auto restored_failed = repository.get_version(failed.version_id());

    ASSERT_TRUE(restored_staging.has_value());
    ASSERT_TRUE(restored_committed.has_value());
    ASSERT_TRUE(restored_failed.has_value());

    EXPECT_EQ(restored_staging->state(), VersionState::Staging);

    EXPECT_EQ(restored_committed->state(), VersionState::Committed);

    EXPECT_EQ(restored_failed->state(), VersionState::Failed);
}

TEST(PostgresMetadataRepositoryTest, MissingArtifactVersionReturnsNullopt) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const auto restored = repository.get_version(UuidV7::generate());

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
        UuidV7::generate(),
        UuidV7::generate(),
        object.object_id(),
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
        UuidV7::generate(),
        artifact_id,
        std::string(64, 'c'),
        VersionState::Committed,
    };

    EXPECT_THROW(repository.create_version(version), pqxx::foreign_key_violation);
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
        UuidV7::generate(),
        artifact_id,
        object.object_id(),
        VersionState::Committed,
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
        UuidV7::generate(),
        artifact_id,
        object.object_id(),
        VersionState::Committed,
    };

    const ArtifactVersion second_version{
        UuidV7::generate(),
        artifact_id,
        object.object_id(),
        VersionState::Committed,
    };

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
        UuidV7::generate(),
        artifact_a_id,
        object.object_id(),
        VersionState::Committed,
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

}  // namespace
