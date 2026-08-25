#include "aistore/metadata/postgres_metadata_repository.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <pqxx/pqxx>
#include <string>

namespace {

using aistore::metadata::Artifact;
using aistore::metadata::ArtifactVersion;
using aistore::metadata::ChunkRef;
using aistore::metadata::ObjectDescriptor;
using aistore::metadata::ObjectLayout;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::UuidV7;
using aistore::metadata::VersionState;

const std::string kChunkA(64, 'a');

const std::string kChunkB(64, 'b');

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
            "    object_chunks, "
            "    artifacts, "
            "    objects, "
            "    chunks "
            "RESTART IDENTITY")
        .no_rows();

    transaction.commit();
}

ObjectDescriptor make_object_descriptor() {
    return ObjectDescriptor{
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

TEST(PostgresMetadataRepositoryTest, RegistersAndReadsObjectTransactionally) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const ObjectDescriptor descriptor = make_object_descriptor();

    repository.register_object(descriptor);

    const auto restored = repository.get_object(descriptor.object_id());

    ASSERT_TRUE(restored.has_value());

    EXPECT_EQ(restored->object_id(), descriptor.object_id());

    EXPECT_EQ(restored->canonical_bytes(), descriptor.canonical_bytes());

    EXPECT_EQ(restored->layout().total_size(), 6U);

    ASSERT_EQ(restored->layout().chunks().size(), 2U);

    EXPECT_EQ(restored->layout().chunks()[0].chunk_id, kChunkA);

    EXPECT_EQ(restored->layout().chunks()[0].offset, 0U);

    EXPECT_EQ(restored->layout().chunks()[0].size, 4U);

    EXPECT_EQ(restored->layout().chunks()[1].chunk_id, kChunkB);

    EXPECT_EQ(restored->layout().chunks()[1].offset, 4U);

    EXPECT_EQ(restored->layout().chunks()[1].size, 2U);
}

TEST(PostgresMetadataRepositoryTest, RegisterObjectIsIdempotent) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const ObjectDescriptor descriptor = make_object_descriptor();

    repository.register_object(descriptor);

    repository.register_object(descriptor);

    pqxx::connection connection{
        test_database_connection_string(),
    };

    pqxx::work transaction{connection};

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) FROM objects"), 1);

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) FROM chunks"), 2);

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) FROM object_chunks"), 2);

    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, RegisterObjectRollsBackOnChunkSizeConflict) {
    reset_metadata_data();

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

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const ObjectDescriptor descriptor = make_object_descriptor();

    EXPECT_THROW(repository.register_object(descriptor), std::runtime_error);

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

    const bool object_exists = transaction.query_value<bool>(
        "SELECT EXISTS ("
        "    SELECT 1 "
        "    FROM objects "
        "    WHERE object_id = $1"
        ")",
        pqxx::params{
            descriptor.object_id(),
        });

    const auto existing_chunk_b_size = transaction.query_value<long long>(
        "SELECT size_bytes "
        "FROM chunks "
        "WHERE chunk_id = $1",
        pqxx::params{
            kChunkB,
        });

    EXPECT_FALSE(chunk_a_exists);
    EXPECT_FALSE(object_exists);
    EXPECT_EQ(existing_chunk_b_size, 99);

    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, MissingObjectReturnsNullopt) {
    reset_metadata_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const auto restored = repository.get_object(std::string(64, 'c'));

    EXPECT_FALSE(restored.has_value());
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

    const ObjectDescriptor descriptor = make_object_descriptor();

    repository.register_object(descriptor);

    const UuidV7 version_id = UuidV7::generate();

    const ArtifactVersion version{
        version_id,
        artifact_id,
        descriptor.object_id(),
        VersionState::Committed,
    };

    repository.create_version(version);

    const auto restored = repository.get_version(version_id);

    ASSERT_TRUE(restored.has_value());

    EXPECT_EQ(restored->version_id(), version_id);

    EXPECT_EQ(restored->artifact_id(), artifact_id);

    EXPECT_EQ(restored->root_object_id(), descriptor.object_id());

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

    const ObjectDescriptor descriptor = make_object_descriptor();

    repository.register_object(descriptor);

    const ArtifactVersion staging{
        UuidV7::generate(),
        artifact_id,
        descriptor.object_id(),
        VersionState::Staging,
    };

    const ArtifactVersion committed{
        UuidV7::generate(),
        artifact_id,
        descriptor.object_id(),
        VersionState::Committed,
    };

    const ArtifactVersion failed{
        UuidV7::generate(),
        artifact_id,
        descriptor.object_id(),
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

    const ObjectDescriptor descriptor = make_object_descriptor();

    repository.register_object(descriptor);

    const ArtifactVersion version{
        UuidV7::generate(),
        UuidV7::generate(),
        descriptor.object_id(),
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

}  // namespace
