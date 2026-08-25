#include "aistore/metadata/postgres_metadata_repository.hpp"

#include <gtest/gtest.h>

#include <cstdlib>
#include <pqxx/pqxx>
#include <string>

namespace {

using aistore::metadata::Artifact;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::UuidV7;

std::string test_database_connection_string() {
    const char* configured = std::getenv("AISTORE_TEST_DB_URL");

    if (configured != nullptr && configured[0] != '\0') {
        return configured;
    }

    return "dbname=ai_artifact_store_test";
}

void reset_artifact_data() {
    pqxx::connection connection{
        test_database_connection_string(),
    };

    pqxx::work transaction{connection};

    transaction
        .exec(
            "TRUNCATE TABLE artifacts "
            "RESTART IDENTITY CASCADE")
        .no_rows();

    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, CreatesAndReadsArtifact) {
    reset_artifact_data();

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
    reset_artifact_data();

    PostgresMetadataRepository repository{
        test_database_connection_string(),
    };

    const auto restored = repository.get_artifact(UuidV7::generate());

    EXPECT_FALSE(restored.has_value());
}

TEST(PostgresMetadataRepositoryTest, DatabaseEnforcesUniqueProjectAndName) {
    reset_artifact_data();

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

}  // namespace
