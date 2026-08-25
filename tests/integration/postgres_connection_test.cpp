#include <gtest/gtest.h>

#include <cstdlib>
#include <pqxx/pqxx>
#include <string>

namespace {

std::string test_database_connection_string() {
    const char* configured = std::getenv("AISTORE_TEST_DB_URL");

    if (configured != nullptr && configured[0] != '\0') {
        return configured;
    }

    return "dbname=ai_artifact_store_test";
}

TEST(PostgresConnectionTest, ConnectsToMigratedTestDatabase) {
    pqxx::connection connection{
        test_database_connection_string(),
    };

    ASSERT_TRUE(connection.is_open());

    pqxx::work transaction{connection};

    const std::string database_name = transaction.query_value<std::string>("SELECT current_database()");

    EXPECT_EQ(database_name, "ai_artifact_store_test");

    const bool initial_migration_exists = transaction.query_value<bool>(
        "SELECT EXISTS ("
        "    SELECT 1 "
        "    FROM schema_migrations "
        "    WHERE version = 1 "
        "      AND name = 'initial_schema'"
        ")");

    EXPECT_TRUE(initial_migration_exists);
}

}  // namespace
