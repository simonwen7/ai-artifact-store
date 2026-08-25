#include "aistore/metadata/postgres_metadata_repository.hpp"

#include <optional>
#include <pqxx/pqxx>
#include <string>
#include <utility>

namespace aistore::metadata {

class PostgresMetadataRepository::Impl {
   public:
    explicit Impl(const std::string& connection_string) : connection_(connection_string) {}

    void create_artifact(const Artifact& artifact) {
        pqxx::work transaction{connection_};

        transaction
            .exec(
                "INSERT INTO artifacts ("
                "    artifact_id, "
                "    project, "
                "    name"
                ") "
                "VALUES ("
                "    $1::uuid, "
                "    $2, "
                "    $3"
                ")",
                pqxx::params{
                    artifact.artifact_id().str(),
                    artifact.project(),
                    artifact.name(),
                })
            .no_rows();

        transaction.commit();
    }

    [[nodiscard]] std::optional<Artifact> get_artifact(const UuidV7& artifact_id) {
        pqxx::work transaction{connection_};

        auto stored = transaction.query01<std::string, std::string, std::string>(
            "SELECT "
            "    artifact_id::text, "
            "    name, "
            "    project "
            "FROM artifacts "
            "WHERE artifact_id = $1::uuid",
            pqxx::params{
                artifact_id.str(),
            });

        if (!stored.has_value()) {
            transaction.commit();
            return std::nullopt;
        }

        auto [stored_artifact_id, name, project] = std::move(*stored);

        transaction.commit();

        return Artifact{
            UuidV7{
                std::move(stored_artifact_id),
            },
            std::move(name),
            std::move(project),
        };
    }

   private:
    pqxx::connection connection_;
};

PostgresMetadataRepository::PostgresMetadataRepository(const std::string& connection_string)
    : impl_(std::make_unique<Impl>(connection_string)) {}

PostgresMetadataRepository::~PostgresMetadataRepository() = default;

PostgresMetadataRepository::PostgresMetadataRepository(PostgresMetadataRepository&&) noexcept = default;

PostgresMetadataRepository& PostgresMetadataRepository::operator=(PostgresMetadataRepository&&) noexcept = default;

void PostgresMetadataRepository::create_artifact(const Artifact& artifact) { impl_->create_artifact(artifact); }

std::optional<Artifact> PostgresMetadataRepository::get_artifact(const UuidV7& artifact_id) {
    return impl_->get_artifact(artifact_id);
}

}  // namespace aistore::metadata
