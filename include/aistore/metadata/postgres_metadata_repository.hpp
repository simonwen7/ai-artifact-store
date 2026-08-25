#ifndef AISTORE_METADATA_POSTGRES_METADATA_REPOSITORY_HPP
#define AISTORE_METADATA_POSTGRES_METADATA_REPOSITORY_HPP

#include <memory>
#include <optional>
#include <string>

#include "aistore/metadata/artifact_model.hpp"

namespace aistore::metadata {

class PostgresMetadataRepository {
   public:
    explicit PostgresMetadataRepository(const std::string& connection_string);

    ~PostgresMetadataRepository();

    PostgresMetadataRepository(const PostgresMetadataRepository&) = delete;

    PostgresMetadataRepository& operator=(const PostgresMetadataRepository&) = delete;

    PostgresMetadataRepository(PostgresMetadataRepository&&) noexcept;

    PostgresMetadataRepository& operator=(PostgresMetadataRepository&&) noexcept;

    void create_artifact(const Artifact& artifact);

    [[nodiscard]] std::optional<Artifact> get_artifact(const UuidV7& artifact_id);

   private:
    class Impl;

    std::unique_ptr<Impl> impl_;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_POSTGRES_METADATA_REPOSITORY_HPP
