#ifndef AISTORE_METADATA_POSTGRES_METADATA_REPOSITORY_HPP
#define AISTORE_METADATA_POSTGRES_METADATA_REPOSITORY_HPP

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "aistore/metadata/artifact_model.hpp"
#include "aistore/metadata/manifest_model.hpp"
#include "aistore/metadata/object.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/run_model.hpp"
#include "aistore/metadata/storage_location.hpp"

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

    void register_object(const Object& object);

    [[nodiscard]] std::optional<Object> get_object(std::string_view object_id);

    void register_object_layout(const ObjectLayoutDescriptor& descriptor);

    [[nodiscard]] std::optional<ObjectLayoutDescriptor> get_object_layout(std::string_view layout_id);

    [[nodiscard]] std::vector<ObjectLayoutDescriptor> get_object_layouts(std::string_view object_id);

    void create_version(const ArtifactVersion& version);

    [[nodiscard]] std::optional<ArtifactVersion> get_version(std::string_view version_id);

    void set_version_state(std::string_view version_id, VersionState state);

    void set_tag(const UuidV7& artifact_id, std::string_view tag_name, std::string_view version_id);

    [[nodiscard]] std::optional<std::string> get_tag(const UuidV7& artifact_id, std::string_view tag_name);

    void register_manifest(const Manifest& manifest);

    [[nodiscard]] std::optional<Manifest> get_manifest(std::string_view manifest_id);

    void create_run(const Run& run);

    [[nodiscard]] std::optional<Run> get_run(const UuidV7& run_id);

    void register_storage_location(const StorageLocation& location);

    [[nodiscard]] std::vector<StorageLocation> get_storage_locations(std::string_view chunk_id);

   private:
    class Impl;

    std::unique_ptr<Impl> impl_;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_POSTGRES_METADATA_REPOSITORY_HPP
