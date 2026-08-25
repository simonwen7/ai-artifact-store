#include "aistore/metadata/postgres_metadata_repository.hpp"

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <pqxx/pqxx>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace aistore::metadata {

namespace {

long long to_postgres_bigint(std::uint64_t value, std::string_view field_name) {
    const auto maximum = static_cast<std::uint64_t>(std::numeric_limits<long long>::max());

    if (value > maximum) {
        throw std::overflow_error(std::string{field_name} + " exceeds PostgreSQL BIGINT range");
    }

    return static_cast<long long>(value);
}

long long size_to_postgres_bigint(std::size_t value, std::string_view field_name) {
    const auto maximum = static_cast<std::size_t>(std::numeric_limits<long long>::max());

    if (value > maximum) {
        throw std::overflow_error(std::string{field_name} + " exceeds PostgreSQL BIGINT range");
    }

    return static_cast<long long>(value);
}

std::string bytes_to_hex(std::span<const std::byte> bytes) {
    constexpr std::array<char, 16> kHexDigits{
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    };

    std::string result;
    result.reserve(bytes.size() * 2);

    for (const std::byte byte : bytes) {
        const auto value = std::to_integer<unsigned int>(byte);

        result.push_back(kHexDigits[(value >> 4U) & 0x0FU]);

        result.push_back(kHexDigits[value & 0x0FU]);
    }

    return result;
}

void validate_object_id(std::string_view object_id) {
    if (object_id.size() != 64) {
        throw std::invalid_argument("object ID must contain exactly 64 hexadecimal characters");
    }

    for (const char character : object_id) {
        const bool is_digit = character >= '0' && character <= '9';

        const bool is_lower_hex = character >= 'a' && character <= 'f';

        if (!is_digit && !is_lower_hex) {
            throw std::invalid_argument("object ID must use lowercase hexadecimal characters");
        }
    }
}

void validate_layout_id(std::string_view layout_id) {
    if (layout_id.size() != 64) {
        throw std::invalid_argument("layout ID must contain exactly 64 hexadecimal characters");
    }

    for (const char character : layout_id) {
        const bool is_digit = character >= '0' && character <= '9';

        const bool is_lower_hex = character >= 'a' && character <= 'f';

        if (!is_digit && !is_lower_hex) {
            throw std::invalid_argument("layout ID must use lowercase hexadecimal characters");
        }
    }
}

void validate_version_id(std::string_view version_id) {
    if (version_id.size() != 64) {
        throw std::invalid_argument("version ID must contain exactly 64 hexadecimal characters");
    }

    for (const char character : version_id) {
        const bool is_digit = character >= '0' && character <= '9';

        const bool is_lower_hex = character >= 'a' && character <= 'f';

        if (!is_digit && !is_lower_hex) {
            throw std::invalid_argument("version ID must use lowercase hexadecimal characters");
        }
    }
}

void validate_manifest_id(std::string_view manifest_id) {
    if (manifest_id.size() != 64) {
        throw std::invalid_argument("manifest ID must contain exactly 64 hexadecimal characters");
    }

    for (const char character : manifest_id) {
        const bool is_digit = character >= '0' && character <= '9';

        const bool is_lower_hex = character >= 'a' && character <= 'f';

        if (!is_digit && !is_lower_hex) {
            throw std::invalid_argument("manifest ID must use lowercase hexadecimal characters");
        }
    }
}

long long timestamp_to_unix_milliseconds(Run::Timestamp timestamp) {
    return static_cast<long long>(timestamp.time_since_epoch().count());
}

Run::Timestamp timestamp_from_unix_milliseconds(long long milliseconds) {
    return Run::Timestamp{
        std::chrono::milliseconds{
            milliseconds,
        },
    };
}

std::string_view version_state_to_string(VersionState state) {
    switch (state) {
        case VersionState::Staging:
            return "staging";

        case VersionState::Committed:
            return "committed";

        case VersionState::Failed:
            return "failed";
    }

    throw std::logic_error("unsupported artifact version state");
}

VersionState version_state_from_string(std::string_view state) {
    if (state == "staging") {
        return VersionState::Staging;
    }

    if (state == "committed") {
        return VersionState::Committed;
    }

    if (state == "failed") {
        return VersionState::Failed;
    }

    throw std::runtime_error("database contains an unsupported artifact version state");
}

std::string_view storage_location_state_to_string(StorageLocationState state) {
    switch (state) {
        case StorageLocationState::Available:
            return "available";

        case StorageLocationState::Missing:
            return "missing";

        case StorageLocationState::Corrupt:
            return "corrupt";
    }

    throw std::logic_error("unsupported storage location state");
}

StorageLocationState storage_location_state_from_string(std::string_view state) {
    if (state == "available") {
        return StorageLocationState::Available;
    }

    if (state == "missing") {
        return StorageLocationState::Missing;
    }

    if (state == "corrupt") {
        return StorageLocationState::Corrupt;
    }

    throw std::runtime_error("database contains an unsupported storage location state");
}

void validate_chunk_id(std::string_view chunk_id) {
    if (chunk_id.size() != 64) {
        throw std::invalid_argument("chunk ID must contain exactly 64 hexadecimal characters");
    }

    for (const char character : chunk_id) {
        const bool is_digit = character >= '0' && character <= '9';

        const bool is_lower_hex = character >= 'a' && character <= 'f';

        if (!is_digit && !is_lower_hex) {
            throw std::invalid_argument("chunk ID must use lowercase hexadecimal characters");
        }
    }
}

std::string_view chunking_strategy_to_string(ChunkingStrategy strategy) {
    switch (strategy) {
        case ChunkingStrategy::FixedSize:
            return "fixed-size";
    }

    throw std::logic_error("unsupported chunking strategy");
}

ChunkingStrategy chunking_strategy_from_string(std::string_view strategy) {
    if (strategy == "fixed-size") {
        return ChunkingStrategy::FixedSize;
    }

    throw std::runtime_error("database contains an unsupported chunking strategy");
}

}  // namespace

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

    void register_object(const Object& object) {
        pqxx::work transaction{connection_};

        const long long total_size = to_postgres_bigint(object.total_size(), "object total size");

        transaction
            .exec(
                "INSERT INTO objects ("
                "    object_id, "
                "    total_size_bytes"
                ") "
                "VALUES ("
                "    $1, "
                "    $2"
                ") "
                "ON CONFLICT (object_id) "
                "DO NOTHING",
                pqxx::params{
                    object.object_id(),
                    total_size,
                })
            .no_rows();

        const auto stored_total_size = transaction.query_value<long long>(
            "SELECT "
            "    total_size_bytes "
            "FROM objects "
            "WHERE object_id = $1",
            pqxx::params{
                object.object_id(),
            });

        if (stored_total_size != total_size) {
            throw std::runtime_error("existing object size does not match object identity");
        }

        transaction.commit();
    }

    [[nodiscard]] std::optional<Object> get_object(std::string_view object_id) {
        validate_object_id(object_id);

        pqxx::work transaction{connection_};

        auto stored = transaction.query01<std::string, long long>(
            "SELECT "
            "    object_id, "
            "    total_size_bytes "
            "FROM objects "
            "WHERE object_id = $1",
            pqxx::params{
                object_id,
            });

        if (!stored.has_value()) {
            transaction.commit();
            return std::nullopt;
        }

        auto [stored_object_id, stored_total_size] = std::move(*stored);

        if (stored_total_size < 0) {
            throw std::runtime_error("stored object contains an invalid negative size");
        }

        Object object{
            std::move(stored_object_id),
            static_cast<std::uint64_t>(stored_total_size),
        };

        transaction.commit();

        return object;
    }

    void register_object_layout(const ObjectLayoutDescriptor& descriptor) {
        pqxx::work transaction{connection_};

        const Object& object = descriptor.object();

        const ObjectLayout& layout = descriptor.layout();

        const auto stored_object_size = transaction.query01<long long>(
            "SELECT "
            "    total_size_bytes "
            "FROM objects "
            "WHERE object_id = $1",
            pqxx::params{
                object.object_id(),
            });

        if (!stored_object_size.has_value()) {
            throw std::runtime_error("object must be registered before its layout");
        }

        const auto [stored_size] = *stored_object_size;

        if (stored_size != to_postgres_bigint(object.total_size(), "object total size")) {
            throw std::runtime_error("registered object size does not match object layout");
        }

        for (const ChunkRef& chunk : layout.chunks()) {
            const long long chunk_size = to_postgres_bigint(chunk.size, "chunk size");

            transaction
                .exec(
                    "INSERT INTO chunks ("
                    "    chunk_id, "
                    "    size_bytes"
                    ") "
                    "VALUES ("
                    "    $1, "
                    "    $2"
                    ") "
                    "ON CONFLICT (chunk_id) "
                    "DO NOTHING",
                    pqxx::params{
                        chunk.chunk_id,
                        chunk_size,
                    })
                .no_rows();

            const auto stored_chunk_size = transaction.query_value<long long>(
                "SELECT "
                "    size_bytes "
                "FROM chunks "
                "WHERE chunk_id = $1",
                pqxx::params{
                    chunk.chunk_id,
                });

            if (stored_chunk_size != chunk_size) {
                throw std::runtime_error("existing chunk size does not match object layout");
            }
        }

        const long long total_size = to_postgres_bigint(layout.total_size(), "object layout total size");

        const long long chunk_count = size_to_postgres_bigint(layout.chunks().size(), "object layout chunk count");

        const pqxx::result layout_insert = transaction.exec(
            "INSERT INTO object_layouts ("
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
            "    $3, "
            "    $4, "
            "    $5::bytea, "
            "    $6, "
            "    $7"
            ") "
            "ON CONFLICT (layout_id) "
            "DO NOTHING",
            pqxx::params{
                descriptor.layout_id(),
                descriptor.object_id(),
                static_cast<int>(ObjectLayoutDescriptor::kFormatVersion),
                chunking_strategy_to_string(descriptor.chunking_strategy()),
                descriptor.canonical_bytes(),
                total_size,
                chunk_count,
            });

        const bool layout_was_inserted = layout_insert.affected_rows() == 1U;

        if (layout_was_inserted) {
            for (std::size_t index = 0; index < layout.chunks().size(); ++index) {
                const ChunkRef& chunk = layout.chunks()[index];

                transaction
                    .exec(
                        "INSERT INTO object_layout_chunks ("
                        "    layout_id, "
                        "    chunk_index, "
                        "    chunk_id, "
                        "    byte_offset, "
                        "    chunk_size_bytes"
                        ") "
                        "VALUES ("
                        "    $1, "
                        "    $2, "
                        "    $3, "
                        "    $4, "
                        "    $5"
                        ")",
                        pqxx::params{
                            descriptor.layout_id(),
                            size_to_postgres_bigint(index, "chunk index"),
                            chunk.chunk_id,
                            to_postgres_bigint(chunk.offset, "chunk byte offset"),
                            to_postgres_bigint(chunk.size, "chunk size"),
                        })
                    .no_rows();
            }
        }

        verify_registered_object_layout(transaction, descriptor);

        transaction.commit();
    }

    [[nodiscard]] std::optional<ObjectLayoutDescriptor> get_object_layout(std::string_view layout_id) {
        pqxx::work transaction{connection_};

        auto descriptor = load_object_layout(transaction, layout_id);

        transaction.commit();

        return descriptor;
    }

    [[nodiscard]] std::vector<ObjectLayoutDescriptor> get_object_layouts(std::string_view object_id) {
        validate_object_id(object_id);

        pqxx::work transaction{connection_};

        std::vector<ObjectLayoutDescriptor> layouts;

        for (const auto& [layout_id] : transaction.query<std::string>("SELECT "
                                                                      "    layout_id "
                                                                      "FROM object_layouts "
                                                                      "WHERE object_id = $1 "
                                                                      "ORDER BY layout_id",
                                                                      pqxx::params{
                                                                          object_id,
                                                                      })) {
            auto descriptor = load_object_layout(transaction, layout_id);

            if (!descriptor.has_value()) {
                throw std::runtime_error("object layout disappeared during transaction");
            }

            layouts.push_back(std::move(*descriptor));
        }

        transaction.commit();

        return layouts;
    }

    void create_version(const ArtifactVersion& version) {
        pqxx::work transaction{connection_};

        transaction
            .exec(
                "INSERT INTO artifact_versions ("
                "    version_id, "
                "    artifact_id, "
                "    root_object_id, "
                "    parent_version_id, "
                "    descriptor_version, "
                "    canonical_descriptor, "
                "    state"
                ") "
                "VALUES ("
                "    $1, "
                "    $2::uuid, "
                "    $3, "
                "    $4, "
                "    $5, "
                "    $6::bytea, "
                "    $7"
                ")",
                pqxx::params{
                    version.version_id(),
                    version.artifact_id().str(),
                    version.root_object_id(),
                    version.parent_version_id(),
                    static_cast<int>(ArtifactVersion::kFormatVersion),
                    version.canonical_bytes(),
                    version_state_to_string(version.state()),
                })
            .no_rows();

        for (const auto& [key, value] : version.immutable_metadata()) {
            transaction
                .exec(
                    "INSERT INTO artifact_version_metadata ("
                    "    version_id, "
                    "    metadata_key, "
                    "    metadata_value"
                    ") "
                    "VALUES ("
                    "    $1, "
                    "    $2, "
                    "    $3"
                    ")",
                    pqxx::params{
                        version.version_id(),
                        key,
                        value,
                    })
                .no_rows();
        }

        transaction.commit();
    }

    [[nodiscard]] std::optional<ArtifactVersion> get_version(std::string_view version_id) {
        validate_version_id(version_id);

        pqxx::work transaction{connection_};

        auto stored = transaction.query01<std::string, std::string, std::string, std::optional<std::string>, int,
                                          std::string, std::string>(
            "SELECT "
            "    version_id, "
            "    artifact_id::text, "
            "    root_object_id, "
            "    parent_version_id, "
            "    descriptor_version, "
            "    encode("
            "        canonical_descriptor, "
            "        'hex'"
            "    ), "
            "    state "
            "FROM artifact_versions "
            "WHERE version_id = $1",
            pqxx::params{
                version_id,
            });

        if (!stored.has_value()) {
            transaction.commit();
            return std::nullopt;
        }

        auto [stored_version_id, stored_artifact_id, root_object_id, parent_version_id, descriptor_version,
              stored_descriptor_hex, state] = std::move(*stored);

        if (std::cmp_not_equal(descriptor_version, ArtifactVersion::kFormatVersion)) {
            throw std::runtime_error("stored artifact version uses an unsupported descriptor version");
        }

        ArtifactVersion::ImmutableMetadata immutable_metadata;

        for (auto [metadata_key, metadata_value] :
             transaction.query<std::string, std::string>("SELECT "
                                                         "    metadata_key, "
                                                         "    metadata_value "
                                                         "FROM artifact_version_metadata "
                                                         "WHERE version_id = $1 "
                                                         "ORDER BY metadata_key",
                                                         pqxx::params{
                                                             version_id,
                                                         })) {
            immutable_metadata.emplace(std::move(metadata_key), std::move(metadata_value));
        }

        ArtifactVersion reconstructed{
            UuidV7{
                std::move(stored_artifact_id),
            },
            std::move(root_object_id),
            std::move(parent_version_id),
            std::move(immutable_metadata),
            version_state_from_string(state),
        };

        if (reconstructed.version_id() != stored_version_id) {
            throw std::runtime_error("stored artifact version does not match version ID");
        }

        const std::string reconstructed_descriptor_hex = bytes_to_hex(std::span<const std::byte>{
            reconstructed.canonical_bytes(),
        });

        if (reconstructed_descriptor_hex != stored_descriptor_hex) {
            throw std::runtime_error("stored canonical descriptor does not match artifact version");
        }

        transaction.commit();

        return reconstructed;
    }

    void set_version_state(std::string_view version_id, VersionState state) {
        validate_version_id(version_id);

        pqxx::work transaction{connection_};

        const pqxx::result updated = transaction.exec(
            "UPDATE artifact_versions "
            "SET state = $2 "
            "WHERE version_id = $1",
            pqxx::params{
                version_id,
                version_state_to_string(state),
            });

        if (updated.affected_rows() != 1U) {
            throw std::runtime_error("artifact version does not exist");
        }

        transaction.commit();
    }

    void set_tag(const UuidV7& artifact_id, std::string_view tag_name, std::string_view version_id) {
        if (tag_name.empty()) {
            throw std::invalid_argument("tag name must not be empty");
        }

        validate_version_id(version_id);

        pqxx::work transaction{connection_};

        transaction
            .exec(
                "INSERT INTO tags ("
                "    artifact_id, "
                "    tag_name, "
                "    version_id"
                ") "
                "VALUES ("
                "    $1::uuid, "
                "    $2, "
                "    $3"
                ") "
                "ON CONFLICT (artifact_id, tag_name) "
                "DO UPDATE SET "
                "    version_id = EXCLUDED.version_id, "
                "    updated_at = CURRENT_TIMESTAMP",
                pqxx::params{
                    artifact_id.str(),
                    tag_name,
                    version_id,
                })
            .no_rows();

        transaction.commit();
    }

    [[nodiscard]] std::optional<std::string> get_tag(const UuidV7& artifact_id, std::string_view tag_name) {
        if (tag_name.empty()) {
            throw std::invalid_argument("tag name must not be empty");
        }

        pqxx::work transaction{connection_};

        auto stored = transaction.query01<std::string>(
            "SELECT "
            "    version_id "
            "FROM tags "
            "WHERE artifact_id = $1::uuid "
            "  AND tag_name = $2",
            pqxx::params{
                artifact_id.str(),
                tag_name,
            });

        if (!stored.has_value()) {
            transaction.commit();
            return std::nullopt;
        }

        auto [stored_version_id] = std::move(*stored);

        transaction.commit();

        return stored_version_id;
    }

    void register_manifest(const Manifest& manifest) {
        pqxx::work transaction{connection_};

        const long long entry_count = size_to_postgres_bigint(manifest.entries().size(), "manifest entry count");

        const pqxx::result manifest_insert = transaction.exec(
            "INSERT INTO manifests ("
            "    manifest_id, "
            "    descriptor_version, "
            "    canonical_descriptor, "
            "    entry_count"
            ") "
            "VALUES ("
            "    $1, "
            "    $2, "
            "    $3::bytea, "
            "    $4"
            ") "
            "ON CONFLICT (manifest_id) "
            "DO NOTHING",
            pqxx::params{
                manifest.manifest_id(),
                static_cast<int>(Manifest::kFormatVersion),
                manifest.canonical_bytes(),
                entry_count,
            });

        const bool manifest_was_inserted = manifest_insert.affected_rows() == 1U;

        if (manifest_was_inserted) {
            for (const auto& [role, version_id] : manifest.entries()) {
                transaction
                    .exec(
                        "INSERT INTO manifest_entries ("
                        "    manifest_id, "
                        "    role, "
                        "    version_id"
                        ") "
                        "VALUES ("
                        "    $1, "
                        "    $2, "
                        "    $3"
                        ")",
                        pqxx::params{
                            manifest.manifest_id(),
                            role,
                            version_id,
                        })
                    .no_rows();
            }
        }

        verify_registered_manifest(transaction, manifest);

        transaction.commit();
    }

    [[nodiscard]] std::optional<Manifest> get_manifest(std::string_view manifest_id) {
        validate_manifest_id(manifest_id);

        pqxx::work transaction{connection_};

        auto stored = transaction.query01<std::string, int, std::string, long long>(
            "SELECT "
            "    manifest_id, "
            "    descriptor_version, "
            "    encode("
            "        canonical_descriptor, "
            "        'hex'"
            "    ), "
            "    entry_count "
            "FROM manifests "
            "WHERE manifest_id = $1",
            pqxx::params{
                manifest_id,
            });

        if (!stored.has_value()) {
            transaction.commit();
            return std::nullopt;
        }

        auto [stored_manifest_id, descriptor_version, stored_descriptor_hex, stored_entry_count] = std::move(*stored);

        if (std::cmp_not_equal(descriptor_version, Manifest::kFormatVersion)) {
            throw std::runtime_error("stored manifest uses an unsupported descriptor version");
        }

        if (stored_entry_count < 0) {
            throw std::runtime_error("stored manifest contains an invalid negative entry count");
        }

        Manifest::Entries entries;

        for (auto [role, version_id] : transaction.query<std::string, std::string>("SELECT "
                                                                                   "    role, "
                                                                                   "    version_id "
                                                                                   "FROM manifest_entries "
                                                                                   "WHERE manifest_id = $1 "
                                                                                   "ORDER BY role",
                                                                                   pqxx::params{
                                                                                       manifest_id,
                                                                                   })) {
            entries.emplace(std::move(role), std::move(version_id));
        }

        if (std::cmp_not_equal(entries.size(), stored_entry_count)) {
            throw std::runtime_error("stored manifest entry count does not match entry rows");
        }

        Manifest reconstructed{
            std::move(entries),
        };

        if (reconstructed.manifest_id() != stored_manifest_id) {
            throw std::runtime_error("stored manifest does not match manifest ID");
        }

        const std::string reconstructed_descriptor_hex = bytes_to_hex(std::span<const std::byte>{
            reconstructed.canonical_bytes(),
        });

        if (reconstructed_descriptor_hex != stored_descriptor_hex) {
            throw std::runtime_error("stored canonical descriptor does not match manifest");
        }

        transaction.commit();

        return reconstructed;
    }

    void create_run(const Run& run) {
        pqxx::work transaction{connection_};

        const long long started_at_ms = timestamp_to_unix_milliseconds(run.started_at());

        std::optional<long long> completed_at_ms;

        if (run.completed_at().has_value()) {
            completed_at_ms = timestamp_to_unix_milliseconds(*run.completed_at());
        }

        transaction
            .exec(
                "INSERT INTO runs ("
                "    run_id, "
                "    manifest_id, "
                "    name, "
                "    source_commit, "
                "    started_at, "
                "    completed_at"
                ") "
                "VALUES ("
                "    $1::uuid, "
                "    $2, "
                "    $3, "
                "    $4, "
                "    to_timestamp("
                "        $5::double precision / 1000.0"
                "    ), "
                "    CASE "
                "        WHEN $6::bigint IS NULL "
                "            THEN NULL "
                "        ELSE to_timestamp("
                "            $6::double precision / 1000.0"
                "        ) "
                "    END"
                ")",
                pqxx::params{
                    run.run_id().str(),
                    run.manifest_id(),
                    run.name(),
                    run.source_commit(),
                    started_at_ms,
                    completed_at_ms,
                })
            .no_rows();

        for (const auto& [tag_key, tag_value] : run.tags()) {
            transaction
                .exec(
                    "INSERT INTO run_tags ("
                    "    run_id, "
                    "    tag_key, "
                    "    tag_value"
                    ") "
                    "VALUES ("
                    "    $1::uuid, "
                    "    $2, "
                    "    $3"
                    ")",
                    pqxx::params{
                        run.run_id().str(),
                        tag_key,
                        tag_value,
                    })
                .no_rows();
        }

        transaction.commit();
    }

    [[nodiscard]] std::optional<Run> get_run(const UuidV7& run_id) {
        pqxx::work transaction{connection_};

        auto stored = transaction.query01<std::string, std::string, std::string, std::optional<std::string>, long long,
                                          std::optional<long long>>(
            "SELECT "
            "    run_id::text, "
            "    manifest_id, "
            "    name, "
            "    source_commit, "
            "    ROUND("
            "        EXTRACT(EPOCH FROM started_at) "
            "        * 1000"
            "    )::bigint, "
            "    CASE "
            "        WHEN completed_at IS NULL "
            "            THEN NULL "
            "        ELSE ROUND("
            "            EXTRACT(EPOCH FROM completed_at) "
            "            * 1000"
            "        )::bigint "
            "    END "
            "FROM runs "
            "WHERE run_id = $1::uuid",
            pqxx::params{
                run_id.str(),
            });

        if (!stored.has_value()) {
            transaction.commit();
            return std::nullopt;
        }

        auto [stored_run_id, stored_manifest_id, name, source_commit, started_at_ms, completed_at_ms] =
            std::move(*stored);

        Run::Tags tags;

        for (auto [tag_key, tag_value] : transaction.query<std::string, std::string>("SELECT "
                                                                                     "    tag_key, "
                                                                                     "    tag_value "
                                                                                     "FROM run_tags "
                                                                                     "WHERE run_id = $1::uuid "
                                                                                     "ORDER BY tag_key",
                                                                                     pqxx::params{
                                                                                         run_id.str(),
                                                                                     })) {
            tags.emplace(std::move(tag_key), std::move(tag_value));
        }

        std::optional<Run::Timestamp> completed_at;

        if (completed_at_ms.has_value()) {
            completed_at = timestamp_from_unix_milliseconds(*completed_at_ms);
        }

        Run reconstructed{
            UuidV7{
                std::move(stored_run_id),
            },
            std::move(stored_manifest_id),
            std::move(name),
            std::move(source_commit),
            std::move(tags),
            timestamp_from_unix_milliseconds(started_at_ms),
            completed_at,
        };

        transaction.commit();

        return reconstructed;
    }

    void register_storage_location(const StorageLocation& location) {
        validate_chunk_id(location.chunk_id);

        if (location.node_id.empty()) {
            throw std::invalid_argument("storage node ID must not be empty");
        }

        if (location.storage_path.empty()) {
            throw std::invalid_argument("storage path must not be empty");
        }

        pqxx::work transaction{connection_};

        transaction
            .exec(
                "INSERT INTO storage_locations ("
                "    chunk_id, "
                "    node_id, "
                "    storage_path, "
                "    state"
                ") "
                "VALUES ("
                "    $1, "
                "    $2, "
                "    $3, "
                "    $4"
                ") "
                "ON CONFLICT (chunk_id, node_id) "
                "DO UPDATE SET "
                "    storage_path = EXCLUDED.storage_path, "
                "    state = EXCLUDED.state",
                pqxx::params{
                    location.chunk_id,
                    location.node_id,
                    location.storage_path,
                    storage_location_state_to_string(location.state),
                })
            .no_rows();

        transaction.commit();
    }

    [[nodiscard]] std::vector<StorageLocation> get_storage_locations(std::string_view chunk_id) {
        validate_chunk_id(chunk_id);

        pqxx::work transaction{connection_};

        std::vector<StorageLocation> locations;

        for (const auto& [stored_chunk_id, node_id, storage_path, state] :
             transaction.query<std::string, std::string, std::string, std::string>("SELECT "
                                                                                   "    chunk_id, "
                                                                                   "    node_id, "
                                                                                   "    storage_path, "
                                                                                   "    state "
                                                                                   "FROM storage_locations "
                                                                                   "WHERE chunk_id = $1 "
                                                                                   "ORDER BY node_id",
                                                                                   pqxx::params{
                                                                                       chunk_id,
                                                                                   })) {
            locations.push_back(StorageLocation{
                .chunk_id = stored_chunk_id,
                .node_id = node_id,
                .storage_path = storage_path,
                .state = storage_location_state_from_string(state),
            });
        }

        transaction.commit();

        return locations;
    }

   private:
    [[nodiscard]] std::optional<ObjectLayoutDescriptor> load_object_layout(pqxx::work& transaction,
                                                                           std::string_view layout_id) {
        validate_layout_id(layout_id);

        auto stored = transaction.query01<std::string, int, std::string, std::string, long long, long long>(
            "SELECT "
            "    object_id, "
            "    descriptor_version, "
            "    chunking_strategy, "
            "    encode("
            "        canonical_descriptor, "
            "        'hex'"
            "    ), "
            "    total_size_bytes, "
            "    chunk_count "
            "FROM object_layouts "
            "WHERE layout_id = $1",
            pqxx::params{
                layout_id,
            });

        if (!stored.has_value()) {
            return std::nullopt;
        }

        auto [stored_object_id, descriptor_version, stored_strategy, stored_descriptor_hex, stored_total_size,
              stored_chunk_count] = std::move(*stored);

        if (std::cmp_not_equal(descriptor_version, ObjectLayoutDescriptor::kFormatVersion)) {
            throw std::runtime_error("stored object layout uses an unsupported descriptor version");
        }

        if (stored_total_size < 0 || stored_chunk_count < 0) {
            throw std::runtime_error("stored object layout contains invalid negative metadata");
        }

        std::vector<ChunkRef> chunks;

        chunks.reserve(static_cast<std::size_t>(stored_chunk_count));

        std::size_t expected_index = 0;

        for (auto [stored_index, chunk_id, byte_offset, chunk_size] :
             transaction.query<long long, std::string, long long, long long>("SELECT "
                                                                             "    chunk_index, "
                                                                             "    chunk_id, "
                                                                             "    byte_offset, "
                                                                             "    chunk_size_bytes "
                                                                             "FROM object_layout_chunks "
                                                                             "WHERE layout_id = $1 "
                                                                             "ORDER BY chunk_index",
                                                                             pqxx::params{
                                                                                 layout_id,
                                                                             })) {
            if (stored_index != size_to_postgres_bigint(expected_index, "chunk index")) {
                throw std::runtime_error("stored object layout chunk indexes are not contiguous");
            }

            if (byte_offset < 0 || chunk_size <= 0) {
                throw std::runtime_error("stored object layout chunk contains invalid size metadata");
            }

            chunks.push_back(ChunkRef{
                .chunk_id = std::move(chunk_id),
                .offset = static_cast<std::uint64_t>(byte_offset),
                .size = static_cast<std::uint64_t>(chunk_size),
            });

            ++expected_index;
        }

        if (std::cmp_not_equal(expected_index, stored_chunk_count)) {
            throw std::runtime_error("stored object layout chunk count does not match layout rows");
        }

        ObjectLayoutDescriptor descriptor{
            Object{
                std::move(stored_object_id),
                static_cast<std::uint64_t>(stored_total_size),
            },
            chunking_strategy_from_string(stored_strategy),
            ObjectLayout{
                std::move(chunks),
            },
        };

        if (descriptor.layout_id() != layout_id) {
            throw std::runtime_error("stored object layout does not match layout ID");
        }

        const std::string reconstructed_descriptor_hex = bytes_to_hex(std::span<const std::byte>{
            descriptor.canonical_bytes(),
        });

        if (reconstructed_descriptor_hex != stored_descriptor_hex) {
            throw std::runtime_error("stored canonical descriptor does not match object layout");
        }

        return descriptor;
    }

    void verify_registered_object_layout(pqxx::work& transaction, const ObjectLayoutDescriptor& descriptor) {
        const ObjectLayout& layout = descriptor.layout();

        auto stored = transaction.query01<std::string, int, std::string, std::string, long long, long long>(
            "SELECT "
            "    object_id, "
            "    descriptor_version, "
            "    chunking_strategy, "
            "    encode("
            "        canonical_descriptor, "
            "        'hex'"
            "    ), "
            "    total_size_bytes, "
            "    chunk_count "
            "FROM object_layouts "
            "WHERE layout_id = $1",
            pqxx::params{
                descriptor.layout_id(),
            });

        if (!stored.has_value()) {
            throw std::runtime_error("registered object layout is missing from database");
        }

        const auto& [stored_object_id, descriptor_version, stored_strategy, stored_descriptor_hex, stored_total_size,
                     stored_chunk_count] = *stored;

        if (stored_object_id != descriptor.object_id() ||
            std::cmp_not_equal(descriptor_version, ObjectLayoutDescriptor::kFormatVersion) ||
            stored_strategy != chunking_strategy_to_string(descriptor.chunking_strategy()) ||
            stored_descriptor_hex != bytes_to_hex(std::span<const std::byte>{
                                         descriptor.canonical_bytes(),
                                     }) ||
            stored_total_size != to_postgres_bigint(layout.total_size(), "object layout total size") ||
            stored_chunk_count != size_to_postgres_bigint(layout.chunks().size(), "object layout chunk count")) {
            throw std::runtime_error("existing object layout metadata does not match descriptor");
        }

        std::size_t expected_index = 0;

        for (const auto& [stored_index, stored_chunk_id, stored_offset, stored_size] :
             transaction.query<long long, std::string, long long, long long>("SELECT "
                                                                             "    chunk_index, "
                                                                             "    chunk_id, "
                                                                             "    byte_offset, "
                                                                             "    chunk_size_bytes "
                                                                             "FROM object_layout_chunks "
                                                                             "WHERE layout_id = $1 "
                                                                             "ORDER BY chunk_index",
                                                                             pqxx::params{
                                                                                 descriptor.layout_id(),
                                                                             })) {
            if (expected_index >= layout.chunks().size()) {
                throw std::runtime_error("stored object layout contains unexpected chunk rows");
            }

            const ChunkRef& expected = layout.chunks()[expected_index];

            if (stored_index != size_to_postgres_bigint(expected_index, "chunk index") ||
                stored_chunk_id != expected.chunk_id ||
                stored_offset != to_postgres_bigint(expected.offset, "chunk byte offset") ||
                stored_size != to_postgres_bigint(expected.size, "chunk size")) {
                throw std::runtime_error("stored object layout does not match descriptor");
            }

            ++expected_index;
        }

        if (expected_index != layout.chunks().size()) {
            throw std::runtime_error("stored object layout is missing chunk rows");
        }
    }

    void verify_registered_manifest(pqxx::work& transaction, const Manifest& manifest) {
        auto stored = transaction.query01<int, std::string, long long>(
            "SELECT "
            "    descriptor_version, "
            "    encode("
            "        canonical_descriptor, "
            "        'hex'"
            "    ), "
            "    entry_count "
            "FROM manifests "
            "WHERE manifest_id = $1",
            pqxx::params{
                manifest.manifest_id(),
            });

        if (!stored.has_value()) {
            throw std::runtime_error("registered manifest is missing from database");
        }

        const auto& [descriptor_version, stored_descriptor_hex, stored_entry_count] = *stored;

        if (std::cmp_not_equal(descriptor_version, Manifest::kFormatVersion) ||
            stored_descriptor_hex != bytes_to_hex(std::span<const std::byte>{
                                         manifest.canonical_bytes(),
                                     }) ||
            stored_entry_count != size_to_postgres_bigint(manifest.entries().size(), "manifest entry count")) {
            throw std::runtime_error("existing manifest metadata does not match descriptor");
        }

        auto expected = manifest.entries().cbegin();

        for (const auto& [stored_role, stored_version_id] :
             transaction.query<std::string, std::string>("SELECT "
                                                         "    role, "
                                                         "    version_id "
                                                         "FROM manifest_entries "
                                                         "WHERE manifest_id = $1 "
                                                         "ORDER BY role",
                                                         pqxx::params{
                                                             manifest.manifest_id(),
                                                         })) {
            if (expected == manifest.entries().cend()) {
                throw std::runtime_error("stored manifest contains unexpected entry rows");
            }

            if (stored_role != expected->first || stored_version_id != expected->second) {
                throw std::runtime_error("stored manifest does not match descriptor");
            }

            ++expected;
        }

        if (expected != manifest.entries().cend()) {
            throw std::runtime_error("stored manifest is missing entry rows");
        }
    }

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

void PostgresMetadataRepository::register_object(const Object& object) { impl_->register_object(object); }

std::optional<Object> PostgresMetadataRepository::get_object(std::string_view object_id) {
    return impl_->get_object(object_id);
}

void PostgresMetadataRepository::register_object_layout(const ObjectLayoutDescriptor& descriptor) {
    impl_->register_object_layout(descriptor);
}

std::optional<ObjectLayoutDescriptor> PostgresMetadataRepository::get_object_layout(std::string_view layout_id) {
    return impl_->get_object_layout(layout_id);
}

std::vector<ObjectLayoutDescriptor> PostgresMetadataRepository::get_object_layouts(std::string_view object_id) {
    return impl_->get_object_layouts(object_id);
}

void PostgresMetadataRepository::create_version(const ArtifactVersion& version) { impl_->create_version(version); }

std::optional<ArtifactVersion> PostgresMetadataRepository::get_version(std::string_view version_id) {
    return impl_->get_version(version_id);
}

void PostgresMetadataRepository::set_version_state(std::string_view version_id, VersionState state) {
    impl_->set_version_state(version_id, state);
}

void PostgresMetadataRepository::set_tag(const UuidV7& artifact_id, std::string_view tag_name,
                                         std::string_view version_id) {
    impl_->set_tag(artifact_id, tag_name, version_id);
}

std::optional<std::string> PostgresMetadataRepository::get_tag(const UuidV7& artifact_id, std::string_view tag_name) {
    return impl_->get_tag(artifact_id, tag_name);
}

void PostgresMetadataRepository::register_manifest(const Manifest& manifest) { impl_->register_manifest(manifest); }

std::optional<Manifest> PostgresMetadataRepository::get_manifest(std::string_view manifest_id) {
    return impl_->get_manifest(manifest_id);
}

void PostgresMetadataRepository::create_run(const Run& run) { impl_->create_run(run); }

std::optional<Run> PostgresMetadataRepository::get_run(const UuidV7& run_id) { return impl_->get_run(run_id); }

void PostgresMetadataRepository::register_storage_location(const StorageLocation& location) {
    impl_->register_storage_location(location);
}

std::vector<StorageLocation> PostgresMetadataRepository::get_storage_locations(std::string_view chunk_id) {
    return impl_->get_storage_locations(chunk_id);
}

}  // namespace aistore::metadata
