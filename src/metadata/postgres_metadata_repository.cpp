#include "aistore/metadata/postgres_metadata_repository.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <optional>
#include <pqxx/pqxx>
#include <ranges>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "aistore/metadata/chunking.hpp"
#include "aistore/metadata/gc.hpp"
#include "aistore/metadata/lifecycle.hpp"
#include "aistore/metadata/placement.hpp"
#include "aistore/metadata/replication.hpp"
#include "aistore/metadata/restore_plan.hpp"
#include "aistore/metadata/storage_node.hpp"

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

void validate_source_node_id(std::string_view node_id) {
    if (node_id.empty() || node_id.size() > 128U) {
        throw std::invalid_argument("source node ID is invalid");
    }

    for (const char character : node_id) {
        const bool is_upper = character >= 'A' && character <= 'Z';
        const bool is_lower = character >= 'a' && character <= 'z';
        const bool is_digit = character >= '0' && character <= '9';
        const bool is_allowed_punct = character == '-' || character == '_' || character == '.';

        if (!is_upper && !is_lower && !is_digit && !is_allowed_punct) {
            throw std::invalid_argument("source node ID is invalid");
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

[[nodiscard]] std::vector<std::string> load_upload_session_placement_nodes(pqxx::work& transaction,
                                                                           std::string_view session_id) {
    std::vector<std::string> placement_node_ids;

    for (const auto& [node_id] : transaction.query<std::string>("SELECT "
                                                                "    node_id "
                                                                "FROM upload_session_nodes "
                                                                "WHERE session_id = $1::uuid "
                                                                "ORDER BY node_rank",
                                                                pqxx::params{
                                                                    session_id,
                                                                })) {
        placement_node_ids.push_back(node_id);
    }

    return placement_node_ids;
}

[[nodiscard]] std::vector<std::string> load_replication_run_placement_nodes(pqxx::work& transaction,
                                                                            std::string_view run_id) {
    std::vector<std::string> placement_node_ids;

    for (const auto& [node_id] : transaction.query<std::string>("SELECT "
                                                                "    node_id "
                                                                "FROM replication_run_nodes "
                                                                "WHERE run_id = $1::uuid "
                                                                "ORDER BY node_rank",
                                                                pqxx::params{
                                                                    run_id,
                                                                })) {
        placement_node_ids.push_back(node_id);
    }

    return placement_node_ids;
}

[[nodiscard]] StorageNode reconstruct_storage_node(std::string node_id, std::string address, int port,
                                                   const std::string& state) {
    if (port < 0 || port > 65535) {
        throw std::runtime_error("stored storage node port is out of range");
    }

    return StorageNode{
        .node_id = std::move(node_id),
        .address = std::move(address),
        .port = static_cast<std::uint16_t>(port),
        .state = storage_node_state_from_string(state),
    };
}

[[nodiscard]] bool stored_fastcdc_columns_match(std::optional<long long> stored_min,
                                                std::optional<long long> stored_avg,
                                                std::optional<long long> stored_max,
                                                const ObjectLayoutDescriptor& descriptor) {
    const std::optional<FastCdcParameters> params = descriptor.fastcdc_parameters();

    if (descriptor.chunking_strategy() == ChunkingStrategy::FixedSize) {
        return !stored_min.has_value() && !stored_avg.has_value() && !stored_max.has_value();
    }

    if (!params.has_value() || !stored_min.has_value() || !stored_avg.has_value() || !stored_max.has_value()) {
        return false;
    }

    return std::cmp_equal(*stored_min, params->min_chunk_size_bytes) &&
           std::cmp_equal(*stored_avg, params->avg_chunk_size_bytes) &&
           std::cmp_equal(*stored_max, params->max_chunk_size_bytes);
}

std::string_view upload_session_state_to_string(UploadSessionState state) {
    switch (state) {
        case UploadSessionState::Open:
            return "open";

        case UploadSessionState::Committed:
            return "committed";

        case UploadSessionState::Aborted:
            return "aborted";
    }

    throw std::logic_error("unsupported upload session state");
}

UploadSessionState upload_session_state_from_string(std::string_view state) {
    if (state == "open") {
        return UploadSessionState::Open;
    }

    if (state == "committed") {
        return UploadSessionState::Committed;
    }

    if (state == "aborted") {
        return UploadSessionState::Aborted;
    }

    throw std::runtime_error("database contains an unsupported upload session state");
}

constexpr std::string_view kLiveLayoutPredicate = R"(
    (
        EXISTS (
            SELECT 1
            FROM artifact_versions av
            WHERE av.root_object_id = ol.object_id
              AND NOT EXISTS (
                  SELECT 1
                  FROM artifact_version_retirements avr
                  WHERE avr.version_id = av.version_id
              )
        )
        OR EXISTS (
            SELECT 1
            FROM upload_session_finalizations usf
            INNER JOIN upload_sessions us ON us.session_id = usf.session_id
            WHERE usf.layout_id = ol.layout_id
              AND us.finalized_version_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM artifact_version_retirements avr
                  WHERE avr.version_id = us.finalized_version_id
              )
        )
    )
)";

constexpr std::size_t kMaxLifecycleDecisionPageLimit = 256U;
constexpr std::size_t kMaxPinReasonBytes = 256U;

[[nodiscard]] bool is_version_retired(pqxx::work& transaction, std::string_view version_id) {
    return transaction.query_value<bool>(
        "SELECT EXISTS ("
        "    SELECT 1 "
        "    FROM artifact_version_retirements "
        "    WHERE version_id = $1"
        ")",
        pqxx::params{
            version_id,
        });
}

constexpr std::string_view kMultiNodeRestorableChunkPredicate = R"(
    EXISTS (
        SELECT 1
        FROM storage_locations sl
        INNER JOIN storage_nodes sn ON sn.node_id = sl.node_id
        WHERE sl.chunk_id = olc.chunk_id
          AND sl.state = 'available'
          AND sn.state IN (
              'active',
              'draining'
          )
    )
)";

void acquire_gc_coordination_advisory_lock(pqxx::work& transaction) {
    (void)transaction.exec("SELECT pg_advisory_xact_lock($1)", pqxx::params{
                                                                   kGcCoordinationAdvisoryLockKey,
                                                               });
}

[[nodiscard]] bool gc_run_has_zero_stats(const GcRun& run) {
    const GcPhysicalStats& physical = run.physical_stats;
    const GcMetadataStats& metadata = run.metadata_stats;

    return physical.physical_chunks_scanned == 0U && physical.physical_bytes_scanned == 0U &&
           physical.collectible_chunks == 0U && physical.collectible_bytes == 0U &&
           physical.physically_deleted_chunks == 0U && physical.physically_deleted_bytes == 0U &&
           metadata.storage_locations_swept == 0U && metadata.chunk_rows_swept == 0U &&
           metadata.object_layouts_swept == 0U && metadata.objects_swept == 0U;
}

void validate_gc_run_start_request(const GcRun& requested_run) {
    if (requested_run.state != GcRunState::Open) {
        throw std::invalid_argument("start_gc_run requires an open GC run state");
    }

    validate_source_node_id(requested_run.target_node_id);

    if (!gc_run_has_zero_stats(requested_run)) {
        throw std::invalid_argument("start_gc_run requires zero stats");
    }
}

[[nodiscard]] GcRun reconstruct_gc_run_from_row(std::string run_id, std::string target_node_id, const std::string& mode,
                                                const std::string& state, long long physical_chunks_scanned,
                                                long long physical_bytes_scanned, long long collectible_chunks,
                                                long long collectible_bytes, long long physically_deleted_chunks,
                                                long long physically_deleted_bytes, long long storage_locations_swept,
                                                long long chunk_rows_swept, long long object_layouts_swept,
                                                long long objects_swept) {
    auto require_non_negative = [](long long value, std::string_view field_name) -> std::uint64_t {
        if (value < 0) {
            throw std::runtime_error(std::string{field_name} + " is negative in stored GC run");
        }

        return static_cast<std::uint64_t>(value);
    };

    return GcRun{
        .run_id =
            UuidV7{
                std::move(run_id),
            },
        .target_node_id = std::move(target_node_id),
        .mode = gc_run_mode_from_string(mode),
        .state = gc_run_state_from_string(state),
        .physical_stats =
            GcPhysicalStats{
                .physical_chunks_scanned = require_non_negative(physical_chunks_scanned, "physical_chunks_scanned"),
                .physical_bytes_scanned = require_non_negative(physical_bytes_scanned, "physical_bytes_scanned"),
                .collectible_chunks = require_non_negative(collectible_chunks, "collectible_chunks"),
                .collectible_bytes = require_non_negative(collectible_bytes, "collectible_bytes"),
                .physically_deleted_chunks =
                    require_non_negative(physically_deleted_chunks, "physically_deleted_chunks"),
                .physically_deleted_bytes = require_non_negative(physically_deleted_bytes, "physically_deleted_bytes"),
            },
        .metadata_stats =
            GcMetadataStats{
                .storage_locations_swept = require_non_negative(storage_locations_swept, "storage_locations_swept"),
                .chunk_rows_swept = require_non_negative(chunk_rows_swept, "chunk_rows_swept"),
                .object_layouts_swept = require_non_negative(object_layouts_swept, "object_layouts_swept"),
                .objects_swept = require_non_negative(objects_swept, "objects_swept"),
            },
    };
}

void ensure_no_open_upload_sessions(pqxx::work& transaction) {
    const bool open_upload_sessions_present = transaction.query_value<bool>(
        "SELECT EXISTS ("
        "    SELECT 1 "
        "    FROM upload_sessions "
        "    WHERE state = 'open'"
        ")");

    if (open_upload_sessions_present) {
        throw GcError{GcErrorKind::OpenUploadSessionsPresent, "open upload sessions are present"};
    }
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

        register_object_in_transaction(transaction, object);

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

        register_object_layout_in_transaction(transaction, descriptor);

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

    [[nodiscard]] std::optional<ArtifactVersion> load_version(pqxx::work& transaction, std::string_view version_id) {
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

        return reconstructed;
    }

    [[nodiscard]] std::optional<ArtifactVersion> get_version(std::string_view version_id) {
        validate_version_id(version_id);

        pqxx::work transaction{connection_};

        auto reconstructed = load_version(transaction, version_id);

        transaction.commit();

        return reconstructed;
    }

    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters) — frozen M5 restore-plan API parameter order
    [[nodiscard]] RestorePlan resolve_restore_plan(std::string_view version_id, std::string_view source_node_id) {
        validate_version_id(version_id);
        validate_source_node_id(source_node_id);

        pqxx::work transaction{connection_};

        auto version = load_version(transaction, version_id);

        if (!version.has_value()) {
            throw RestorePlanError{RestorePlanErrorKind::VersionNotFound,
                                   "artifact version does not exist: " + std::string{version_id}};
        }

        if (version->state() != VersionState::Committed) {
            throw RestorePlanError{RestorePlanErrorKind::VersionNotCommitted,
                                   "artifact version is not committed: " + std::string{version_id}};
        }

        if (is_version_retired(transaction, version_id)) {
            throw RestorePlanError{RestorePlanErrorKind::VersionRetired,
                                   "artifact version has been retired: " + std::string{version_id}};
        }

        auto selected_layout = transaction.query01<std::string>(
            "SELECT "
            "    layout_id "
            "FROM object_layouts ol "
            "WHERE ol.object_id = $1 "
            "  AND NOT EXISTS ( "
            "      SELECT 1 "
            "      FROM object_layout_chunks olc "
            "      WHERE olc.layout_id = ol.layout_id "
            "        AND NOT EXISTS ( "
            "            SELECT 1 "
            "            FROM storage_locations sl "
            "            WHERE sl.chunk_id = olc.chunk_id "
            "              AND sl.node_id = $2 "
            "              AND sl.state = 'available' "
            "        ) "
            "  ) "
            "ORDER BY layout_id "
            "LIMIT 1",
            pqxx::params{
                version->root_object_id(),
                source_node_id,
            });

        if (!selected_layout.has_value()) {
            throw RestorePlanError{RestorePlanErrorKind::SourceUnavailable,
                                   "no fully available layout for requested source node"};
        }

        auto descriptor = load_object_layout(transaction, std::get<0>(*selected_layout));

        if (!descriptor.has_value()) {
            throw std::runtime_error("selected restore layout could not be loaded");
        }

        if (descriptor->object_id() != version->root_object_id()) {
            throw std::runtime_error("selected restore layout object does not match version root object");
        }

        RestorePlan plan{
            .artifact_id = version->artifact_id(),
            .version_id = version->version_id(),
            .source_node_id = std::string{source_node_id},
            .layout_descriptor = std::move(*descriptor),
        };

        transaction.commit();

        return plan;
    }

    [[nodiscard]] MultiNodeRestorePlan resolve_multi_node_restore_plan(std::string_view version_id) {
        validate_version_id(version_id);

        pqxx::work transaction{connection_};

        auto version = load_version(transaction, version_id);

        if (!version.has_value()) {
            throw RestorePlanError{RestorePlanErrorKind::VersionNotFound,
                                   "artifact version does not exist: " + std::string{version_id}};
        }

        if (version->state() != VersionState::Committed) {
            throw RestorePlanError{RestorePlanErrorKind::VersionNotCommitted,
                                   "artifact version is not committed: " + std::string{version_id}};
        }

        if (is_version_retired(transaction, version_id)) {
            throw RestorePlanError{RestorePlanErrorKind::VersionRetired,
                                   "artifact version has been retired: " + std::string{version_id}};
        }

        const std::string restorable_layout_sql =
            std::string{
                "SELECT "
                "    layout_id "
                "FROM object_layouts ol "
                "WHERE ol.object_id = $1 "
                "  AND NOT EXISTS ( "
                "      SELECT 1 "
                "      FROM object_layout_chunks olc "
                "      WHERE olc.layout_id = ol.layout_id "
                "        AND NOT ",
            } +
            std::string{kMultiNodeRestorableChunkPredicate} + ") ORDER BY layout_id LIMIT 1";

        auto selected_layout = transaction.query01<std::string>(restorable_layout_sql, pqxx::params{
                                                                                           version->root_object_id(),
                                                                                       });

        if (!selected_layout.has_value()) {
            throw RestorePlanError{RestorePlanErrorKind::SourceUnavailable,
                                   "no fully restorable layout for registered active or draining nodes"};
        }

        auto descriptor = load_object_layout(transaction, std::get<0>(*selected_layout));

        if (!descriptor.has_value()) {
            throw std::runtime_error("selected multi-node restore layout could not be loaded");
        }

        if (descriptor->object_id() != version->root_object_id()) {
            throw std::runtime_error("selected multi-node restore layout object does not match version root object");
        }

        std::vector<RestoreChunkSources> chunk_sources;

        for (const ChunkRef& chunk : descriptor->layout().chunks()) {
            std::vector<RestoreNodeEndpoint> sources;

            for (const auto& [node_id, address, port] : transaction.query<std::string, std::string, int>(
                     "SELECT "
                     "    sn.node_id, "
                     "    sn.address, "
                     "    sn.port "
                     "FROM storage_locations sl "
                     "INNER JOIN storage_nodes sn ON sn.node_id = sl.node_id "
                     "WHERE sl.chunk_id = $1 "
                     "  AND sl.state = 'available' "
                     "  AND sn.state IN ("
                     "      'active', "
                     "      'draining'"
                     "  ) "
                     "ORDER BY sn.node_id",
                     pqxx::params{
                         chunk.chunk_id,
                     })) {
                if (port < 0 || port > 65535) {
                    throw std::runtime_error("stored storage node port is out of range");
                }

                sources.push_back(RestoreNodeEndpoint{
                    .node_id = node_id,
                    .address = address,
                    .port = static_cast<std::uint16_t>(port),
                });
            }

            chunk_sources.push_back(RestoreChunkSources{
                .chunk_id = chunk.chunk_id,
                .offset = chunk.offset,
                .size_bytes = chunk.size,
                .sources = std::move(sources),
            });
        }

        MultiNodeRestorePlan plan{
            .artifact_id = version->artifact_id(),
            .version_id = version->version_id(),
            .object_id = descriptor->object_id(),
            .layout_id = descriptor->layout_id(),
            .layout_descriptor = *descriptor,
            .chunks = std::move(chunk_sources),
        };

        transaction.commit();

        return plan;
    }

    void register_storage_node(const StorageNode& node) {
        validate_storage_node(node);

        pqxx::work transaction{connection_};

        transaction
            .exec(
                "INSERT INTO storage_nodes ("
                "    node_id, "
                "    address, "
                "    port, "
                "    state"
                ") "
                "VALUES ("
                "    $1, "
                "    $2, "
                "    $3, "
                "    $4"
                ") "
                "ON CONFLICT (node_id) "
                "DO UPDATE SET "
                "    address = EXCLUDED.address, "
                "    port = EXCLUDED.port, "
                "    state = EXCLUDED.state, "
                "    updated_at = CURRENT_TIMESTAMP",
                pqxx::params{
                    node.node_id,
                    node.address,
                    static_cast<int>(node.port),
                    storage_node_state_to_string(node.state),
                })
            .no_rows();

        transaction.commit();
    }

    [[nodiscard]] std::optional<StorageNode> get_storage_node(std::string_view node_id) {
        validate_storage_node_id(node_id);

        pqxx::work transaction{connection_};

        auto stored = transaction.query01<std::string, std::string, int, std::string>(
            "SELECT "
            "    node_id, "
            "    address, "
            "    port, "
            "    state "
            "FROM storage_nodes "
            "WHERE node_id = $1",
            pqxx::params{
                node_id,
            });

        if (!stored.has_value()) {
            transaction.commit();
            return std::nullopt;
        }

        auto [stored_node_id, address, port, state] = std::move(*stored);

        StorageNode node = reconstruct_storage_node(std::move(stored_node_id), std::move(address), port, state);

        transaction.commit();

        return node;
    }

    [[nodiscard]] std::vector<StorageNode> list_storage_nodes() {
        pqxx::work transaction{connection_};

        std::vector<StorageNode> nodes;

        for (const auto& [node_id, address, port, state] :
             transaction.query<std::string, std::string, int, std::string>("SELECT "
                                                                           "    node_id, "
                                                                           "    address, "
                                                                           "    port, "
                                                                           "    state "
                                                                           "FROM storage_nodes "
                                                                           "ORDER BY node_id")) {
            nodes.push_back(reconstruct_storage_node(node_id, address, port, state));
        }

        transaction.commit();

        return nodes;
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

        acquire_gc_coordination_advisory_lock(transaction);

        if (is_version_retired(transaction, version_id)) {
            throw LifecycleError{LifecycleErrorKind::VersionRetired, "cannot set tag on a retired artifact version"};
        }

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

        acquire_gc_coordination_advisory_lock(transaction);

        for (const auto& [role, version_id] : manifest.entries()) {
            if (is_version_retired(transaction, version_id)) {
                throw LifecycleError{LifecycleErrorKind::VersionRetired,
                                     "cannot register manifest referencing a retired artifact version"};
            }
        }

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

    void register_chunks(const std::vector<ChunkMetadata>& chunks) {
        if (chunks.empty()) {
            return;
        }

        std::map<std::string, std::uint64_t> unique_chunks;

        for (const ChunkMetadata& chunk : chunks) {
            validate_chunk_id(chunk.chunk_id);

            if (chunk.size_bytes == 0U) {
                throw std::invalid_argument("chunk size must be greater than zero");
            }

            const auto [iterator, inserted] = unique_chunks.emplace(chunk.chunk_id, chunk.size_bytes);

            if (!inserted && iterator->second != chunk.size_bytes) {
                throw std::invalid_argument(
                    "chunk registration batch contains conflicting sizes for the same chunk ID");
            }
        }

        pqxx::work transaction{connection_};

        for (const auto& [chunk_id, size_bytes] : unique_chunks) {
            const long long chunk_size = to_postgres_bigint(size_bytes, "chunk size");

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
                        chunk_id,
                        chunk_size,
                    })
                .no_rows();

            const auto stored_chunk_size = transaction.query_value<long long>(
                "SELECT "
                "    size_bytes "
                "FROM chunks "
                "WHERE chunk_id = $1",
                pqxx::params{
                    chunk_id,
                });

            if (stored_chunk_size != chunk_size) {
                throw std::runtime_error("existing chunk size does not match chunk identity");
            }
        }

        transaction.commit();
    }

    [[nodiscard]] std::optional<std::uint64_t> get_chunk_size(std::string_view chunk_id) {
        validate_chunk_id(chunk_id);

        pqxx::work transaction{connection_};

        const auto stored = transaction.query01<long long>(
            "SELECT "
            "    size_bytes "
            "FROM chunks "
            "WHERE chunk_id = $1",
            pqxx::params{
                chunk_id,
            });

        if (!stored.has_value()) {
            transaction.commit();
            return std::nullopt;
        }

        const auto [size_bytes] = *stored;

        if (size_bytes <= 0) {
            throw std::runtime_error("stored chunk contains an invalid non-positive size");
        }

        transaction.commit();

        return static_cast<std::uint64_t>(size_bytes);
    }

    [[nodiscard]] ChunkNegotiationBatch negotiate_chunks(const std::vector<ChunkMetadata>& chunks) {
        std::vector<ChunkMetadata> distinct_chunks;
        std::map<std::string, std::uint64_t> seen_sizes;

        for (const ChunkMetadata& chunk : chunks) {
            validate_chunk_id(chunk.chunk_id);

            if (chunk.size_bytes == 0U) {
                throw std::invalid_argument("chunk size must be greater than zero");
            }

            (void)to_postgres_bigint(chunk.size_bytes, "chunk size");

            const auto [iterator, inserted] = seen_sizes.emplace(chunk.chunk_id, chunk.size_bytes);

            if (!inserted) {
                if (iterator->second != chunk.size_bytes) {
                    throw std::invalid_argument(
                        "chunk negotiation batch contains conflicting sizes for the same chunk ID");
                }

                continue;
            }

            distinct_chunks.push_back(chunk);
        }

        if (distinct_chunks.empty()) {
            return ChunkNegotiationBatch{
                .chunks = {},
                .conflict = std::nullopt,
            };
        }

        pqxx::work transaction{connection_};

        std::vector<bool> was_known;
        was_known.reserve(distinct_chunks.size());

        for (const ChunkMetadata& chunk : distinct_chunks) {
            const long long requested_size = to_postgres_bigint(chunk.size_bytes, "chunk size");

            const auto stored = transaction.query01<long long>(
                "SELECT "
                "    size_bytes "
                "FROM chunks "
                "WHERE chunk_id = $1",
                pqxx::params{
                    chunk.chunk_id,
                });

            if (!stored.has_value()) {
                was_known.push_back(false);
                continue;
            }

            const auto [stored_size] = *stored;

            if (stored_size <= 0) {
                throw std::runtime_error("stored chunk contains an invalid non-positive size");
            }

            if (stored_size != requested_size) {
                return ChunkNegotiationBatch{
                    .chunks = {},
                    .conflict =
                        ChunkSizeConflict{
                            .chunk_id = chunk.chunk_id,
                            .requested_size_bytes = chunk.size_bytes,
                            .stored_size_bytes = static_cast<std::uint64_t>(stored_size),
                        },
                };
            }

            was_known.push_back(true);
        }

        for (std::size_t index = 0; index < distinct_chunks.size(); ++index) {
            if (was_known[index]) {
                continue;
            }

            const ChunkMetadata& chunk = distinct_chunks[index];
            const long long chunk_size = to_postgres_bigint(chunk.size_bytes, "chunk size");

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
        }

        for (const ChunkMetadata& chunk : distinct_chunks) {
            const long long requested_size = to_postgres_bigint(chunk.size_bytes, "chunk size");

            const auto stored_chunk_size = transaction.query_value<long long>(
                "SELECT "
                "    size_bytes "
                "FROM chunks "
                "WHERE chunk_id = $1",
                pqxx::params{
                    chunk.chunk_id,
                });

            if (stored_chunk_size != requested_size) {
                return ChunkNegotiationBatch{
                    .chunks = {},
                    .conflict =
                        ChunkSizeConflict{
                            .chunk_id = chunk.chunk_id,
                            .requested_size_bytes = chunk.size_bytes,
                            .stored_size_bytes = static_cast<std::uint64_t>(stored_chunk_size),
                        },
                };
            }
        }

        std::vector<ChunkNegotiationEntry> entries;
        entries.reserve(distinct_chunks.size());

        for (std::size_t index = 0; index < distinct_chunks.size(); ++index) {
            const ChunkMetadata& chunk = distinct_chunks[index];

            std::vector<std::string> available_node_ids;

            for (const auto& [node_id] : transaction.query<std::string>("SELECT "
                                                                        "    node_id "
                                                                        "FROM storage_locations "
                                                                        "WHERE chunk_id = $1 "
                                                                        "  AND state = 'available' "
                                                                        "ORDER BY node_id",
                                                                        pqxx::params{
                                                                            chunk.chunk_id,
                                                                        })) {
                available_node_ids.push_back(node_id);
            }

            entries.push_back(ChunkNegotiationEntry{
                .chunk = chunk,
                .metadata_was_known = was_known[index],
                .available_node_ids = std::move(available_node_ids),
            });
        }

        transaction.commit();

        return ChunkNegotiationBatch{
            .chunks = std::move(entries),
            .conflict = std::nullopt,
        };
    }

    void create_upload_session(const UploadSession& session) {
        if (session.state() != UploadSessionState::Open) {
            throw std::invalid_argument("create_upload_session may only create open upload sessions");
        }

        if (session.finalized_version_id().has_value()) {
            throw std::invalid_argument("create_upload_session requires a null finalized version ID");
        }

        pqxx::work transaction{connection_};

        acquire_gc_coordination_advisory_lock(transaction);

        const bool gc_in_progress = transaction.query_value<bool>(
            "SELECT EXISTS ("
            "    SELECT 1 "
            "    FROM gc_runs "
            "    WHERE state = 'open'"
            ")");

        if (gc_in_progress) {
            throw GcError{GcErrorKind::GcInProgress, "garbage collection is in progress"};
        }

        const auto existing_session = transaction.query01<std::string>(
            "SELECT "
            "    session_id::text "
            "FROM upload_sessions "
            "WHERE session_id = $1::uuid "
            "FOR UPDATE",
            pqxx::params{
                session.session_id().str(),
            });

        if (existing_session.has_value()) {
            const UploadSession existing = load_upload_session(transaction, session.session_id());

            if (existing.state() != UploadSessionState::Open || existing.finalized_version_id().has_value() ||
                !upload_session_identity_matches(existing, session)) {
                throw std::runtime_error("existing upload session does not match requested session");
            }

            transaction.commit();
            return;
        }

        for (const std::string& placement_node_id : session.placement_node_ids()) {
            const auto stored_state = transaction.query01<std::string>(
                "SELECT "
                "    state "
                "FROM storage_nodes "
                "WHERE node_id = $1 "
                "FOR SHARE",
                pqxx::params{
                    placement_node_id,
                });

            if (!stored_state.has_value()) {
                throw std::invalid_argument("upload session placement node is not registered: " + placement_node_id);
            }

            if (storage_node_state_from_string(std::get<0>(*stored_state)) != StorageNodeState::Active) {
                throw std::invalid_argument("upload session placement node is not active: " + placement_node_id);
            }
        }

        const std::optional<long long> chunk_size_param =
            session.fixed_chunk_size_bytes().has_value()
                ? std::optional<long long>{to_postgres_bigint(*session.fixed_chunk_size_bytes(),
                                                              "upload session chunk size")}
                : std::nullopt;
        const std::optional<FastCdcParameters> fastcdc = session.fastcdc_parameters();
        const std::optional<long long> fastcdc_min =
            fastcdc.has_value()
                ? std::optional<long long>{to_postgres_bigint(fastcdc->min_chunk_size_bytes, "FastCDC min chunk size")}
                : std::nullopt;
        const std::optional<long long> fastcdc_avg =
            fastcdc.has_value()
                ? std::optional<long long>{to_postgres_bigint(fastcdc->avg_chunk_size_bytes, "FastCDC avg chunk size")}
                : std::nullopt;
        const std::optional<long long> fastcdc_max =
            fastcdc.has_value()
                ? std::optional<long long>{to_postgres_bigint(fastcdc->max_chunk_size_bytes, "FastCDC max chunk size")}
                : std::nullopt;

        transaction
            .exec(
                "INSERT INTO upload_sessions ("
                "    session_id, "
                "    artifact_id, "
                "    target_node_id, "
                "    replication_factor, "
                "    chunking_strategy, "
                "    chunk_size_bytes, "
                "    fastcdc_min_chunk_size_bytes, "
                "    fastcdc_avg_chunk_size_bytes, "
                "    fastcdc_max_chunk_size_bytes, "
                "    parent_version_id, "
                "    state, "
                "    finalized_version_id"
                ") "
                "VALUES ("
                "    $1::uuid, "
                "    $2::uuid, "
                "    $3, "
                "    $4, "
                "    $5, "
                "    $6, "
                "    $7, "
                "    $8, "
                "    $9, "
                "    $10, "
                "    $11, "
                "    $12"
                ")",
                pqxx::params{
                    session.session_id().str(),
                    session.artifact_id().str(),
                    session.target_node_id(),
                    static_cast<int>(session.replication_factor()),
                    chunking_strategy_to_string(session.chunking_strategy()),
                    chunk_size_param,
                    fastcdc_min,
                    fastcdc_avg,
                    fastcdc_max,
                    session.parent_version_id(),
                    upload_session_state_to_string(session.state()),
                    session.finalized_version_id(),
                })
            .no_rows();

        for (std::size_t rank = 0; rank < session.placement_node_ids().size(); ++rank) {
            transaction
                .exec(
                    "INSERT INTO upload_session_nodes ("
                    "    session_id, "
                    "    node_rank, "
                    "    node_id"
                    ") "
                    "VALUES ("
                    "    $1::uuid, "
                    "    $2, "
                    "    $3"
                    ")",
                    pqxx::params{
                        session.session_id().str(),
                        static_cast<int>(rank),
                        session.placement_node_ids()[rank],
                    })
                .no_rows();
        }

        for (const auto& [key, value] : session.immutable_metadata()) {
            transaction
                .exec(
                    "INSERT INTO upload_session_metadata ("
                    "    session_id, "
                    "    metadata_key, "
                    "    metadata_value"
                    ") "
                    "VALUES ("
                    "    $1::uuid, "
                    "    $2, "
                    "    $3"
                    ")",
                    pqxx::params{
                        session.session_id().str(),
                        key,
                        value,
                    })
                .no_rows();
        }

        const UploadSession verified = load_upload_session(transaction, session.session_id());

        if (!upload_session_identity_matches(verified, session) || verified.state() != UploadSessionState::Open ||
            verified.finalized_version_id().has_value()) {
            throw std::runtime_error("persisted upload session does not match requested session");
        }

        transaction.commit();
    }

    [[nodiscard]] std::optional<UploadSession> get_upload_session(const UuidV7& session_id) {
        pqxx::work transaction{connection_};

        auto stored =
            transaction.query01<std::string, std::string, std::string, int, std::string, std::optional<long long>,
                                std::optional<long long>, std::optional<long long>, std::optional<long long>,
                                std::optional<std::string>, std::string, std::optional<std::string>>(
                "SELECT "
                "    session_id::text, "
                "    artifact_id::text, "
                "    target_node_id, "
                "    replication_factor, "
                "    chunking_strategy, "
                "    chunk_size_bytes, "
                "    fastcdc_min_chunk_size_bytes, "
                "    fastcdc_avg_chunk_size_bytes, "
                "    fastcdc_max_chunk_size_bytes, "
                "    parent_version_id, "
                "    state, "
                "    finalized_version_id "
                "FROM upload_sessions "
                "WHERE session_id = $1::uuid",
                pqxx::params{
                    session_id.str(),
                });

        if (!stored.has_value()) {
            transaction.commit();
            return std::nullopt;
        }

        UploadSession session = reconstruct_upload_session(transaction, std::move(*stored));

        transaction.commit();

        return session;
    }

    void abort_upload_session(const UuidV7& session_id) {
        pqxx::work transaction{connection_};

        const auto stored = transaction.query01<std::string>(
            "SELECT "
            "    state "
            "FROM upload_sessions "
            "WHERE session_id = $1::uuid "
            "FOR UPDATE",
            pqxx::params{
                session_id.str(),
            });

        if (!stored.has_value()) {
            throw std::runtime_error("upload session does not exist");
        }

        const auto [state] = *stored;
        const UploadSessionState current_state = upload_session_state_from_string(state);

        if (current_state == UploadSessionState::Committed) {
            throw std::runtime_error("committed upload session cannot be aborted");
        }

        if (current_state == UploadSessionState::Open) {
            transaction
                .exec(
                    "UPDATE upload_sessions "
                    "SET "
                    "    state = 'aborted', "
                    "    updated_at = CURRENT_TIMESTAMP "
                    "WHERE session_id = $1::uuid",
                    pqxx::params{
                        session_id.str(),
                    })
                .no_rows();
        }

        transaction.commit();
    }

    [[nodiscard]] FinalizeUploadResult finalize_upload(const UuidV7& session_id,
                                                       const ObjectLayoutDescriptor& descriptor) {
        pqxx::work transaction{connection_};

        auto stored =
            transaction.query01<std::string, std::string, std::string, int, std::string, std::optional<long long>,
                                std::optional<long long>, std::optional<long long>, std::optional<long long>,
                                std::optional<std::string>, std::string, std::optional<std::string>>(
                "SELECT "
                "    session_id::text, "
                "    artifact_id::text, "
                "    target_node_id, "
                "    replication_factor, "
                "    chunking_strategy, "
                "    chunk_size_bytes, "
                "    fastcdc_min_chunk_size_bytes, "
                "    fastcdc_avg_chunk_size_bytes, "
                "    fastcdc_max_chunk_size_bytes, "
                "    parent_version_id, "
                "    state, "
                "    finalized_version_id "
                "FROM upload_sessions "
                "WHERE session_id = $1::uuid "
                "FOR UPDATE",
                pqxx::params{
                    session_id.str(),
                });

        if (!stored.has_value()) {
            throw FinalizeUploadError{
                FinalizeUploadErrorKind::SessionNotFound,
                "upload session does not exist",
            };
        }

        const UploadSession session = reconstruct_upload_session(transaction, std::move(*stored));
        const ArtifactVersion version{
            session.artifact_id(),        descriptor.object_id(),  session.parent_version_id(),
            session.immutable_metadata(), VersionState::Committed,
        };

        if (session.state() == UploadSessionState::Aborted) {
            throw FinalizeUploadError{
                FinalizeUploadErrorKind::SessionNotOpen,
                "upload session is aborted",
            };
        }

        if (is_version_retired(transaction, version.version_id())) {
            throw FinalizeUploadError{
                FinalizeUploadErrorKind::VersionRetired,
                "artifact version has been retired",
            };
        }

        if (session.state() == UploadSessionState::Committed) {
            if (!session.finalized_version_id().has_value()) {
                throw FinalizeUploadError{
                    FinalizeUploadErrorKind::Conflict,
                    "committed upload session has no finalized version ID",
                };
            }

            if (is_version_retired(transaction, *session.finalized_version_id())) {
                throw FinalizeUploadError{
                    FinalizeUploadErrorKind::VersionRetired,
                    "artifact version has been retired",
                };
            }

            const auto finalized_layout = transaction.query01<std::optional<std::string>>(
                "SELECT "
                "    layout_id "
                "FROM upload_session_finalizations "
                "WHERE session_id = $1::uuid",
                pqxx::params{
                    session_id.str(),
                });

            const std::optional<std::string> stored_finalized_layout_id =
                finalized_layout.has_value() ? std::get<0>(*finalized_layout) : std::nullopt;

            if (!stored_finalized_layout_id.has_value() || *stored_finalized_layout_id != descriptor.layout_id() ||
                *session.finalized_version_id() != version.version_id()) {
                throw FinalizeUploadError{
                    FinalizeUploadErrorKind::Conflict,
                    "finalize upload retry does not match the committed result",
                };
            }

            const auto stored_object_size = transaction.query01<long long>(
                "SELECT "
                "    total_size_bytes "
                "FROM objects "
                "WHERE object_id = $1",
                pqxx::params{
                    descriptor.object_id(),
                });

            if (!stored_object_size.has_value() ||
                std::get<0>(*stored_object_size) !=
                    to_postgres_bigint(descriptor.object().total_size(), "object total size")) {
                throw FinalizeUploadError{
                    FinalizeUploadErrorKind::Conflict,
                    "finalized object does not match the submitted descriptor",
                };
            }

            std::optional<ObjectLayoutDescriptor> stored_layout;

            try {
                stored_layout = load_object_layout(transaction, descriptor.layout_id());
            } catch (const std::runtime_error&) {
                throw FinalizeUploadError{
                    FinalizeUploadErrorKind::Conflict,
                    "finalized object layout is inconsistent with the submitted descriptor",
                };
            }

            if (!stored_layout.has_value() || stored_layout->object_id() != descriptor.object_id() ||
                stored_layout->chunking_strategy() != descriptor.chunking_strategy() ||
                stored_layout->canonical_bytes() != descriptor.canonical_bytes()) {
                throw FinalizeUploadError{
                    FinalizeUploadErrorKind::Conflict,
                    "finalized object layout does not match the submitted descriptor",
                };
            }

            if (!artifact_version_identity_matches(transaction, version)) {
                throw FinalizeUploadError{
                    FinalizeUploadErrorKind::Conflict,
                    "finalized artifact version does not match the upload session",
                };
            }

            transaction.commit();

            return FinalizeUploadResult{
                .session_id = session_id,
                .version_id = version.version_id(),
                .object_id = descriptor.object_id(),
                .layout_id = descriptor.layout_id(),
            };
        }

        if (descriptor.chunking_strategy() != session.chunking_strategy()) {
            throw std::invalid_argument("object layout chunking strategy does not match upload session");
        }

        const auto& chunks = descriptor.layout().chunks();

        if (!chunks.empty() && session.chunking_strategy() == ChunkingStrategy::FixedSize) {
            for (std::size_t index = 0; index + 1U < chunks.size(); ++index) {
                if (chunks[index].size != session.chunk_size_bytes()) {
                    throw std::invalid_argument("non-final chunk size does not match upload session chunk size");
                }
            }

            const std::uint64_t final_size = chunks.back().size;

            if (final_size == 0U || final_size > session.chunk_size_bytes()) {
                throw std::invalid_argument("final chunk size is invalid for upload session chunk size");
            }
        }

        if (!chunks.empty() && session.chunking_strategy() == ChunkingStrategy::FastCdc) {
            if (!session.fastcdc_parameters().has_value() || !descriptor.fastcdc_parameters().has_value() ||
                *session.fastcdc_parameters() != *descriptor.fastcdc_parameters()) {
                throw std::invalid_argument("object layout FastCDC parameters do not match upload session");
            }

            const FastCdcParameters params = *session.fastcdc_parameters();

            for (std::size_t index = 0; index + 1U < chunks.size(); ++index) {
                const std::uint64_t chunk_size = chunks[index].size;

                if (chunk_size < params.min_chunk_size_bytes || chunk_size > params.max_chunk_size_bytes) {
                    throw std::invalid_argument("non-final chunk size is outside upload session FastCDC bounds");
                }
            }

            const std::uint64_t final_size = chunks.back().size;

            if (final_size == 0U || final_size > params.max_chunk_size_bytes) {
                throw std::invalid_argument("final chunk size is invalid for upload session FastCDC bounds");
            }
        }

        std::map<std::string, std::uint64_t> unique_chunks;

        for (const ChunkRef& chunk : chunks) {
            const auto [existing, inserted] = unique_chunks.emplace(chunk.chunk_id, chunk.size);

            if (!inserted && existing->second != chunk.size) {
                throw FinalizeUploadError{
                    FinalizeUploadErrorKind::Conflict,
                    "object layout uses the same chunk ID with different sizes",
                    chunk.chunk_id,
                };
            }
        }

        for (const auto& [chunk_id, chunk_size] : unique_chunks) {
            const auto stored_chunk = transaction.query01<long long>(
                "SELECT "
                "    size_bytes "
                "FROM chunks "
                "WHERE chunk_id = $1 "
                "FOR SHARE",
                pqxx::params{
                    chunk_id,
                });

            if (!stored_chunk.has_value() ||
                std::get<0>(*stored_chunk) != to_postgres_bigint(chunk_size, "chunk size")) {
                throw FinalizeUploadError{
                    FinalizeUploadErrorKind::Conflict,
                    "chunk metadata is missing or has a conflicting size",
                    chunk_id,
                };
            }

            const std::vector<std::string> desired_node_ids =
                select_replica_nodes(chunk_id, session.placement_node_ids(), session.replication_factor());

            for (const std::string& desired_node_id : desired_node_ids) {
                const auto stored_location = transaction.query01<std::string>(
                    "SELECT "
                    "    state "
                    "FROM storage_locations "
                    "WHERE chunk_id = $1 "
                    "  AND node_id = $2 "
                    "FOR SHARE",
                    pqxx::params{
                        chunk_id,
                        desired_node_id,
                    });

                if (!stored_location.has_value() || storage_location_state_from_string(std::get<0>(*stored_location)) !=
                                                        StorageLocationState::Available) {
                    throw FinalizeUploadError{
                        FinalizeUploadErrorKind::ChunkUnderReplicated,
                        "chunk is not available on every desired replica node",
                        chunk_id,
                    };
                }
            }
        }

        try {
            register_object_in_transaction(transaction, descriptor.object());
            register_object_layout_in_transaction(transaction, descriptor);
        } catch (const std::runtime_error&) {
            throw FinalizeUploadError{
                FinalizeUploadErrorKind::Conflict,
                "registered object or layout conflicts with the submitted descriptor",
            };
        }

        const pqxx::result version_insert = transaction.exec(
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
            ") "
            "ON CONFLICT (version_id) "
            "DO NOTHING",
            pqxx::params{
                version.version_id(),
                version.artifact_id().str(),
                version.root_object_id(),
                version.parent_version_id(),
                static_cast<int>(ArtifactVersion::kFormatVersion),
                version.canonical_bytes(),
                version_state_to_string(version.state()),
            });

        if (version_insert.affected_rows() == 1U) {
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
        } else {
            if (!artifact_version_identity_matches(transaction, version)) {
                throw FinalizeUploadError{
                    FinalizeUploadErrorKind::Conflict,
                    "existing artifact version conflicts with the finalized upload",
                };
            }

            const pqxx::result version_update = transaction.exec(
                "UPDATE artifact_versions "
                "SET state = 'committed' "
                "WHERE version_id = $1 "
                "  AND state <> 'committed'",
                pqxx::params{
                    version.version_id(),
                });

            if (version_update.affected_rows() > 1) {
                throw std::runtime_error("updated more than one artifact version");
            }
        }

        const pqxx::result finalization_insert = transaction.exec(
            "INSERT INTO upload_session_finalizations ("
            "    session_id, "
            "    layout_id"
            ") "
            "VALUES ("
            "    $1::uuid, "
            "    $2"
            ") "
            "ON CONFLICT (session_id) "
            "DO NOTHING",
            pqxx::params{
                session_id.str(),
                descriptor.layout_id(),
            });

        if (finalization_insert.affected_rows() != 1U) {
            throw FinalizeUploadError{
                FinalizeUploadErrorKind::Conflict,
                "upload session already has a conflicting finalization record",
            };
        }

        const pqxx::result session_update = transaction.exec(
            "UPDATE upload_sessions "
            "SET "
            "    state = 'committed', "
            "    finalized_version_id = $2, "
            "    updated_at = CURRENT_TIMESTAMP "
            "WHERE session_id = $1::uuid",
            pqxx::params{
                session_id.str(),
                version.version_id(),
            });

        if (session_update.affected_rows() != 1U) {
            throw std::runtime_error("upload session disappeared during finalization");
        }

        transaction.commit();

        return FinalizeUploadResult{
            .session_id = session_id,
            .version_id = version.version_id(),
            .object_id = descriptor.object_id(),
            .layout_id = descriptor.layout_id(),
        };
    }

    [[nodiscard]] GcRun start_gc_run(const GcRun& requested_run) {
        validate_gc_run_start_request(requested_run);

        pqxx::work transaction{connection_};

        acquire_gc_coordination_advisory_lock(transaction);

        const auto existing = load_gc_run(transaction, requested_run.run_id);

        if (existing.has_value()) {
            if (existing->target_node_id == requested_run.target_node_id && existing->mode == requested_run.mode) {
                transaction.commit();
                return *existing;
            }

            throw GcError{GcErrorKind::RunConflict, "GC run ID conflicts with an existing run"};
        }

        const bool another_open_run = transaction.query_value<bool>(
            "SELECT EXISTS ("
            "    SELECT 1 "
            "    FROM gc_runs "
            "    WHERE state = 'open'"
            ")");

        if (another_open_run) {
            throw GcError{GcErrorKind::AnotherRunOpen, "another GC run is already open"};
        }

        const bool replication_in_progress = transaction.query_value<bool>(
            "SELECT EXISTS ("
            "    SELECT 1 "
            "    FROM replication_runs "
            "    WHERE state = 'open'"
            ")");

        if (replication_in_progress) {
            throw GcError{GcErrorKind::ReplicationInProgress, "replication is in progress"};
        }

        ensure_no_open_upload_sessions(transaction);

        transaction
            .exec(
                "INSERT INTO gc_runs ("
                "    run_id, "
                "    target_node_id, "
                "    mode, "
                "    state"
                ") "
                "VALUES ("
                "    $1::uuid, "
                "    $2, "
                "    $3, "
                "    $4"
                ")",
                pqxx::params{
                    requested_run.run_id.str(),
                    requested_run.target_node_id,
                    gc_run_mode_to_string(requested_run.mode),
                    gc_run_state_to_string(requested_run.state),
                })
            .no_rows();

        const auto loaded = load_gc_run(transaction, requested_run.run_id);

        if (!loaded.has_value()) {
            throw std::runtime_error("GC run disappeared immediately after insert");
        }

        transaction.commit();

        return *loaded;
    }

    [[nodiscard]] std::optional<GcRun> get_gc_run(const UuidV7& run_id) {
        pqxx::work transaction{connection_};

        const auto loaded = load_gc_run(transaction, run_id);

        transaction.commit();

        return loaded;
    }

    [[nodiscard]] std::vector<GcChunkDecision> classify_gc_chunks(const UuidV7& run_id,
                                                                  const std::vector<std::string>& chunk_ids) {
        if (chunk_ids.empty() || chunk_ids.size() > 256U) {
            throw std::invalid_argument("classify_gc_chunks requires 1 to 256 chunk IDs");
        }

        std::set<std::string_view> unique_chunk_ids;

        for (const std::string& chunk_id : chunk_ids) {
            validate_chunk_id(chunk_id);

            if (!unique_chunk_ids.insert(chunk_id).second) {
                throw std::invalid_argument("classify_gc_chunks requires unique chunk IDs");
            }
        }

        pqxx::work transaction{connection_};

        const auto run = load_gc_run(transaction, run_id);

        if (!run.has_value()) {
            throw GcError{GcErrorKind::RunNotFound, "GC run does not exist"};
        }

        if (run->state != GcRunState::Open) {
            throw GcError{GcErrorKind::RunNotOpen, "GC run is not open"};
        }

        std::vector<GcChunkDecision> decisions;
        decisions.reserve(chunk_ids.size());

        const std::string collectible_sql =
            std::string{
                "SELECT NOT EXISTS ("
                "    SELECT 1 "
                "    FROM object_layout_chunks olc "
                "    INNER JOIN object_layouts ol ON ol.layout_id = olc.layout_id "
                "    WHERE olc.chunk_id = $1 "
                "      AND ",
            } +
            std::string{kLiveLayoutPredicate} + ")";

        for (const std::string& chunk_id : chunk_ids) {
            const bool collectible = transaction.query_value<bool>(collectible_sql, pqxx::params{chunk_id});

            decisions.push_back(GcChunkDecision{
                .chunk_id = chunk_id,
                .collectible = collectible,
            });
        }

        transaction.commit();

        return decisions;
    }

    [[nodiscard]] GcRun complete_gc_run(const UuidV7& run_id, const GcPhysicalStats& physical_stats) {
        pqxx::work transaction{connection_};

        acquire_gc_coordination_advisory_lock(transaction);

        const pqxx::result locked_run = transaction.exec(
            "SELECT "
            "    run_id::text, "
            "    target_node_id, "
            "    mode, "
            "    state, "
            "    physical_chunks_scanned, "
            "    physical_bytes_scanned, "
            "    collectible_chunks, "
            "    collectible_bytes, "
            "    physically_deleted_chunks, "
            "    physically_deleted_bytes, "
            "    storage_locations_swept, "
            "    chunk_rows_swept, "
            "    object_layouts_swept, "
            "    objects_swept "
            "FROM gc_runs "
            "WHERE run_id = $1::uuid "
            "FOR UPDATE",
            pqxx::params{
                run_id.str(),
            });

        if (locked_run.empty()) {
            throw GcError{GcErrorKind::RunNotFound, "GC run does not exist"};
        }

        const pqxx::row row{locked_run[0]};

        GcRun stored_run = reconstruct_gc_run_from_row(
            row[0].as<std::string>(), row[1].as<std::string>(), row[2].as<std::string>(), row[3].as<std::string>(),
            row[4].as<long long>(), row[5].as<long long>(), row[6].as<long long>(), row[7].as<long long>(),
            row[8].as<long long>(), row[9].as<long long>(), row[10].as<long long>(), row[11].as<long long>(),
            row[12].as<long long>(), row[13].as<long long>());

        if (stored_run.state == GcRunState::Completed) {
            transaction.commit();
            return stored_run;
        }

        if (stored_run.state != GcRunState::Open) {
            throw GcError{GcErrorKind::RunNotOpen, "GC run is not open"};
        }

        ensure_no_open_upload_sessions(transaction);

        const GcMetadataStats metadata_stats =
            stored_run.mode == GcRunMode::DryRun
                ? count_gc_metadata_sweep_candidates(transaction, stored_run.target_node_id)
                : sweep_gc_metadata(transaction, stored_run.target_node_id);

        transaction
            .exec(
                "UPDATE gc_runs "
                "SET "
                "    physical_chunks_scanned = $2, "
                "    physical_bytes_scanned = $3, "
                "    collectible_chunks = $4, "
                "    collectible_bytes = $5, "
                "    physically_deleted_chunks = $6, "
                "    physically_deleted_bytes = $7, "
                "    storage_locations_swept = $8, "
                "    chunk_rows_swept = $9, "
                "    object_layouts_swept = $10, "
                "    objects_swept = $11, "
                "    state = 'completed', "
                "    completed_at = CURRENT_TIMESTAMP, "
                "    updated_at = CURRENT_TIMESTAMP "
                "WHERE run_id = $1::uuid",
                pqxx::params{
                    run_id.str(),
                    to_postgres_bigint(physical_stats.physical_chunks_scanned, "physical_chunks_scanned"),
                    to_postgres_bigint(physical_stats.physical_bytes_scanned, "physical_bytes_scanned"),
                    to_postgres_bigint(physical_stats.collectible_chunks, "collectible_chunks"),
                    to_postgres_bigint(physical_stats.collectible_bytes, "collectible_bytes"),
                    to_postgres_bigint(physical_stats.physically_deleted_chunks, "physically_deleted_chunks"),
                    to_postgres_bigint(physical_stats.physically_deleted_bytes, "physically_deleted_bytes"),
                    to_postgres_bigint(metadata_stats.storage_locations_swept, "storage_locations_swept"),
                    to_postgres_bigint(metadata_stats.chunk_rows_swept, "chunk_rows_swept"),
                    to_postgres_bigint(metadata_stats.object_layouts_swept, "object_layouts_swept"),
                    to_postgres_bigint(metadata_stats.objects_swept, "objects_swept"),
                })
            .no_rows();

        const auto completed = load_gc_run(transaction, run_id);

        if (!completed.has_value()) {
            throw std::runtime_error("GC run disappeared immediately after completion");
        }

        transaction.commit();

        return *completed;
    }

    void register_lifecycle_policy(const LifecyclePolicy& policy) {
        validate_lifecycle_policy(policy);

        pqxx::work transaction{connection_};

        const auto existing = load_lifecycle_policy(transaction, policy.policy_id);

        if (existing.has_value()) {
            if (existing->name == policy.name && existing->rules == policy.rules) {
                transaction.commit();
                return;
            }

            throw LifecycleError{LifecycleErrorKind::PolicyConflict,
                                 "lifecycle policy ID conflicts with a different configuration"};
        }

        transaction
            .exec(
                "INSERT INTO lifecycle_policies ("
                "    policy_id, "
                "    name"
                ") "
                "VALUES ("
                "    $1::uuid, "
                "    $2"
                ")",
                pqxx::params{
                    policy.policy_id.str(),
                    policy.name,
                })
            .no_rows();

        for (const auto& [kind, rule] : policy.rules) {
            transaction
                .exec(
                    "INSERT INTO lifecycle_policy_rules ("
                    "    policy_id, "
                    "    artifact_kind, "
                    "    keep_last_n, "
                    "    max_age_seconds"
                    ") "
                    "VALUES ("
                    "    $1::uuid, "
                    "    $2, "
                    "    $3, "
                    "    $4"
                    ")",
                    pqxx::params{
                        policy.policy_id.str(),
                        std::string{artifact_kind_to_string(kind)},
                        static_cast<int>(rule.keep_last_n),
                        rule.max_age_seconds.has_value()
                            ? std::optional<long long>{to_postgres_bigint(*rule.max_age_seconds, "max_age_seconds")}
                            : std::nullopt,
                    })
                .no_rows();
        }

        transaction.commit();
    }

    [[nodiscard]] std::optional<LifecyclePolicy> get_lifecycle_policy(const UuidV7& policy_id) {
        pqxx::work transaction{connection_};

        auto loaded = load_lifecycle_policy(transaction, policy_id);

        transaction.commit();

        return loaded;
    }

    void pin_version(std::string_view version_id, std::string_view reason) {
        validate_version_id(version_id);

        if (reason.empty() || reason.size() > kMaxPinReasonBytes) {
            throw LifecycleError{LifecycleErrorKind::InvalidRequest, "pin reason is invalid"};
        }

        pqxx::work transaction{connection_};

        acquire_gc_coordination_advisory_lock(transaction);

        const auto version = load_version(transaction, version_id);

        if (!version.has_value()) {
            throw LifecycleError{LifecycleErrorKind::VersionNotFound,
                                 "artifact version does not exist: " + std::string{version_id}};
        }

        if (version->state() != VersionState::Committed) {
            throw LifecycleError{LifecycleErrorKind::InvalidRequest, "only committed artifact versions can be pinned"};
        }

        if (is_version_retired(transaction, version_id)) {
            throw LifecycleError{LifecycleErrorKind::VersionRetired, "cannot pin a retired artifact version"};
        }

        transaction
            .exec(
                "INSERT INTO artifact_version_pins ("
                "    version_id, "
                "    reason"
                ") "
                "VALUES ("
                "    $1, "
                "    $2"
                ") "
                "ON CONFLICT (version_id) "
                "DO UPDATE SET "
                "    reason = EXCLUDED.reason, "
                "    pinned_at = CURRENT_TIMESTAMP",
                pqxx::params{
                    version_id,
                    reason,
                })
            .no_rows();

        transaction.commit();
    }

    [[nodiscard]] bool unpin_version(std::string_view version_id) {
        validate_version_id(version_id);

        pqxx::work transaction{connection_};

        acquire_gc_coordination_advisory_lock(transaction);

        const pqxx::result deleted = transaction.exec(
            "DELETE FROM artifact_version_pins "
            "WHERE version_id = $1",
            pqxx::params{
                version_id,
            });

        transaction.commit();

        return deleted.affected_rows() > 0;
    }

    [[nodiscard]] std::optional<std::string> get_version_pin(std::string_view version_id) {
        validate_version_id(version_id);

        pqxx::work transaction{connection_};

        auto stored = transaction.query01<std::string>(
            "SELECT "
            "    reason "
            "FROM artifact_version_pins "
            "WHERE version_id = $1",
            pqxx::params{
                version_id,
            });

        if (!stored.has_value()) {
            transaction.commit();
            return std::nullopt;
        }

        auto [reason] = std::move(*stored);

        transaction.commit();

        return reason;
    }

    [[nodiscard]] LifecycleRun run_lifecycle(const UuidV7& run_id, const UuidV7& policy_id, LifecycleRunMode mode) {
        pqxx::work transaction{connection_};

        acquire_gc_coordination_advisory_lock(transaction);

        const auto existing = load_lifecycle_run(transaction, run_id);

        if (existing.has_value()) {
            if (existing->policy_id == policy_id && existing->mode == mode) {
                transaction.commit();
                return *existing;
            }

            throw LifecycleError{LifecycleErrorKind::RunConflict, "lifecycle run ID conflicts with an existing run"};
        }

        if (transaction.query_value<bool>("SELECT EXISTS (SELECT 1 FROM upload_sessions WHERE state = 'open')")) {
            throw LifecycleError{LifecycleErrorKind::BlockedByOpenUploadSessions, "open upload sessions are present"};
        }

        if (transaction.query_value<bool>("SELECT EXISTS (SELECT 1 FROM gc_runs WHERE state = 'open')")) {
            throw LifecycleError{LifecycleErrorKind::BlockedByGc, "an open GC run is present"};
        }

        if (transaction.query_value<bool>("SELECT EXISTS (SELECT 1 FROM replication_runs WHERE state = 'open')")) {
            throw LifecycleError{LifecycleErrorKind::BlockedByReplication, "an open replication run is present"};
        }

        const auto policy = load_lifecycle_policy(transaction, policy_id);

        if (!policy.has_value()) {
            throw LifecycleError{LifecycleErrorKind::PolicyNotFound, "lifecycle policy does not exist"};
        }

        const auto evaluated_at_ms =
            transaction.query_value<long long>("SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::bigint");

        struct Candidate {
            std::string version_id;
            UuidV7 artifact_id;
            ArtifactKind artifact_kind = ArtifactKind::Generic;
            double created_at_epoch = 0.0;
            std::uint64_t logical_size_bytes = 0;
            bool pinned = false;
            bool tagged = false;
            bool manifest_referenced = false;
            std::uint32_t rank_in_kind = 0;
        };

        std::vector<Candidate> candidates;

        for (auto row :
             transaction
                 .query<std::string, std::string, double, long long, std::optional<std::string>, bool, bool, bool>(
                     "SELECT "
                     "    av.version_id, "
                     "    av.artifact_id::text, "
                     "    EXTRACT(EPOCH FROM av.created_at), "
                     "    o.total_size_bytes, "
                     "    ("
                     "        SELECT avm.metadata_value "
                     "        FROM artifact_version_metadata avm "
                     "        WHERE avm.version_id = av.version_id "
                     "          AND avm.metadata_key = $1"
                     "    ), "
                     "    EXISTS ("
                     "        SELECT 1 FROM artifact_version_pins avp WHERE avp.version_id = av.version_id"
                     "    ), "
                     "    EXISTS ("
                     "        SELECT 1 FROM tags t WHERE t.version_id = av.version_id"
                     "    ), "
                     "    EXISTS ("
                     "        SELECT 1 FROM manifest_entries me WHERE me.version_id = av.version_id"
                     "    ) "
                     "FROM artifact_versions av "
                     "INNER JOIN objects o ON o.object_id = av.root_object_id "
                     "WHERE av.state = 'committed' "
                     "  AND NOT EXISTS ("
                     "      SELECT 1 FROM artifact_version_retirements avr WHERE avr.version_id = av.version_id"
                     "  )",
                     pqxx::params{
                         std::string{kArtifactKindMetadataKey},
                     })) {
            auto [version_id, artifact_id_text, created_at_epoch, total_size_bytes, kind_metadata, pinned, tagged,
                  manifest_referenced] = std::move(row);

            if (total_size_bytes < 0) {
                throw std::runtime_error("object total_size_bytes is negative");
            }

            candidates.push_back(Candidate{
                .version_id = std::move(version_id),
                .artifact_id =
                    UuidV7{
                        std::move(artifact_id_text),
                    },
                .artifact_kind = resolve_artifact_kind(
                    kind_metadata.has_value() ? std::optional<std::string_view>{*kind_metadata} : std::nullopt),
                .created_at_epoch = created_at_epoch,
                .logical_size_bytes = static_cast<std::uint64_t>(total_size_bytes),
                .pinned = pinned,
                .tagged = tagged,
                .manifest_referenced = manifest_referenced,
            });
        }

        std::ranges::sort(candidates, [](const Candidate& left, const Candidate& right) {
            if (left.artifact_id.str() != right.artifact_id.str()) {
                return left.artifact_id.str() < right.artifact_id.str();
            }

            const std::string_view left_kind = artifact_kind_to_string(left.artifact_kind);
            const std::string_view right_kind = artifact_kind_to_string(right.artifact_kind);

            if (left_kind != right_kind) {
                return left_kind < right_kind;
            }

            if (left.created_at_epoch != right.created_at_epoch) {
                return left.created_at_epoch > right.created_at_epoch;
            }

            return left.version_id < right.version_id;
        });

        {
            std::string partition_artifact;
            std::string partition_kind;
            std::uint32_t rank = 0;

            for (Candidate& candidate : candidates) {
                const std::string artifact_key = candidate.artifact_id.str();
                const std::string kind_key{artifact_kind_to_string(candidate.artifact_kind)};

                if (artifact_key != partition_artifact || kind_key != partition_kind) {
                    partition_artifact = artifact_key;
                    partition_kind = kind_key;
                    rank = 0;
                }

                ++rank;
                candidate.rank_in_kind = rank;
            }
        }

        std::vector<LifecycleDecision> decisions;
        decisions.reserve(candidates.size());

        LifecycleStats stats{};
        stats.versions_scanned = candidates.size();

        const double evaluated_at_seconds = static_cast<double>(evaluated_at_ms) / 1000.0;

        for (const Candidate& candidate : candidates) {
            const auto rule_it = policy->rules.find(candidate.artifact_kind);

            if (rule_it == policy->rules.end()) {
                throw std::runtime_error("lifecycle policy is missing a rule for an artifact kind");
            }

            const LifecycleRule& rule = rule_it->second;

            LifecycleDecision decision{
                .version_id = candidate.version_id,
                .artifact_id = candidate.artifact_id,
                .artifact_kind = candidate.artifact_kind,
                .retire = false,
                .reason = LifecycleDecisionReason::PolicyRetire,
                .logical_size_bytes = candidate.logical_size_bytes,
            };

            if (candidate.pinned) {
                decision.reason = LifecycleDecisionReason::Pinned;
            } else if (candidate.tagged) {
                decision.reason = LifecycleDecisionReason::Tagged;
            } else if (candidate.manifest_referenced) {
                decision.reason = LifecycleDecisionReason::ManifestReferenced;
            } else if (candidate.rank_in_kind <= rule.keep_last_n) {
                decision.reason = LifecycleDecisionReason::KeepLastN;
            } else if (rule.max_age_seconds.has_value() && (evaluated_at_seconds - candidate.created_at_epoch) <
                                                               static_cast<double>(*rule.max_age_seconds)) {
                decision.reason = LifecycleDecisionReason::AgeNotReached;
            } else {
                decision.retire = true;
                decision.reason = LifecycleDecisionReason::PolicyRetire;
            }

            switch (decision.reason) {
                case LifecycleDecisionReason::Pinned:
                case LifecycleDecisionReason::Tagged:
                case LifecycleDecisionReason::ManifestReferenced:
                    ++stats.versions_protected;
                    break;

                case LifecycleDecisionReason::KeepLastN:
                case LifecycleDecisionReason::AgeNotReached:
                    ++stats.versions_retained_by_policy;
                    break;

                case LifecycleDecisionReason::PolicyRetire:
                    ++stats.versions_candidates;
                    stats.logical_bytes_candidates += decision.logical_size_bytes;
                    break;
            }

            decisions.push_back(std::move(decision));
        }

        std::vector<LifecycleDecision> retirements_to_persist;

        if (mode == LifecycleRunMode::Apply) {
            for (const LifecycleDecision& decision : decisions) {
                if (!decision.retire || decision.reason != LifecycleDecisionReason::PolicyRetire) {
                    continue;
                }

                retirements_to_persist.push_back(decision);
                ++stats.versions_retired;
                stats.logical_bytes_retired += decision.logical_size_bytes;
            }
        }

        transaction
            .exec(
                "INSERT INTO lifecycle_runs ("
                "    run_id, "
                "    policy_id, "
                "    mode, "
                "    evaluated_at, "
                "    versions_scanned, "
                "    versions_protected, "
                "    versions_retained_by_policy, "
                "    versions_candidates, "
                "    versions_retired, "
                "    logical_bytes_candidates, "
                "    logical_bytes_retired"
                ") "
                "VALUES ("
                "    $1::uuid, "
                "    $2::uuid, "
                "    $3, "
                "    to_timestamp($4::double precision / 1000.0), "
                "    $5, "
                "    $6, "
                "    $7, "
                "    $8, "
                "    $9, "
                "    $10, "
                "    $11"
                ")",
                pqxx::params{
                    run_id.str(),
                    policy_id.str(),
                    std::string{lifecycle_run_mode_to_string(mode)},
                    evaluated_at_ms,
                    to_postgres_bigint(stats.versions_scanned, "versions_scanned"),
                    to_postgres_bigint(stats.versions_protected, "versions_protected"),
                    to_postgres_bigint(stats.versions_retained_by_policy, "versions_retained_by_policy"),
                    to_postgres_bigint(stats.versions_candidates, "versions_candidates"),
                    to_postgres_bigint(stats.versions_retired, "versions_retired"),
                    to_postgres_bigint(stats.logical_bytes_candidates, "logical_bytes_candidates"),
                    to_postgres_bigint(stats.logical_bytes_retired, "logical_bytes_retired"),
                })
            .no_rows();

        for (const LifecycleDecision& decision : retirements_to_persist) {
            transaction
                .exec(
                    "INSERT INTO artifact_version_retirements ("
                    "    version_id, "
                    "    lifecycle_run_id, "
                    "    artifact_kind, "
                    "    reason"
                    ") "
                    "VALUES ("
                    "    $1, "
                    "    $2::uuid, "
                    "    $3, "
                    "    $4"
                    ")",
                    pqxx::params{
                        decision.version_id,
                        run_id.str(),
                        std::string{artifact_kind_to_string(decision.artifact_kind)},
                        std::string{lifecycle_decision_reason_to_string(decision.reason)},
                    })
                .no_rows();
        }

        for (const LifecycleDecision& decision : decisions) {
            transaction
                .exec(
                    "INSERT INTO lifecycle_run_decisions ("
                    "    run_id, "
                    "    version_id, "
                    "    artifact_id, "
                    "    artifact_kind, "
                    "    decision, "
                    "    reason, "
                    "    logical_size_bytes"
                    ") "
                    "VALUES ("
                    "    $1::uuid, "
                    "    $2, "
                    "    $3::uuid, "
                    "    $4, "
                    "    $5, "
                    "    $6, "
                    "    $7"
                    ")",
                    pqxx::params{
                        run_id.str(),
                        decision.version_id,
                        decision.artifact_id.str(),
                        std::string{artifact_kind_to_string(decision.artifact_kind)},
                        decision.retire ? "retire" : "retain",
                        std::string{lifecycle_decision_reason_to_string(decision.reason)},
                        to_postgres_bigint(decision.logical_size_bytes, "logical_size_bytes"),
                    })
                .no_rows();
        }

        const auto loaded = load_lifecycle_run(transaction, run_id);

        if (!loaded.has_value()) {
            throw std::runtime_error("lifecycle run disappeared immediately after persistence");
        }

        transaction.commit();

        return *loaded;
    }

    [[nodiscard]] std::optional<LifecycleRun> get_lifecycle_run(const UuidV7& run_id) {
        pqxx::work transaction{connection_};

        auto loaded = load_lifecycle_run(transaction, run_id);

        transaction.commit();

        return loaded;
    }

    [[nodiscard]] std::vector<LifecycleDecision> list_lifecycle_decisions(
        const UuidV7& run_id, std::optional<std::string_view> after_version_id, std::size_t limit) {
        if (limit < 1U || limit > kMaxLifecycleDecisionPageLimit) {
            throw LifecycleError{LifecycleErrorKind::InvalidRequest,
                                 "lifecycle decision page limit must be between 1 and 256"};
        }

        if (after_version_id.has_value()) {
            validate_version_id(*after_version_id);
        }

        pqxx::work transaction{connection_};

        const bool run_exists = transaction.query_value<bool>(
            "SELECT EXISTS ("
            "    SELECT 1 "
            "    FROM lifecycle_runs "
            "    WHERE run_id = $1::uuid"
            ")",
            pqxx::params{
                run_id.str(),
            });

        if (!run_exists) {
            throw LifecycleError{LifecycleErrorKind::RunNotFound, "lifecycle run does not exist"};
        }

        std::vector<LifecycleDecision> decisions;

        auto append_row = [&](std::string version_id, std::string artifact_id_text, const std::string& artifact_kind,
                              const std::string& decision, const std::string& reason, long long logical_size_bytes) {
            if (logical_size_bytes < 0) {
                throw std::runtime_error("stored lifecycle decision has negative logical_size_bytes");
            }

            decisions.push_back(LifecycleDecision{
                .version_id = std::move(version_id),
                .artifact_id =
                    UuidV7{
                        std::move(artifact_id_text),
                    },
                .artifact_kind = artifact_kind_from_string(artifact_kind),
                .retire = decision == "retire",
                .reason = lifecycle_decision_reason_from_string(reason),
                .logical_size_bytes = static_cast<std::uint64_t>(logical_size_bytes),
            });
        };

        if (after_version_id.has_value()) {
            for (auto row :
                 transaction.query<std::string, std::string, std::string, std::string, std::string, long long>(
                     "SELECT "
                     "    version_id, "
                     "    artifact_id::text, "
                     "    artifact_kind, "
                     "    decision, "
                     "    reason, "
                     "    logical_size_bytes "
                     "FROM lifecycle_run_decisions "
                     "WHERE run_id = $1::uuid "
                     "  AND version_id > $2 "
                     "ORDER BY version_id ASC "
                     "LIMIT $3",
                     pqxx::params{
                         run_id.str(),
                         *after_version_id,
                         size_to_postgres_bigint(limit, "lifecycle decision limit"),
                     })) {
                auto [version_id, artifact_id_text, artifact_kind, decision, reason, logical_size_bytes] =
                    std::move(row);
                append_row(std::move(version_id), std::move(artifact_id_text), artifact_kind, decision, reason,
                           logical_size_bytes);
            }
        } else {
            for (auto row :
                 transaction.query<std::string, std::string, std::string, std::string, std::string, long long>(
                     "SELECT "
                     "    version_id, "
                     "    artifact_id::text, "
                     "    artifact_kind, "
                     "    decision, "
                     "    reason, "
                     "    logical_size_bytes "
                     "FROM lifecycle_run_decisions "
                     "WHERE run_id = $1::uuid "
                     "ORDER BY version_id ASC "
                     "LIMIT $2",
                     pqxx::params{
                         run_id.str(),
                         size_to_postgres_bigint(limit, "lifecycle decision limit"),
                     })) {
                auto [version_id, artifact_id_text, artifact_kind, decision, reason, logical_size_bytes] =
                    std::move(row);
                append_row(std::move(version_id), std::move(artifact_id_text), artifact_kind, decision, reason,
                           logical_size_bytes);
            }
        }

        transaction.commit();

        return decisions;
    }

    [[nodiscard]] ReplicationRun start_replication_run(const UuidV7& run_id, std::string_view version_id,
                                                       std::uint8_t replication_factor) {
        validate_version_id(version_id);

        if (replication_factor < 1U || replication_factor > 8U) {
            throw std::invalid_argument("replication factor must be between 1 and 8");
        }

        pqxx::work transaction{connection_};

        acquire_gc_coordination_advisory_lock(transaction);

        const bool gc_in_progress = transaction.query_value<bool>(
            "SELECT EXISTS ("
            "    SELECT 1 "
            "    FROM gc_runs "
            "    WHERE state = 'open'"
            ")");

        if (gc_in_progress) {
            throw ReplicationError{ReplicationErrorKind::GcInProgress, "garbage collection is in progress"};
        }

        const auto existing = load_replication_run(transaction, run_id);

        if (existing.has_value()) {
            if (existing->version_id == version_id && existing->replication_factor == replication_factor) {
                transaction.commit();
                return *existing;
            }

            throw ReplicationError{ReplicationErrorKind::RunConflict,
                                   "replication run ID conflicts with an existing run"};
        }

        const bool another_open_run = transaction.query_value<bool>(
            "SELECT EXISTS ("
            "    SELECT 1 "
            "    FROM replication_runs "
            "    WHERE state = 'open'"
            ")");

        if (another_open_run) {
            throw ReplicationError{ReplicationErrorKind::AnotherRunOpen, "another replication run is already open"};
        }

        auto version = load_version(transaction, version_id);

        if (!version.has_value()) {
            throw ReplicationError{ReplicationErrorKind::VersionNotFound,
                                   "artifact version does not exist: " + std::string{version_id}};
        }

        if (version->state() != VersionState::Committed) {
            throw ReplicationError{ReplicationErrorKind::VersionNotCommitted,
                                   "artifact version is not committed: " + std::string{version_id}};
        }

        if (is_version_retired(transaction, version_id)) {
            throw ReplicationError{ReplicationErrorKind::VersionRetired,
                                   "artifact version has been retired: " + std::string{version_id}};
        }

        const std::string restorable_layout_sql =
            std::string{
                "SELECT "
                "    layout_id "
                "FROM object_layouts ol "
                "WHERE ol.object_id = $1 "
                "  AND NOT EXISTS ( "
                "      SELECT 1 "
                "      FROM object_layout_chunks olc "
                "      WHERE olc.layout_id = ol.layout_id "
                "        AND NOT ",
            } +
            std::string{kMultiNodeRestorableChunkPredicate} + ") ORDER BY layout_id LIMIT 1";

        auto selected_layout = transaction.query01<std::string>(restorable_layout_sql, pqxx::params{
                                                                                           version->root_object_id(),
                                                                                       });

        if (!selected_layout.has_value()) {
            throw ReplicationError{ReplicationErrorKind::SourceUnavailable,
                                   "no fully restorable layout for registered active or draining nodes"};
        }

        std::vector<std::string> active_node_ids;

        for (const auto& [node_id] : transaction.query<std::string>("SELECT "
                                                                    "    node_id "
                                                                    "FROM storage_nodes "
                                                                    "WHERE state = 'active' "
                                                                    "ORDER BY node_id")) {
            active_node_ids.push_back(node_id);
        }

        if (active_node_ids.size() > 64U) {
            throw ReplicationError{ReplicationErrorKind::SourceUnavailable,
                                   "active storage node snapshot exceeds supported placement size"};
        }

        if (active_node_ids.size() < replication_factor) {
            throw ReplicationError{ReplicationErrorKind::SourceUnavailable,
                                   "active storage node count is below replication factor"};
        }

        transaction
            .exec(
                "INSERT INTO replication_runs ("
                "    run_id, "
                "    version_id, "
                "    layout_id, "
                "    replication_factor, "
                "    state"
                ") "
                "VALUES ("
                "    $1::uuid, "
                "    $2, "
                "    $3, "
                "    $4, "
                "    $5"
                ")",
                pqxx::params{
                    run_id.str(),
                    version_id,
                    std::get<0>(*selected_layout),
                    static_cast<int>(replication_factor),
                    replication_run_state_to_string(ReplicationRunState::Open),
                })
            .no_rows();

        for (std::size_t rank = 0; rank < active_node_ids.size(); ++rank) {
            transaction
                .exec(
                    "INSERT INTO replication_run_nodes ("
                    "    run_id, "
                    "    node_rank, "
                    "    node_id"
                    ") "
                    "VALUES ("
                    "    $1::uuid, "
                    "    $2, "
                    "    $3"
                    ")",
                    pqxx::params{
                        run_id.str(),
                        static_cast<int>(rank),
                        active_node_ids[rank],
                    })
                .no_rows();
        }

        const auto loaded = load_replication_run(transaction, run_id);

        if (!loaded.has_value()) {
            throw std::runtime_error("replication run disappeared immediately after insert");
        }

        transaction.commit();

        return *loaded;
    }

    [[nodiscard]] std::optional<ReplicationRun> get_replication_run(const UuidV7& run_id) {
        pqxx::work transaction{connection_};

        const auto loaded = load_replication_run(transaction, run_id);

        transaction.commit();

        return loaded;
    }

    [[nodiscard]] ReplicationPlan get_replication_plan(const UuidV7& run_id) {
        pqxx::work transaction{connection_};

        const auto run = load_replication_run(transaction, run_id);

        if (!run.has_value()) {
            throw ReplicationError{ReplicationErrorKind::RunNotFound, "replication run does not exist"};
        }

        if (run->state != ReplicationRunState::Open) {
            throw ReplicationError{ReplicationErrorKind::RunNotOpen, "replication run is not open"};
        }

        auto descriptor = load_object_layout(transaction, run->layout_id);

        if (!descriptor.has_value()) {
            throw std::runtime_error("replication run layout could not be loaded");
        }

        std::vector<ReplicationChunkPlan> chunk_plans;

        for (const ChunkRef& chunk : descriptor->layout().chunks()) {
            const std::vector<std::string> desired_node_ids =
                select_replica_nodes(chunk.chunk_id, run->placement_node_ids, run->replication_factor);

            for (const std::string& desired_node_id : desired_node_ids) {
                const auto stored_state = transaction.query01<std::string>(
                    "SELECT "
                    "    state "
                    "FROM storage_nodes "
                    "WHERE node_id = $1",
                    pqxx::params{
                        desired_node_id,
                    });

                if (!stored_state.has_value() ||
                    storage_node_state_from_string(std::get<0>(*stored_state)) == StorageNodeState::Disabled) {
                    throw ReplicationError{ReplicationErrorKind::TargetDisabled,
                                           "replication target node is disabled: " + desired_node_id};
                }
            }

            std::vector<ReplicationNodeEndpoint> source_nodes;

            for (const auto& [node_id, address, port] : transaction.query<std::string, std::string, int>(
                     "SELECT "
                     "    sn.node_id, "
                     "    sn.address, "
                     "    sn.port "
                     "FROM storage_locations sl "
                     "INNER JOIN storage_nodes sn ON sn.node_id = sl.node_id "
                     "WHERE sl.chunk_id = $1 "
                     "  AND sl.state = 'available' "
                     "  AND sn.state IN ("
                     "      'active', "
                     "      'draining'"
                     "  ) "
                     "ORDER BY sn.node_id",
                     pqxx::params{
                         chunk.chunk_id,
                     })) {
                if (port < 0 || port > 65535) {
                    throw std::runtime_error("stored storage node port is out of range");
                }

                source_nodes.push_back(ReplicationNodeEndpoint{
                    .node_id = node_id,
                    .address = address,
                    .port = static_cast<std::uint16_t>(port),
                });
            }

            std::vector<ReplicationNodeEndpoint> target_nodes;

            for (const std::string& desired_node_id : desired_node_ids) {
                auto stored_node = transaction.query01<std::string, int, std::string>(
                    "SELECT "
                    "    address, "
                    "    port, "
                    "    state "
                    "FROM storage_nodes "
                    "WHERE node_id = $1",
                    pqxx::params{
                        desired_node_id,
                    });

                if (!stored_node.has_value()) {
                    throw ReplicationError{ReplicationErrorKind::TargetDisabled,
                                           "replication target node is missing: " + desired_node_id};
                }

                auto [address, port, state] = std::move(*stored_node);

                if (storage_node_state_from_string(state) == StorageNodeState::Disabled) {
                    throw ReplicationError{ReplicationErrorKind::TargetDisabled,
                                           "replication target node is disabled: " + desired_node_id};
                }

                if (port < 0 || port > 65535) {
                    throw std::runtime_error("stored storage node port is out of range");
                }

                target_nodes.push_back(ReplicationNodeEndpoint{
                    .node_id = desired_node_id,
                    .address = address,
                    .port = static_cast<std::uint16_t>(port),
                });
            }

            chunk_plans.push_back(ReplicationChunkPlan{
                .chunk_id = chunk.chunk_id,
                .offset = chunk.offset,
                .size_bytes = chunk.size,
                .desired_node_ids = desired_node_ids,
                .source_nodes = std::move(source_nodes),
                .target_nodes = std::move(target_nodes),
            });
        }

        ReplicationPlan plan{
            .run_id = run->run_id,
            .version_id = run->version_id,
            .layout_id = run->layout_id,
            .replication_factor = run->replication_factor,
            .placement_node_ids = run->placement_node_ids,
            .chunks = std::move(chunk_plans),
        };

        transaction.commit();

        return plan;
    }

    [[nodiscard]] ReplicationRun complete_replication_run(const UuidV7& run_id, const ReplicationStats& stats) {
        pqxx::work transaction{connection_};

        acquire_gc_coordination_advisory_lock(transaction);

        const pqxx::result locked_run = transaction.exec(
            "SELECT "
            "    run_id::text, "
            "    version_id, "
            "    layout_id, "
            "    replication_factor, "
            "    state, "
            "    chunks_scanned, "
            "    chunks_under_replicated, "
            "    replicas_verified, "
            "    replicas_written, "
            "    bytes_copied, "
            "    source_failovers "
            "FROM replication_runs "
            "WHERE run_id = $1::uuid "
            "FOR UPDATE",
            pqxx::params{
                run_id.str(),
            });

        if (locked_run.empty()) {
            throw ReplicationError{ReplicationErrorKind::RunNotFound, "replication run does not exist"};
        }

        const pqxx::row row{locked_run[0]};
        ReplicationRun stored_run = reconstruct_replication_run_from_row(
            row[0].as<std::string>(), row[1].as<std::string>(), row[2].as<std::string>(), row[3].as<int>(),
            row[4].as<std::string>(), row[5].as<long long>(), row[6].as<long long>(), row[7].as<long long>(),
            row[8].as<long long>(), row[9].as<long long>(), row[10].as<long long>(),
            load_replication_run_placement_nodes(transaction, run_id.str()));

        if (stored_run.state == ReplicationRunState::Completed) {
            transaction.commit();
            return stored_run;
        }

        if (stored_run.state != ReplicationRunState::Open) {
            throw ReplicationError{ReplicationErrorKind::RunNotOpen, "replication run is not open"};
        }

        const bool gc_in_progress = transaction.query_value<bool>(
            "SELECT EXISTS ("
            "    SELECT 1 "
            "    FROM gc_runs "
            "    WHERE state = 'open'"
            ")");

        if (gc_in_progress) {
            throw ReplicationError{ReplicationErrorKind::GcInProgress, "garbage collection is in progress"};
        }

        auto descriptor = load_object_layout(transaction, stored_run.layout_id);

        if (!descriptor.has_value()) {
            throw std::runtime_error("replication run layout could not be loaded");
        }

        for (const ChunkRef& chunk : descriptor->layout().chunks()) {
            const std::vector<std::string> desired_node_ids =
                select_replica_nodes(chunk.chunk_id, stored_run.placement_node_ids, stored_run.replication_factor);

            for (const std::string& desired_node_id : desired_node_ids) {
                const auto stored_location = transaction.query01<std::string>(
                    "SELECT "
                    "    state "
                    "FROM storage_locations "
                    "WHERE chunk_id = $1 "
                    "  AND node_id = $2 "
                    "FOR SHARE",
                    pqxx::params{
                        chunk.chunk_id,
                        desired_node_id,
                    });

                if (!stored_location.has_value() || storage_location_state_from_string(std::get<0>(*stored_location)) !=
                                                        StorageLocationState::Available) {
                    throw ReplicationError{ReplicationErrorKind::UnderReplicated,
                                           "chunk is under-replicated: " + chunk.chunk_id};
                }
            }
        }

        transaction
            .exec(
                "UPDATE replication_runs "
                "SET "
                "    chunks_scanned = $2, "
                "    chunks_under_replicated = $3, "
                "    replicas_verified = $4, "
                "    replicas_written = $5, "
                "    bytes_copied = $6, "
                "    source_failovers = $7, "
                "    state = 'completed', "
                "    completed_at = CURRENT_TIMESTAMP, "
                "    updated_at = CURRENT_TIMESTAMP "
                "WHERE run_id = $1::uuid",
                pqxx::params{
                    run_id.str(),
                    to_postgres_bigint(stats.chunks_scanned, "chunks_scanned"),
                    to_postgres_bigint(stats.chunks_under_replicated, "chunks_under_replicated"),
                    to_postgres_bigint(stats.replicas_verified, "replicas_verified"),
                    to_postgres_bigint(stats.replicas_written, "replicas_written"),
                    to_postgres_bigint(stats.bytes_copied, "bytes_copied"),
                    to_postgres_bigint(stats.source_failovers, "source_failovers"),
                })
            .no_rows();

        const auto completed = load_replication_run(transaction, run_id);

        if (!completed.has_value()) {
            throw std::runtime_error("replication run disappeared immediately after completion");
        }

        transaction.commit();

        return *completed;
    }

   private:
    using StoredUploadSessionRow =
        std::tuple<std::string, std::string, std::string, int, std::string, std::optional<long long>,
                   std::optional<long long>, std::optional<long long>, std::optional<long long>,
                   std::optional<std::string>, std::string, std::optional<std::string>>;

    [[nodiscard]] ReplicationRun reconstruct_replication_run_from_row(
        std::string run_id, std::string version_id, std::string layout_id, int replication_factor,
        const std::string& state, long long chunks_scanned, long long chunks_under_replicated,
        long long replicas_verified, long long replicas_written, long long bytes_copied, long long source_failovers,
        std::vector<std::string> placement_node_ids) {
        auto require_non_negative = [](long long value, std::string_view field_name) -> std::uint64_t {
            if (value < 0) {
                throw std::runtime_error(std::string{field_name} + " is negative in stored replication run");
            }

            return static_cast<std::uint64_t>(value);
        };

        if (replication_factor < 1 || replication_factor > 8) {
            throw std::runtime_error("stored replication run has invalid replication factor");
        }

        return ReplicationRun{
            .run_id =
                UuidV7{
                    std::move(run_id),
                },
            .version_id = std::move(version_id),
            .layout_id = std::move(layout_id),
            .replication_factor = static_cast<std::uint8_t>(replication_factor),
            .placement_node_ids = std::move(placement_node_ids),
            .state = replication_run_state_from_string(state),
            .stats =
                ReplicationStats{
                    .chunks_scanned = require_non_negative(chunks_scanned, "chunks_scanned"),
                    .chunks_under_replicated = require_non_negative(chunks_under_replicated, "chunks_under_replicated"),
                    .replicas_verified = require_non_negative(replicas_verified, "replicas_verified"),
                    .replicas_written = require_non_negative(replicas_written, "replicas_written"),
                    .bytes_copied = require_non_negative(bytes_copied, "bytes_copied"),
                    .source_failovers = require_non_negative(source_failovers, "source_failovers"),
                },
        };
    }

    [[nodiscard]] std::optional<ReplicationRun> load_replication_run(pqxx::work& transaction, const UuidV7& run_id) {
        auto stored = transaction.query01<std::string, std::string, std::string, int, std::string, long long, long long,
                                          long long, long long, long long, long long>(
            "SELECT "
            "    run_id::text, "
            "    version_id, "
            "    layout_id, "
            "    replication_factor, "
            "    state, "
            "    chunks_scanned, "
            "    chunks_under_replicated, "
            "    replicas_verified, "
            "    replicas_written, "
            "    bytes_copied, "
            "    source_failovers "
            "FROM replication_runs "
            "WHERE run_id = $1::uuid",
            pqxx::params{
                run_id.str(),
            });

        if (!stored.has_value()) {
            return std::nullopt;
        }

        auto [stored_run_id, version_id, layout_id, replication_factor, state, chunks_scanned, chunks_under_replicated,
              replicas_verified, replicas_written, bytes_copied, source_failovers] = std::move(*stored);

        return reconstruct_replication_run_from_row(
            std::move(stored_run_id), std::move(version_id), std::move(layout_id), replication_factor, state,
            chunks_scanned, chunks_under_replicated, replicas_verified, replicas_written, bytes_copied,
            source_failovers, load_replication_run_placement_nodes(transaction, run_id.str()));
    }

    void register_object_in_transaction(pqxx::work& transaction, const Object& object) {
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
    }

    void register_object_layout_in_transaction(pqxx::work& transaction, const ObjectLayoutDescriptor& descriptor) {
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

        const std::optional<FastCdcParameters> fastcdc = descriptor.fastcdc_parameters();
        const std::optional<long long> fastcdc_min =
            fastcdc.has_value()
                ? std::optional<long long>{to_postgres_bigint(fastcdc->min_chunk_size_bytes, "FastCDC min chunk size")}
                : std::nullopt;
        const std::optional<long long> fastcdc_avg =
            fastcdc.has_value()
                ? std::optional<long long>{to_postgres_bigint(fastcdc->avg_chunk_size_bytes, "FastCDC avg chunk size")}
                : std::nullopt;
        const std::optional<long long> fastcdc_max =
            fastcdc.has_value()
                ? std::optional<long long>{to_postgres_bigint(fastcdc->max_chunk_size_bytes, "FastCDC max chunk size")}
                : std::nullopt;

        const pqxx::result layout_insert = transaction.exec(
            "INSERT INTO object_layouts ("
            "    layout_id, "
            "    object_id, "
            "    descriptor_version, "
            "    chunking_strategy, "
            "    fastcdc_min_chunk_size_bytes, "
            "    fastcdc_avg_chunk_size_bytes, "
            "    fastcdc_max_chunk_size_bytes, "
            "    canonical_descriptor, "
            "    total_size_bytes, "
            "    chunk_count"
            ") "
            "VALUES ("
            "    $1, "
            "    $2, "
            "    $3, "
            "    $4, "
            "    $5, "
            "    $6, "
            "    $7, "
            "    $8::bytea, "
            "    $9, "
            "    $10"
            ") "
            "ON CONFLICT (layout_id) "
            "DO NOTHING",
            pqxx::params{
                descriptor.layout_id(),
                descriptor.object_id(),
                static_cast<int>(ObjectLayoutDescriptor::kFormatVersion),
                chunking_strategy_to_string(descriptor.chunking_strategy()),
                fastcdc_min,
                fastcdc_avg,
                fastcdc_max,
                descriptor.canonical_bytes(),
                total_size,
                chunk_count,
            });

        if (layout_insert.affected_rows() == 1U) {
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
    }

    [[nodiscard]] bool artifact_version_identity_matches(pqxx::work& transaction, const ArtifactVersion& expected) {
        auto stored =
            transaction.query01<std::string, std::string, std::string, std::optional<std::string>, int, std::string>(
                "SELECT "
                "    version_id, "
                "    artifact_id::text, "
                "    root_object_id, "
                "    parent_version_id, "
                "    descriptor_version, "
                "    encode("
                "        canonical_descriptor, "
                "        'hex'"
                "    ) "
                "FROM artifact_versions "
                "WHERE version_id = $1",
                pqxx::params{
                    expected.version_id(),
                });

        if (!stored.has_value()) {
            return false;
        }

        const auto& [stored_version_id, stored_artifact_id, stored_root_object_id, stored_parent_version_id,
                     descriptor_version, stored_descriptor_hex] = *stored;

        if (stored_version_id != expected.version_id() || stored_artifact_id != expected.artifact_id().str() ||
            stored_root_object_id != expected.root_object_id() ||
            stored_parent_version_id != expected.parent_version_id() ||
            std::cmp_not_equal(descriptor_version, ArtifactVersion::kFormatVersion) ||
            stored_descriptor_hex != bytes_to_hex(std::span<const std::byte>{
                                         expected.canonical_bytes(),
                                     })) {
            return false;
        }

        ArtifactVersion::ImmutableMetadata stored_metadata;

        for (auto [key, value] : transaction.query<std::string, std::string>("SELECT "
                                                                             "    metadata_key, "
                                                                             "    metadata_value "
                                                                             "FROM artifact_version_metadata "
                                                                             "WHERE version_id = $1 "
                                                                             "ORDER BY metadata_key",
                                                                             pqxx::params{
                                                                                 expected.version_id(),
                                                                             })) {
            stored_metadata.emplace(std::move(key), std::move(value));
        }

        return stored_metadata == expected.immutable_metadata();
    }

    [[nodiscard]] UploadSession reconstruct_upload_session(pqxx::work& transaction, StoredUploadSessionRow stored) {
        auto [stored_session_id, stored_artifact_id, target_node_id, stored_replication_factor, stored_strategy,
              stored_chunk_size, stored_fastcdc_min, stored_fastcdc_avg, stored_fastcdc_max, parent_version_id, state,
              finalized_version_id] = std::move(stored);

        if (stored_replication_factor < 1 || stored_replication_factor > 8) {
            throw std::runtime_error("stored upload session has invalid replication factor");
        }

        std::vector<std::string> placement_node_ids =
            load_upload_session_placement_nodes(transaction, stored_session_id);

        if (placement_node_ids.empty()) {
            throw std::runtime_error("stored upload session is missing placement nodes");
        }

        if (placement_node_ids.front() != target_node_id) {
            throw std::runtime_error("stored upload session target node does not match placement snapshot");
        }

        const auto replication_factor = static_cast<std::uint8_t>(stored_replication_factor);

        UploadSession::ImmutableMetadata immutable_metadata;

        for (auto [metadata_key, metadata_value] :
             transaction.query<std::string, std::string>("SELECT "
                                                         "    metadata_key, "
                                                         "    metadata_value "
                                                         "FROM upload_session_metadata "
                                                         "WHERE session_id = $1::uuid "
                                                         "ORDER BY metadata_key",
                                                         pqxx::params{
                                                             stored_session_id,
                                                         })) {
            immutable_metadata.emplace(std::move(metadata_key), std::move(metadata_value));
        }

        const ChunkingStrategy strategy = chunking_strategy_from_string(stored_strategy);
        const UploadSessionState session_state = upload_session_state_from_string(state);

        if (strategy == ChunkingStrategy::FixedSize) {
            if (!stored_chunk_size.has_value() || *stored_chunk_size <= 0 || stored_fastcdc_min.has_value() ||
                stored_fastcdc_avg.has_value() || stored_fastcdc_max.has_value()) {
                throw std::runtime_error("stored upload session has invalid FixedSize configuration");
            }

            return UploadSession{
                UuidV7{std::move(stored_session_id)},
                UuidV7{std::move(stored_artifact_id)},
                replication_factor,
                std::move(placement_node_ids),
                strategy,
                static_cast<std::uint64_t>(*stored_chunk_size),
                std::move(parent_version_id),
                std::move(immutable_metadata),
                session_state,
                std::move(finalized_version_id),
            };
        }

        if (stored_chunk_size.has_value() || !stored_fastcdc_min.has_value() || !stored_fastcdc_avg.has_value() ||
            !stored_fastcdc_max.has_value()) {
            throw std::runtime_error("stored upload session has invalid FastCDC configuration");
        }

        const FastCdcParameters fastcdc_parameters{
            .min_chunk_size_bytes = static_cast<std::uint64_t>(*stored_fastcdc_min),
            .avg_chunk_size_bytes = static_cast<std::uint64_t>(*stored_fastcdc_avg),
            .max_chunk_size_bytes = static_cast<std::uint64_t>(*stored_fastcdc_max),
        };

        return UploadSession{
            UuidV7{std::move(stored_session_id)},
            UuidV7{std::move(stored_artifact_id)},
            replication_factor,
            std::move(placement_node_ids),
            fastcdc_parameters,
            std::move(parent_version_id),
            std::move(immutable_metadata),
            session_state,
            std::move(finalized_version_id),
        };
    }

    [[nodiscard]] UploadSession load_upload_session(pqxx::work& transaction, const UuidV7& session_id) {
        auto stored =
            transaction.query01<std::string, std::string, std::string, int, std::string, std::optional<long long>,
                                std::optional<long long>, std::optional<long long>, std::optional<long long>,
                                std::optional<std::string>, std::string, std::optional<std::string>>(
                "SELECT "
                "    session_id::text, "
                "    artifact_id::text, "
                "    target_node_id, "
                "    replication_factor, "
                "    chunking_strategy, "
                "    chunk_size_bytes, "
                "    fastcdc_min_chunk_size_bytes, "
                "    fastcdc_avg_chunk_size_bytes, "
                "    fastcdc_max_chunk_size_bytes, "
                "    parent_version_id, "
                "    state, "
                "    finalized_version_id "
                "FROM upload_sessions "
                "WHERE session_id = $1::uuid",
                pqxx::params{
                    session_id.str(),
                });

        if (!stored.has_value()) {
            throw std::runtime_error("upload session disappeared during transaction");
        }

        return reconstruct_upload_session(transaction, std::move(*stored));
    }

    [[nodiscard]] std::optional<ObjectLayoutDescriptor> load_object_layout(pqxx::work& transaction,
                                                                           std::string_view layout_id) {
        validate_layout_id(layout_id);

        auto stored =
            transaction.query01<std::string, int, std::string, std::optional<long long>, std::optional<long long>,
                                std::optional<long long>, std::string, long long, long long>(
                "SELECT "
                "    object_id, "
                "    descriptor_version, "
                "    chunking_strategy, "
                "    fastcdc_min_chunk_size_bytes, "
                "    fastcdc_avg_chunk_size_bytes, "
                "    fastcdc_max_chunk_size_bytes, "
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

        auto [stored_object_id, descriptor_version, stored_strategy, stored_fastcdc_min, stored_fastcdc_avg,
              stored_fastcdc_max, stored_descriptor_hex, stored_total_size, stored_chunk_count] = std::move(*stored);

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

        const ChunkingStrategy strategy = chunking_strategy_from_string(stored_strategy);
        ObjectLayoutDescriptor descriptor = [&]() {
            Object object{
                stored_object_id,
                static_cast<std::uint64_t>(stored_total_size),
            };
            ObjectLayout layout{
                std::move(chunks),
            };

            if (strategy == ChunkingStrategy::FixedSize) {
                if (stored_fastcdc_min.has_value() || stored_fastcdc_avg.has_value() ||
                    stored_fastcdc_max.has_value()) {
                    throw std::runtime_error("stored object layout has invalid FixedSize FastCDC columns");
                }

                return ObjectLayoutDescriptor{std::move(object), strategy, std::move(layout)};
            }

            if (!stored_fastcdc_min.has_value() || !stored_fastcdc_avg.has_value() || !stored_fastcdc_max.has_value()) {
                throw std::runtime_error("stored object layout has invalid FastCDC configuration");
            }

            const FastCdcParameters fastcdc_parameters{
                .min_chunk_size_bytes = static_cast<std::uint64_t>(*stored_fastcdc_min),
                .avg_chunk_size_bytes = static_cast<std::uint64_t>(*stored_fastcdc_avg),
                .max_chunk_size_bytes = static_cast<std::uint64_t>(*stored_fastcdc_max),
            };

            return ObjectLayoutDescriptor{std::move(object), fastcdc_parameters, std::move(layout)};
        }();

        if (descriptor.layout_id() != layout_id) {
            throw std::runtime_error("stored object layout does not match layout ID");
        }

        const std::string reconstructed_descriptor_hex = bytes_to_hex(std::span<const std::byte>{
            descriptor.canonical_bytes(),
        });

        if (reconstructed_descriptor_hex != stored_descriptor_hex) {
            throw std::runtime_error("stored canonical descriptor does not match object layout");
        }

        if (!stored_fastcdc_columns_match(stored_fastcdc_min, stored_fastcdc_avg, stored_fastcdc_max, descriptor)) {
            throw std::runtime_error("stored object layout FastCDC columns do not match descriptor");
        }

        return descriptor;
    }

    [[nodiscard]] std::optional<GcRun> load_gc_run(pqxx::work& transaction, const UuidV7& run_id) {
        auto stored =
            transaction.query01<std::string, std::string, std::string, std::string, long long, long long, long long,
                                long long, long long, long long, long long, long long, long long, long long>(
                "SELECT "
                "    run_id::text, "
                "    target_node_id, "
                "    mode, "
                "    state, "
                "    physical_chunks_scanned, "
                "    physical_bytes_scanned, "
                "    collectible_chunks, "
                "    collectible_bytes, "
                "    physically_deleted_chunks, "
                "    physically_deleted_bytes, "
                "    storage_locations_swept, "
                "    chunk_rows_swept, "
                "    object_layouts_swept, "
                "    objects_swept "
                "FROM gc_runs "
                "WHERE run_id = $1::uuid",
                pqxx::params{
                    run_id.str(),
                });

        if (!stored.has_value()) {
            return std::nullopt;
        }

        auto [stored_run_id, target_node_id, mode, state, physical_chunks_scanned, physical_bytes_scanned,
              collectible_chunks, collectible_bytes, physically_deleted_chunks, physically_deleted_bytes,
              storage_locations_swept, chunk_rows_swept, object_layouts_swept, objects_swept] = std::move(*stored);

        return reconstruct_gc_run_from_row(
            std::move(stored_run_id), std::move(target_node_id), mode, state, physical_chunks_scanned,
            physical_bytes_scanned, collectible_chunks, collectible_bytes, physically_deleted_chunks,
            physically_deleted_bytes, storage_locations_swept, chunk_rows_swept, object_layouts_swept, objects_swept);
    }

    [[nodiscard]] GcMetadataStats count_gc_metadata_sweep_candidates(pqxx::work& transaction,
                                                                     std::string_view target_node_id) {
        const auto storage_locations_swept = transaction.query_value<long long>(
            std::string{
                "SELECT COUNT(*) "
                "FROM storage_locations sl "
                "WHERE sl.node_id = $1 "
                "  AND NOT EXISTS ("
                "      SELECT 1 "
                "      FROM object_layout_chunks olc "
                "      INNER JOIN object_layouts ol ON ol.layout_id = olc.layout_id "
                "      WHERE olc.chunk_id = sl.chunk_id "
                "        AND ",
            } + std::string{kLiveLayoutPredicate} +
                "  )",
            pqxx::params{
                target_node_id,
            });

        const auto object_layouts_swept = transaction.query_value<long long>(std::string{
                                                                                 "SELECT COUNT(*) "
                                                                                 "FROM object_layouts ol "
                                                                                 "WHERE NOT ",
                                                                             } +
                                                                             std::string{kLiveLayoutPredicate});

        const auto objects_swept = transaction.query_value<long long>(
            "SELECT COUNT(*) "
            "FROM objects o "
            "WHERE NOT EXISTS ("
            "    SELECT 1 "
            "    FROM artifact_versions av "
            "    WHERE av.root_object_id = o.object_id"
            ") "
            "  AND NOT EXISTS ("
            "    SELECT 1 "
            "    FROM object_layouts ol "
            "    WHERE ol.object_id = o.object_id"
            ")");

        const auto chunk_rows_swept = transaction.query_value<long long>(
            "SELECT COUNT(*) "
            "FROM chunks c "
            "WHERE NOT EXISTS ("
            "    SELECT 1 "
            "    FROM object_layout_chunks olc "
            "    WHERE olc.chunk_id = c.chunk_id"
            ") "
            "  AND NOT EXISTS ("
            "    SELECT 1 "
            "    FROM storage_locations sl "
            "    WHERE sl.chunk_id = c.chunk_id"
            ")");

        auto require_non_negative = [](long long value, std::string_view field_name) -> std::uint64_t {
            if (value < 0) {
                throw std::runtime_error(std::string{field_name} + " count is negative");
            }

            return static_cast<std::uint64_t>(value);
        };

        return GcMetadataStats{
            .storage_locations_swept = require_non_negative(storage_locations_swept, "storage_locations_swept"),
            .chunk_rows_swept = require_non_negative(chunk_rows_swept, "chunk_rows_swept"),
            .object_layouts_swept = require_non_negative(object_layouts_swept, "object_layouts_swept"),
            .objects_swept = require_non_negative(objects_swept, "objects_swept"),
        };
    }

    [[nodiscard]] GcMetadataStats sweep_gc_metadata(pqxx::work& transaction, std::string_view target_node_id) {
        const pqxx::result storage_location_delete = transaction.exec(
            std::string{
                "DELETE FROM storage_locations sl "
                "WHERE sl.node_id = $1 "
                "  AND NOT EXISTS ("
                "      SELECT 1 "
                "      FROM object_layout_chunks olc "
                "      INNER JOIN object_layouts ol ON ol.layout_id = olc.layout_id "
                "      WHERE olc.chunk_id = sl.chunk_id "
                "        AND ",
            } + std::string{kLiveLayoutPredicate} +
                "  )",
            pqxx::params{
                target_node_id,
            });

        const pqxx::result object_layout_delete = transaction.exec(std::string{
                                                                       "DELETE FROM object_layouts ol "
                                                                       "WHERE NOT ",
                                                                   } +
                                                                   std::string{kLiveLayoutPredicate});

        const pqxx::result object_delete = transaction.exec(
            "DELETE FROM objects o "
            "WHERE NOT EXISTS ("
            "    SELECT 1 "
            "    FROM artifact_versions av "
            "    WHERE av.root_object_id = o.object_id"
            ") "
            "  AND NOT EXISTS ("
            "    SELECT 1 "
            "    FROM object_layouts ol "
            "    WHERE ol.object_id = o.object_id"
            ")");

        const pqxx::result chunk_delete = transaction.exec(
            "DELETE FROM chunks c "
            "WHERE NOT EXISTS ("
            "    SELECT 1 "
            "    FROM object_layout_chunks olc "
            "    WHERE olc.chunk_id = c.chunk_id"
            ") "
            "  AND NOT EXISTS ("
            "    SELECT 1 "
            "    FROM storage_locations sl "
            "    WHERE sl.chunk_id = c.chunk_id"
            ")");

        return GcMetadataStats{
            .storage_locations_swept = static_cast<std::uint64_t>(storage_location_delete.affected_rows()),
            .object_layouts_swept = static_cast<std::uint64_t>(object_layout_delete.affected_rows()),
            .objects_swept = static_cast<std::uint64_t>(object_delete.affected_rows()),
            .chunk_rows_swept = static_cast<std::uint64_t>(chunk_delete.affected_rows()),
        };
    }

    void verify_registered_object_layout(pqxx::work& transaction, const ObjectLayoutDescriptor& descriptor) {
        const ObjectLayout& layout = descriptor.layout();

        auto stored =
            transaction.query01<std::string, int, std::string, std::optional<long long>, std::optional<long long>,
                                std::optional<long long>, std::string, long long, long long>(
                "SELECT "
                "    object_id, "
                "    descriptor_version, "
                "    chunking_strategy, "
                "    fastcdc_min_chunk_size_bytes, "
                "    fastcdc_avg_chunk_size_bytes, "
                "    fastcdc_max_chunk_size_bytes, "
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

        const auto& [stored_object_id, descriptor_version, stored_strategy, stored_fastcdc_min, stored_fastcdc_avg,
                     stored_fastcdc_max, stored_descriptor_hex, stored_total_size, stored_chunk_count] = *stored;

        if (stored_object_id != descriptor.object_id() ||
            std::cmp_not_equal(descriptor_version, ObjectLayoutDescriptor::kFormatVersion) ||
            stored_strategy != chunking_strategy_to_string(descriptor.chunking_strategy()) ||
            !stored_fastcdc_columns_match(stored_fastcdc_min, stored_fastcdc_avg, stored_fastcdc_max, descriptor) ||
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

    [[nodiscard]] std::optional<LifecyclePolicy> load_lifecycle_policy(pqxx::work& transaction,
                                                                       const UuidV7& policy_id) {
        auto stored = transaction.query01<std::string, std::string>(
            "SELECT "
            "    policy_id::text, "
            "    name "
            "FROM lifecycle_policies "
            "WHERE policy_id = $1::uuid",
            pqxx::params{
                policy_id.str(),
            });

        if (!stored.has_value()) {
            return std::nullopt;
        }

        auto [stored_policy_id, name] = std::move(*stored);

        LifecyclePolicy policy{
            .policy_id =
                UuidV7{
                    std::move(stored_policy_id),
                },
            .name = std::move(name),
            .rules = {},
        };

        for (auto [artifact_kind, keep_last_n, max_age_seconds] :
             transaction.query<std::string, int, std::optional<long long>>("SELECT "
                                                                           "    artifact_kind, "
                                                                           "    keep_last_n, "
                                                                           "    max_age_seconds "
                                                                           "FROM lifecycle_policy_rules "
                                                                           "WHERE policy_id = $1::uuid "
                                                                           "ORDER BY artifact_kind",
                                                                           pqxx::params{
                                                                               policy_id.str(),
                                                                           })) {
            if (keep_last_n < 0) {
                throw std::runtime_error("stored lifecycle policy rule has negative keep_last_n");
            }

            if (max_age_seconds.has_value() && *max_age_seconds < 0) {
                throw std::runtime_error("stored lifecycle policy rule has negative max_age_seconds");
            }

            const ArtifactKind kind = artifact_kind_from_string(artifact_kind);

            policy.rules.emplace(
                kind,
                LifecycleRule{
                    .artifact_kind = kind,
                    .keep_last_n = static_cast<std::uint32_t>(keep_last_n),
                    .max_age_seconds = max_age_seconds.has_value()
                                           ? std::optional<std::uint64_t>{static_cast<std::uint64_t>(*max_age_seconds)}
                                           : std::nullopt,
                });
        }

        return policy;
    }

    [[nodiscard]] std::optional<LifecycleRun> load_lifecycle_run(pqxx::work& transaction, const UuidV7& run_id) {
        auto stored = transaction.query01<std::string, std::string, std::string, long long, long long, long long,
                                          long long, long long, long long, long long, long long>(
            "SELECT "
            "    run_id::text, "
            "    policy_id::text, "
            "    mode, "
            "    (EXTRACT(EPOCH FROM evaluated_at) * 1000)::bigint, "
            "    versions_scanned, "
            "    versions_protected, "
            "    versions_retained_by_policy, "
            "    versions_candidates, "
            "    versions_retired, "
            "    logical_bytes_candidates, "
            "    logical_bytes_retired "
            "FROM lifecycle_runs "
            "WHERE run_id = $1::uuid",
            pqxx::params{
                run_id.str(),
            });

        if (!stored.has_value()) {
            return std::nullopt;
        }

        auto [stored_run_id, stored_policy_id, mode, evaluated_at_ms, versions_scanned, versions_protected,
              versions_retained_by_policy, versions_candidates, versions_retired, logical_bytes_candidates,
              logical_bytes_retired] = std::move(*stored);

        auto require_non_negative = [](long long value, std::string_view field_name) -> std::uint64_t {
            if (value < 0) {
                throw std::runtime_error(std::string{field_name} + " is negative in stored lifecycle run");
            }

            return static_cast<std::uint64_t>(value);
        };

        return LifecycleRun{
            .run_id =
                UuidV7{
                    std::move(stored_run_id),
                },
            .policy_id =
                UuidV7{
                    std::move(stored_policy_id),
                },
            .mode = lifecycle_run_mode_from_string(mode),
            .evaluated_at_unix_ms = evaluated_at_ms,
            .stats =
                LifecycleStats{
                    .versions_scanned = require_non_negative(versions_scanned, "versions_scanned"),
                    .versions_protected = require_non_negative(versions_protected, "versions_protected"),
                    .versions_retained_by_policy =
                        require_non_negative(versions_retained_by_policy, "versions_retained_by_policy"),
                    .versions_candidates = require_non_negative(versions_candidates, "versions_candidates"),
                    .versions_retired = require_non_negative(versions_retired, "versions_retired"),
                    .logical_bytes_candidates =
                        require_non_negative(logical_bytes_candidates, "logical_bytes_candidates"),
                    .logical_bytes_retired = require_non_negative(logical_bytes_retired, "logical_bytes_retired"),
                },
        };
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

void PostgresMetadataRepository::register_chunks(const std::vector<ChunkMetadata>& chunks) {
    impl_->register_chunks(chunks);
}

std::optional<std::uint64_t> PostgresMetadataRepository::get_chunk_size(std::string_view chunk_id) {
    return impl_->get_chunk_size(chunk_id);
}

ChunkNegotiationBatch PostgresMetadataRepository::negotiate_chunks(const std::vector<ChunkMetadata>& chunks) {
    return impl_->negotiate_chunks(chunks);
}

void PostgresMetadataRepository::create_upload_session(const UploadSession& session) {
    impl_->create_upload_session(session);
}

std::optional<UploadSession> PostgresMetadataRepository::get_upload_session(const UuidV7& session_id) {
    return impl_->get_upload_session(session_id);
}

void PostgresMetadataRepository::abort_upload_session(const UuidV7& session_id) {
    impl_->abort_upload_session(session_id);
}

FinalizeUploadResult PostgresMetadataRepository::finalize_upload(const UuidV7& session_id,
                                                                 const ObjectLayoutDescriptor& descriptor) {
    return impl_->finalize_upload(session_id, descriptor);
}

RestorePlan PostgresMetadataRepository::resolve_restore_plan(std::string_view version_id,
                                                             std::string_view source_node_id) {
    return impl_->resolve_restore_plan(version_id, source_node_id);
}

MultiNodeRestorePlan PostgresMetadataRepository::resolve_multi_node_restore_plan(std::string_view version_id) {
    return impl_->resolve_multi_node_restore_plan(version_id);
}

void PostgresMetadataRepository::register_storage_node(const StorageNode& node) { impl_->register_storage_node(node); }

std::optional<StorageNode> PostgresMetadataRepository::get_storage_node(std::string_view node_id) {
    return impl_->get_storage_node(node_id);
}

std::vector<StorageNode> PostgresMetadataRepository::list_storage_nodes() { return impl_->list_storage_nodes(); }

ReplicationRun PostgresMetadataRepository::start_replication_run(const UuidV7& run_id, std::string_view version_id,
                                                                 std::uint8_t replication_factor) {
    return impl_->start_replication_run(run_id, version_id, replication_factor);
}

std::optional<ReplicationRun> PostgresMetadataRepository::get_replication_run(const UuidV7& run_id) {
    return impl_->get_replication_run(run_id);
}

ReplicationPlan PostgresMetadataRepository::get_replication_plan(const UuidV7& run_id) {
    return impl_->get_replication_plan(run_id);
}

ReplicationRun PostgresMetadataRepository::complete_replication_run(const UuidV7& run_id,
                                                                    const ReplicationStats& stats) {
    return impl_->complete_replication_run(run_id, stats);
}

GcRun PostgresMetadataRepository::start_gc_run(const GcRun& requested_run) {
    return impl_->start_gc_run(requested_run);
}

std::optional<GcRun> PostgresMetadataRepository::get_gc_run(const UuidV7& run_id) { return impl_->get_gc_run(run_id); }

std::vector<GcChunkDecision> PostgresMetadataRepository::classify_gc_chunks(const UuidV7& run_id,
                                                                            const std::vector<std::string>& chunk_ids) {
    return impl_->classify_gc_chunks(run_id, chunk_ids);
}

GcRun PostgresMetadataRepository::complete_gc_run(const UuidV7& run_id, const GcPhysicalStats& physical_stats) {
    return impl_->complete_gc_run(run_id, physical_stats);
}

void PostgresMetadataRepository::register_lifecycle_policy(const LifecyclePolicy& policy) {
    impl_->register_lifecycle_policy(policy);
}

std::optional<LifecyclePolicy> PostgresMetadataRepository::get_lifecycle_policy(const UuidV7& policy_id) {
    return impl_->get_lifecycle_policy(policy_id);
}

void PostgresMetadataRepository::pin_version(std::string_view version_id, std::string_view reason) {
    impl_->pin_version(version_id, reason);
}

bool PostgresMetadataRepository::unpin_version(std::string_view version_id) { return impl_->unpin_version(version_id); }

std::optional<std::string> PostgresMetadataRepository::get_version_pin(std::string_view version_id) {
    return impl_->get_version_pin(version_id);
}

LifecycleRun PostgresMetadataRepository::run_lifecycle(const UuidV7& run_id, const UuidV7& policy_id,
                                                       LifecycleRunMode mode) {
    return impl_->run_lifecycle(run_id, policy_id, mode);
}

std::optional<LifecycleRun> PostgresMetadataRepository::get_lifecycle_run(const UuidV7& run_id) {
    return impl_->get_lifecycle_run(run_id);
}

std::vector<LifecycleDecision> PostgresMetadataRepository::list_lifecycle_decisions(
    const UuidV7& run_id, std::optional<std::string_view> after_version_id, std::size_t limit) {
    return impl_->list_lifecycle_decisions(run_id, after_version_id, limit);
}

}  // namespace aistore::metadata
