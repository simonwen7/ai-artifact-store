#include "aistore/metadata/postgres_metadata_repository.hpp"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <pqxx/pqxx>
#include <stdexcept>
#include <string>
#include <vector>

#include "aistore/metadata/finalize_upload.hpp"
#include "aistore/metadata/gc.hpp"
#include "aistore/metadata/lifecycle.hpp"
#include "aistore/metadata/placement.hpp"
#include "aistore/metadata/replication.hpp"
#include "aistore/metadata/restore_plan.hpp"
#include "aistore/metadata/storage_node.hpp"

namespace {

using aistore::metadata::Artifact;
using aistore::metadata::ArtifactKind;
using aistore::metadata::ArtifactVersion;
using aistore::metadata::ChunkingStrategy;
using aistore::metadata::ChunkMetadata;
using aistore::metadata::ChunkRef;
using aistore::metadata::FastCdcParameters;
using aistore::metadata::FinalizeUploadError;
using aistore::metadata::FinalizeUploadErrorKind;
using aistore::metadata::GcError;
using aistore::metadata::GcErrorKind;
using aistore::metadata::GcPhysicalStats;
using aistore::metadata::GcRun;
using aistore::metadata::GcRunMode;
using aistore::metadata::GcRunState;
using aistore::metadata::kArtifactKindMetadataKey;
using aistore::metadata::LifecycleDecision;
using aistore::metadata::LifecycleDecisionReason;
using aistore::metadata::LifecycleError;
using aistore::metadata::LifecycleErrorKind;
using aistore::metadata::LifecyclePolicy;
using aistore::metadata::LifecycleRule;
using aistore::metadata::LifecycleRun;
using aistore::metadata::LifecycleRunMode;
using aistore::metadata::Manifest;
using aistore::metadata::MultiNodeRestorePlan;
using aistore::metadata::Object;
using aistore::metadata::ObjectLayout;
using aistore::metadata::ObjectLayoutDescriptor;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::ReplicationError;
using aistore::metadata::ReplicationErrorKind;
using aistore::metadata::ReplicationRunState;
using aistore::metadata::ReplicationStats;
using aistore::metadata::RestorePlan;
using aistore::metadata::RestorePlanError;
using aistore::metadata::RestorePlanErrorKind;
using aistore::metadata::select_replica_nodes;
using aistore::metadata::StorageLocation;
using aistore::metadata::StorageLocationState;
using aistore::metadata::StorageNode;
using aistore::metadata::StorageNodeState;
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

void ensure_replication_migration_applied() {
    pqxx::connection connection{
        test_database_connection_string(),
    };

    {
        pqxx::nontransaction check{connection};

        const bool migration_applied = check.query_value<bool>(
            "SELECT EXISTS ("
            "    SELECT 1 "
            "    FROM schema_migrations "
            "    WHERE version = 9 "
            "      AND name = 'multi_node_replication'"
            ")");

        if (migration_applied) {
            return;
        }
    }

    const std::filesystem::path migration_path =
        std::filesystem::path{__FILE__}.parent_path().parent_path().parent_path() / "migrations" /
        "009_multi_node_replication.sql";

    std::ifstream migration_file{migration_path};

    if (!migration_file.is_open()) {
        throw std::runtime_error("failed to open migration 009 for replication tests");
    }

    std::string migration_sql{
        std::istreambuf_iterator<char>{migration_file},
        std::istreambuf_iterator<char>{},
    };

    pqxx::nontransaction apply{connection};
    apply.exec(migration_sql);
}

void ensure_gc_migration_applied() {
    pqxx::connection connection{
        test_database_connection_string(),
    };

    {
        pqxx::nontransaction check{connection};

        const bool migration_applied = check.query_value<bool>(
            "SELECT EXISTS ("
            "    SELECT 1 "
            "    FROM schema_migrations "
            "    WHERE version = 8 "
            "      AND name = 'garbage_collection'"
            ")");

        if (migration_applied) {
            return;
        }
    }

    const std::filesystem::path migration_path =
        std::filesystem::path{__FILE__}.parent_path().parent_path().parent_path() / "migrations" /
        "008_garbage_collection.sql";

    std::ifstream migration_file{migration_path};

    if (!migration_file.is_open()) {
        throw std::runtime_error("failed to open migration 008 for garbage collection tests");
    }

    std::string migration_sql{
        std::istreambuf_iterator<char>{migration_file},
        std::istreambuf_iterator<char>{},
    };

    pqxx::nontransaction apply{connection};
    apply.exec(migration_sql);
}

void ensure_lifecycle_migration_applied() {
    pqxx::connection connection{
        test_database_connection_string(),
    };

    auto apply_migration_if_missing = [&](int version, std::string_view name, std::string_view filename) {
        {
            pqxx::nontransaction check{connection};

            const bool migration_applied = check.query_value<bool>(
                "SELECT EXISTS ("
                "    SELECT 1 "
                "    FROM schema_migrations "
                "    WHERE version = $1 "
                "      AND name = $2"
                ")",
                pqxx::params{
                    version,
                    std::string{name},
                });

            if (migration_applied) {
                return;
            }
        }

        const std::filesystem::path migration_path =
            std::filesystem::path{__FILE__}.parent_path().parent_path().parent_path() / "migrations" / filename;

        std::ifstream migration_file{migration_path};

        if (!migration_file.is_open()) {
            throw std::runtime_error(std::string{"failed to open migration "} + std::string{filename});
        }

        std::string migration_sql{
            std::istreambuf_iterator<char>{migration_file},
            std::istreambuf_iterator<char>{},
        };

        pqxx::nontransaction apply{connection};
        apply.exec(migration_sql);
    };

    apply_migration_if_missing(10, "ai_aware_lifecycle", "010_ai_aware_lifecycle.sql");
    apply_migration_if_missing(11, "retired_finalization_reclamation", "011_retired_finalization_reclamation.sql");
}

void reset_metadata_data() {
    ensure_gc_migration_applied();
    ensure_replication_migration_applied();
    ensure_lifecycle_migration_applied();

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
            "    upload_session_nodes, "
            "    upload_sessions, "
            "    replication_run_nodes, "
            "    replication_runs, "
            "    gc_runs, "
            "    lifecycle_run_decisions, "
            "    artifact_version_retirements, "
            "    lifecycle_runs, "
            "    lifecycle_policy_rules, "
            "    artifact_version_pins, "
            "    lifecycle_policies, "
            "    artifact_versions, "
            "    object_layout_chunks, "
            "    object_layouts, "
            "    artifacts, "
            "    storage_nodes, "
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

void register_storage_node(PostgresMetadataRepository& repository, std::string node_id, std::string address,
                           std::uint16_t port, StorageNodeState state = StorageNodeState::Active) {
    repository.register_storage_node(StorageNode{
        .node_id = std::move(node_id),
        .address = std::move(address),
        .port = port,
        .state = state,
    });
}

void ensure_active_placement_nodes(PostgresMetadataRepository& repository, const UploadSession& session) {
    for (const std::string& node_id : session.placement_node_ids()) {
        register_storage_node(repository, node_id, "127.0.0.1", 8081, StorageNodeState::Active);
    }
}

void create_verified_upload_session(PostgresMetadataRepository& repository, const UploadSession& session) {
    ensure_active_placement_nodes(repository, session);
    repository.create_upload_session(session);
}

UploadSession make_multi_node_finalize_session(const UuidV7& session_id, const UuidV7& artifact_id,
                                               std::uint8_t replication_factor,
                                               std::vector<std::string> placement_node_ids, std::string marker) {
    return UploadSession{
        session_id,
        artifact_id,
        replication_factor,
        std::move(placement_node_ids),
        ChunkingStrategy::FixedSize,
        4,
        std::nullopt,
        UploadSession::ImmutableMetadata{{"marker", std::move(marker)}},
        UploadSessionState::Open,
        std::nullopt,
    };
}

void register_chunk_on_node(PostgresMetadataRepository& repository, const ChunkRef& chunk, std::string_view node_id,
                            StorageLocationState state) {
    repository.register_chunks({ChunkMetadata{.chunk_id = chunk.chunk_id, .size_bytes = chunk.size}});
    repository.register_storage_location(StorageLocation{
        .chunk_id = chunk.chunk_id,
        .node_id = std::string{node_id},
        .storage_path = std::string{"/m8/"} + chunk.chunk_id + std::string{node_id},
        .state = state,
    });
}

ArtifactVersion register_replication_version(PostgresMetadataRepository& repository, const UuidV7& artifact_id,
                                             const ObjectLayoutDescriptor& descriptor, std::string marker) {
    repository.register_object(descriptor.object());
    repository.register_object_layout(descriptor);

    for (const std::string& node_id : {"m8-node-a", "m8-node-b", "m8-node-c"}) {
        register_finalize_chunks(repository, descriptor, node_id, StorageLocationState::Available);
    }

    ArtifactVersion version{
        artifact_id,
        descriptor.object_id(),
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{{"marker", marker}},
        VersionState::Committed,
    };
    repository.create_version(version);

    return version;
}

LifecyclePolicy make_uniform_lifecycle_policy(const UuidV7& policy_id, std::uint32_t keep_last_n,
                                              std::optional<std::uint64_t> max_age_seconds = std::nullopt) {
    LifecyclePolicy policy{
        .policy_id = policy_id,
        .name = "m9-lifecycle-policy",
        .rules = {},
    };

    for (const ArtifactKind kind : {ArtifactKind::Generic, ArtifactKind::ModelCheckpoint, ArtifactKind::DatasetSnapshot,
                                    ArtifactKind::EmbeddingIndex, ArtifactKind::EvaluationOutput}) {
        policy.rules.emplace(
            kind, LifecycleRule{.artifact_kind = kind, .keep_last_n = keep_last_n, .max_age_seconds = max_age_seconds});
    }

    return policy;
}

std::string make_lifecycle_hex_id(char marker, char role) {
    const unsigned value = (static_cast<unsigned char>(marker) << 8U) | static_cast<unsigned char>(role);

    constexpr char kHex[] = "0123456789abcdef";
    std::string id(62, '0');
    id.push_back(kHex[(value >> 4U) & 0x0fU]);
    id.push_back(kHex[value & 0x0fU]);

    return id;
}

ObjectLayoutDescriptor make_lifecycle_descriptor(char object_marker) {
    return ObjectLayoutDescriptor{
        Object{make_lifecycle_hex_id(object_marker, 'o'), 6},
        ChunkingStrategy::FixedSize,
        ObjectLayout{{
            ChunkRef{.chunk_id = make_lifecycle_hex_id(object_marker, 'a'), .offset = 0, .size = 4},
            ChunkRef{.chunk_id = make_lifecycle_hex_id(object_marker, 'b'), .offset = 4, .size = 2},
        }},
    };
}

ArtifactVersion create_committed_version_with_kind(PostgresMetadataRepository& repository, const UuidV7& artifact_id,
                                                   char object_marker, std::string_view kind_string,
                                                   std::optional<std::string> parent = std::nullopt) {
    const auto descriptor = make_lifecycle_descriptor(object_marker);

    repository.register_object(descriptor.object());
    repository.register_object_layout(descriptor);

    ArtifactVersion version{
        artifact_id,
        descriptor.object_id(),
        std::move(parent),
        ArtifactVersion::ImmutableMetadata{
            {"marker", std::string(1, object_marker)},
            {std::string{kArtifactKindMetadataKey}, std::string{kind_string}},
        },
        VersionState::Committed,
    };

    repository.create_version(version);

    return version;
}

void retire_version_via_lifecycle(PostgresMetadataRepository& repository, std::string_view version_id,
                                  const UuidV7& policy_id) {
    const UuidV7 run_id = UuidV7::generate();
    const LifecycleRun run = repository.run_lifecycle(run_id, policy_id, LifecycleRunMode::Apply);

    pqxx::connection connection{test_database_connection_string()};
    pqxx::work transaction{connection};

    const bool retired = transaction.query_value<bool>(
        "SELECT EXISTS ("
        "    SELECT 1 "
        "    FROM artifact_version_retirements "
        "    WHERE version_id = $1"
        ")",
        pqxx::params{
            version_id,
        });

    transaction.commit();

    if (!retired) {
        throw std::runtime_error("lifecycle apply did not retire version: " + std::string{version_id});
    }

    (void)run;
}

[[nodiscard]] const LifecycleDecision* find_lifecycle_decision(const std::vector<LifecycleDecision>& decisions,
                                                               std::string_view version_id) {
    for (const LifecycleDecision& decision : decisions) {
        if (decision.version_id == version_id) {
            return &decision;
        }
    }

    return nullptr;
}

void set_version_created_at(std::string_view version_id, const std::string& timestamp) {
    pqxx::connection connection{test_database_connection_string()};
    pqxx::work transaction{connection};

    transaction
        .exec(
            "UPDATE artifact_versions "
            "SET created_at = $2::timestamptz "
            "WHERE version_id = $1",
            pqxx::params{
                version_id,
                timestamp,
            })
        .no_rows();

    transaction.commit();
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

    create_verified_upload_session(repository, session);

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

    create_verified_upload_session(repository, session);
    create_verified_upload_session(repository, session);

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

    create_verified_upload_session(repository, original);

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

    EXPECT_THROW(create_verified_upload_session(repository, conflicting), std::runtime_error);

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

    create_verified_upload_session(repository, UploadSession{
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

    EXPECT_THROW(create_verified_upload_session(repository,
                                                UploadSession{
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

    EXPECT_THROW(create_verified_upload_session(repository,
                                                UploadSession{
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
    create_verified_upload_session(repository,
                                   make_finalize_session(session_id, artifact_id, "m4s5-target", marker.str()));
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
    create_verified_upload_session(repository,
                                   make_finalize_session(session_id, artifact_id, "m4s5-target", marker.str()));
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
    create_verified_upload_session(repository,
                                   make_finalize_session(session_id, artifact_id, "m4s5-target", marker.str()));
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
    create_verified_upload_session(repository,
                                   make_finalize_session(session_id, artifact_id, "m4s5-target", marker.str()));
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
    create_verified_upload_session(repository,
                                   make_finalize_session(session_id, artifact_id, "m4s5-target", marker.str()));
    register_finalize_chunks(repository, descriptor, "m4s5-target", StorageLocationState::Missing);

    try {
        (void)repository.finalize_upload(session_id, descriptor);
        FAIL() << "expected FinalizeUploadError";
    } catch (const FinalizeUploadError& error) {
        EXPECT_EQ(error.kind(), FinalizeUploadErrorKind::ChunkUnderReplicated);
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
    create_verified_upload_session(repository,
                                   make_finalize_session(session_id, artifact_id, "m4s5-target", marker.str()));
    register_finalize_chunks(repository, descriptor, "m4s5-other", StorageLocationState::Available);

    try {
        (void)repository.finalize_upload(session_id, descriptor);
        FAIL() << "expected FinalizeUploadError";
    } catch (const FinalizeUploadError& error) {
        EXPECT_EQ(error.kind(), FinalizeUploadErrorKind::ChunkUnderReplicated);
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
    create_verified_upload_session(repository, session);
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
    create_verified_upload_session(repository,
                                   make_finalize_session(session_id, artifact_id, "m4s5-target", marker.str()));

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
    create_verified_upload_session(repository,
                                   make_finalize_session(first_session_id, artifact_id, "m4s5-target", marker.str()));
    create_verified_upload_session(repository,
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

    create_verified_upload_session(repository, session);

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

    create_verified_upload_session(repository, original);

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

    EXPECT_THROW(create_verified_upload_session(repository, conflicting), std::runtime_error);
}

TEST(PostgresMetadataRepositoryTest, FinalizesFastCdcUpload) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const auto descriptor = make_fastcdc_finalize_descriptor('f', kTestFastCdcParameters, 'c', 'd');

    repository.create_artifact(Artifact{artifact_id, "fastcdc-finalize-" + marker.str(), "m6-fastcdc"});
    create_verified_upload_session(
        repository, make_fastcdc_finalize_session(session_id, artifact_id, "m6-fastcdc-target", marker.str(),
                                                  kTestFastCdcParameters));
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
    create_verified_upload_session(
        repository, make_fastcdc_finalize_session(session_id, artifact_id, "m6-fastcdc-target", marker.str(),
                                                  kTestFastCdcParameters));
    register_finalize_chunks(repository, descriptor, "m6-fastcdc-target", StorageLocationState::Available);

    EXPECT_THROW((void)repository.finalize_upload(session_id, descriptor), std::invalid_argument);
}

GcRun make_open_gc_run(const UuidV7& run_id, std::string target_node_id, GcRunMode mode = GcRunMode::Apply) {
    return GcRun{
        .run_id = run_id,
        .target_node_id = std::move(target_node_id),
        .mode = mode,
        .state = GcRunState::Open,
    };
}

void register_gc_chunk(PostgresMetadataRepository& repository, std::string_view chunk_id, std::string_view node_id) {
    if (!repository.get_chunk_size(chunk_id).has_value()) {
        repository.register_chunks({ChunkMetadata{.chunk_id = std::string{chunk_id}, .size_bytes = 3}});
    }

    repository.register_storage_location(StorageLocation{
        .chunk_id = std::string{chunk_id},
        .node_id = std::string{node_id},
        .storage_path = std::string{"/gc/"} + std::string{chunk_id},
        .state = StorageLocationState::Available,
    });
}

ObjectLayoutDescriptor make_gc_layout(char object_marker, char first_chunk_marker, char second_chunk_marker) {
    return ObjectLayoutDescriptor{
        Object{std::string(64, object_marker), 6},
        ChunkingStrategy::FixedSize,
        ObjectLayout{{
            ChunkRef{.chunk_id = std::string(64, first_chunk_marker), .offset = 0, .size = 3},
            ChunkRef{.chunk_id = std::string(64, second_chunk_marker), .offset = 3, .size = 3},
        }},
    };
}

const std::string kGcSharedChunk(64, 'a');
const std::string kGcLiveOnlyChunk(64, 'b');
const std::string kGcDeadOnlyChunk(64, 'c');
const std::string kGcUntrackedChunk(64, 'd');
const std::string kGcDryRunChunkA(64, 'e');
const std::string kGcDryRunChunkB(64, 'f');
const std::string kGcRetryChunk(64, '0');

void register_live_gc_layout(PostgresMetadataRepository& repository, const UuidV7& artifact_id,
                             const ObjectLayoutDescriptor& descriptor, std::string marker) {
    repository.create_artifact(Artifact{artifact_id, "gc-" + marker, "gc-tests"});
    repository.register_object(descriptor.object());
    repository.register_object_layout(descriptor);
    repository.create_version(ArtifactVersion{
        artifact_id,
        descriptor.object_id(),
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{{"marker", std::move(marker)}},
        VersionState::Committed,
    });
}

void register_dead_gc_layout(PostgresMetadataRepository& repository, const ObjectLayoutDescriptor& descriptor) {
    repository.register_object(descriptor.object());
    repository.register_object_layout(descriptor);
}

TEST(PostgresMetadataRepositoryTest, StartsGcOnlyWhenNoOpenUploadSessions) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const UuidV7 gc_run_id = UuidV7::generate();

    repository.create_artifact(Artifact{artifact_id, "gc-open-session-" + session_id.str(), "gc-tests"});
    create_verified_upload_session(repository, UploadSession{
                                                   session_id,
                                                   artifact_id,
                                                   "gc-node-a",
                                                   ChunkingStrategy::FixedSize,
                                                   4,
                                                   std::nullopt,
                                                   UploadSession::ImmutableMetadata{},
                                                   UploadSessionState::Open,
                                                   std::nullopt,
                                               });

    try {
        (void)repository.start_gc_run(make_open_gc_run(gc_run_id, "gc-node-a"));
        FAIL() << "expected GcError";
    } catch (const GcError& error) {
        EXPECT_EQ(error.kind(), GcErrorKind::OpenUploadSessionsPresent);
    }

    repository.abort_upload_session(session_id);

    const GcRun started = repository.start_gc_run(make_open_gc_run(gc_run_id, "gc-node-a"));
    EXPECT_EQ(started.run_id, gc_run_id);
    EXPECT_EQ(started.state, GcRunState::Open);
}

TEST(PostgresMetadataRepositoryTest, CreateUploadSessionIsRejectedDuringOpenGc) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const UuidV7 gc_run_id = UuidV7::generate();

    (void)repository.start_gc_run(make_open_gc_run(gc_run_id, "gc-node-a"));

    repository.create_artifact(Artifact{artifact_id, "gc-in-progress-" + session_id.str(), "gc-tests"});

    try {
        create_verified_upload_session(repository, UploadSession{
                                                       session_id,
                                                       artifact_id,
                                                       "gc-node-a",
                                                       ChunkingStrategy::FixedSize,
                                                       4,
                                                       std::nullopt,
                                                       UploadSession::ImmutableMetadata{},
                                                       UploadSessionState::Open,
                                                       std::nullopt,
                                                   });
        FAIL() << "expected GcError";
    } catch (const GcError& error) {
        EXPECT_EQ(error.kind(), GcErrorKind::GcInProgress);
    }
}

TEST(PostgresMetadataRepositoryTest, StartingSameGcRunIsIdempotent) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 gc_run_id = UuidV7::generate();

    const GcRun first = repository.start_gc_run(make_open_gc_run(gc_run_id, "gc-node-a", GcRunMode::DryRun));
    const GcRun second = repository.start_gc_run(make_open_gc_run(gc_run_id, "gc-node-a", GcRunMode::DryRun));

    EXPECT_EQ(first.run_id, second.run_id);
    EXPECT_EQ(first.target_node_id, second.target_node_id);
    EXPECT_EQ(first.mode, second.mode);
    EXPECT_EQ(first.state, second.state);
}

TEST(PostgresMetadataRepositoryTest, ClassifiesLiveSharedAndUntrackedPhysicalChunks) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 gc_run_id = UuidV7::generate();
    const std::string shared_chunk = kGcSharedChunk;
    const std::string live_only_chunk = kGcLiveOnlyChunk;
    const std::string untracked_chunk = kGcUntrackedChunk;
    const auto live_layout = make_gc_layout('1', 'a', 'b');
    const auto dead_layout = make_gc_layout('2', 'a', 'c');

    register_live_gc_layout(repository, artifact_id, live_layout, UuidV7::generate().str());
    register_dead_gc_layout(repository, dead_layout);

    (void)repository.start_gc_run(make_open_gc_run(gc_run_id, "gc-node-a"));

    const auto decisions = repository.classify_gc_chunks(gc_run_id, {live_only_chunk, shared_chunk, untracked_chunk});

    ASSERT_EQ(decisions.size(), 3U);
    EXPECT_EQ(decisions[0].chunk_id, live_only_chunk);
    EXPECT_FALSE(decisions[0].collectible);
    EXPECT_EQ(decisions[1].chunk_id, shared_chunk);
    EXPECT_FALSE(decisions[1].collectible);
    EXPECT_EQ(decisions[2].chunk_id, untracked_chunk);
    EXPECT_TRUE(decisions[2].collectible);
}

TEST(PostgresMetadataRepositoryTest, ClassifiesChunkReferencedOnlyByDeadLayoutAsCollectible) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 gc_run_id = UuidV7::generate();
    const std::string dead_only_chunk = kGcDeadOnlyChunk;
    const auto dead_layout = make_gc_layout('9', 'c', 'e');

    register_dead_gc_layout(repository, dead_layout);

    (void)repository.start_gc_run(make_open_gc_run(gc_run_id, "gc-node-a"));

    const auto decisions = repository.classify_gc_chunks(gc_run_id, {dead_only_chunk});

    ASSERT_EQ(decisions.size(), 1U);
    EXPECT_TRUE(decisions[0].collectible);
}

TEST(PostgresMetadataRepositoryTest, DryRunCompletionDoesNotSweepMetadata) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 gc_run_id = UuidV7::generate();
    const auto dead_layout = make_gc_layout('8', 'e', 'f');

    register_dead_gc_layout(repository, dead_layout);
    register_gc_chunk(repository, kGcDryRunChunkA, "gc-node-a");
    register_gc_chunk(repository, kGcDryRunChunkB, "gc-node-a");

    (void)repository.start_gc_run(make_open_gc_run(gc_run_id, "gc-node-a", GcRunMode::DryRun));

    const GcPhysicalStats physical_stats{
        .physical_chunks_scanned = 2,
        .physical_bytes_scanned = 8,
        .collectible_chunks = 2,
        .collectible_bytes = 8,
    };

    const GcRun completed = repository.complete_gc_run(gc_run_id, physical_stats);

    EXPECT_EQ(completed.state, GcRunState::Completed);
    EXPECT_EQ(completed.mode, GcRunMode::DryRun);
    EXPECT_GT(completed.metadata_stats.storage_locations_swept, 0U);
    EXPECT_GT(completed.metadata_stats.object_layouts_swept, 0U);
    EXPECT_TRUE(repository.get_object_layout(dead_layout.layout_id()).has_value());
    EXPECT_TRUE(repository.get_chunk_size(kGcDryRunChunkA).has_value());
}

TEST(PostgresMetadataRepositoryTest, ApplyCompletionSweepsDeadRepresentationsAndPreservesLiveData) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 gc_run_id = UuidV7::generate();
    const std::string shared_chunk = kGcSharedChunk;
    const std::string live_only_chunk = kGcLiveOnlyChunk;
    const std::string dead_only_chunk = kGcDeadOnlyChunk;
    const auto live_layout = make_gc_layout('1', 'a', 'b');
    const auto dead_layout = make_gc_layout('2', 'a', 'c');

    register_live_gc_layout(repository, artifact_id, live_layout, UuidV7::generate().str());
    register_dead_gc_layout(repository, dead_layout);
    register_gc_chunk(repository, shared_chunk, "gc-node-a");
    register_gc_chunk(repository, live_only_chunk, "gc-node-a");
    register_gc_chunk(repository, dead_only_chunk, "gc-node-a");

    (void)repository.start_gc_run(make_open_gc_run(gc_run_id, "gc-node-a"));

    const GcRun completed = repository.complete_gc_run(gc_run_id, GcPhysicalStats{});

    EXPECT_EQ(completed.state, GcRunState::Completed);
    EXPECT_EQ(completed.mode, GcRunMode::Apply);
    EXPECT_GE(completed.metadata_stats.storage_locations_swept, 1U);
    EXPECT_GE(completed.metadata_stats.object_layouts_swept, 1U);
    EXPECT_TRUE(repository.get_object_layout(live_layout.layout_id()).has_value());
    EXPECT_FALSE(repository.get_object_layout(dead_layout.layout_id()).has_value());
    EXPECT_TRUE(repository.get_chunk_size(shared_chunk).has_value());
    EXPECT_TRUE(repository.get_chunk_size(live_only_chunk).has_value());
    EXPECT_FALSE(repository.get_chunk_size(dead_only_chunk).has_value());

    const auto shared_locations = repository.get_storage_locations(shared_chunk);
    const auto dead_only_locations = repository.get_storage_locations(dead_only_chunk);

    ASSERT_EQ(shared_locations.size(), 1U);
    EXPECT_EQ(shared_locations.front().node_id, "gc-node-a");
    EXPECT_TRUE(dead_only_locations.empty());
}

TEST(PostgresMetadataRepositoryTest, CompletedGcRetryReturnsStoredResult) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 gc_run_id = UuidV7::generate();
    const auto dead_layout = make_gc_layout('7', '0', '1');

    register_dead_gc_layout(repository, dead_layout);
    register_gc_chunk(repository, kGcRetryChunk, "gc-node-a");

    (void)repository.start_gc_run(make_open_gc_run(gc_run_id, "gc-node-a"));

    const GcPhysicalStats physical_stats{
        .physical_chunks_scanned = 1,
        .physical_bytes_scanned = 4,
        .collectible_chunks = 1,
        .collectible_bytes = 4,
        .physically_deleted_chunks = 1,
        .physically_deleted_bytes = 4,
    };

    const GcRun first = repository.complete_gc_run(gc_run_id, physical_stats);
    const GcRun second = repository.complete_gc_run(gc_run_id, GcPhysicalStats{});

    EXPECT_EQ(first.run_id, second.run_id);
    EXPECT_EQ(first.state, GcRunState::Completed);
    EXPECT_EQ(second.state, GcRunState::Completed);
    EXPECT_EQ(first.physical_stats, second.physical_stats);
    EXPECT_EQ(first.metadata_stats, second.metadata_stats);
}

TEST(PostgresMetadataRepositoryTest, RegistersUpdatesAndListsStorageNodes) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};

    repository.register_storage_node(StorageNode{
        .node_id = "m8-node-a",
        .address = "127.0.0.1",
        .port = 9101,
        .state = StorageNodeState::Active,
    });
    repository.register_storage_node(StorageNode{
        .node_id = "m8-node-b",
        .address = "127.0.0.2",
        .port = 9102,
        .state = StorageNodeState::Draining,
    });

    const auto first = repository.get_storage_node("m8-node-a");
    ASSERT_TRUE(first.has_value());
    EXPECT_EQ(first->address, "127.0.0.1");
    EXPECT_EQ(first->port, 9101U);
    EXPECT_EQ(first->state, StorageNodeState::Active);

    repository.register_storage_node(StorageNode{
        .node_id = "m8-node-a",
        .address = "127.0.0.9",
        .port = 9199,
        .state = StorageNodeState::Disabled,
    });

    const auto updated = repository.get_storage_node("m8-node-a");
    ASSERT_TRUE(updated.has_value());
    EXPECT_EQ(updated->address, "127.0.0.9");
    EXPECT_EQ(updated->port, 9199U);
    EXPECT_EQ(updated->state, StorageNodeState::Disabled);

    const std::vector<StorageNode> nodes = repository.list_storage_nodes();
    ASSERT_EQ(nodes.size(), 2U);
    EXPECT_EQ(nodes[0].node_id, "m8-node-a");
    EXPECT_EQ(nodes[1].node_id, "m8-node-b");
}

TEST(PostgresMetadataRepositoryTest, PersistsMultiNodeUploadSessionSnapshot) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();

    repository.create_artifact(Artifact{artifact_id, "multi-node-session", "m8s5"});

    const UploadSession unknown_session = make_multi_node_finalize_session(
        UuidV7::generate(), artifact_id, 1U, std::vector<std::string>{"m8-unknown-node"}, "unknown");
    EXPECT_THROW(repository.create_upload_session(unknown_session), std::invalid_argument);

    register_storage_node(repository, "m8-node-drain", "127.0.0.1", 9201, StorageNodeState::Draining);
    const UploadSession draining_session = make_multi_node_finalize_session(
        UuidV7::generate(), artifact_id, 1U, std::vector<std::string>{"m8-node-drain"}, "drain");
    EXPECT_THROW(repository.create_upload_session(draining_session), std::invalid_argument);

    register_storage_node(repository, "m8-node-disabled", "127.0.0.1", 9202, StorageNodeState::Disabled);
    const UploadSession disabled_session = make_multi_node_finalize_session(
        UuidV7::generate(), artifact_id, 1U, std::vector<std::string>{"m8-node-disabled"}, "disabled");
    EXPECT_THROW(repository.create_upload_session(disabled_session), std::invalid_argument);

    const UploadSession session = make_multi_node_finalize_session(
        session_id, artifact_id, 3U, std::vector<std::string>{"m8-node-a", "m8-node-b", "m8-node-c"}, "snapshot");

    create_verified_upload_session(repository, session);

    const auto restored = repository.get_upload_session(session_id);
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->replication_factor(), 3U);
    EXPECT_EQ(restored->placement_node_ids(), (std::vector<std::string>{"m8-node-a", "m8-node-b", "m8-node-c"}));
    EXPECT_EQ(restored->target_node_id(), "m8-node-a");

    register_storage_node(repository, "m8-node-a", "127.0.0.1", 8081, StorageNodeState::Disabled);
    register_storage_node(repository, "m8-node-b", "127.0.0.1", 8081, StorageNodeState::Draining);
    create_verified_upload_session(repository, session);
    const auto resumed = repository.get_upload_session(session_id);
    ASSERT_TRUE(resumed.has_value());
    EXPECT_EQ(resumed->placement_node_ids(), (std::vector<std::string>{"m8-node-a", "m8-node-b", "m8-node-c"}));
    EXPECT_EQ(resumed->replication_factor(), 3U);
}

TEST(PostgresMetadataRepositoryTest, FinalizeRequiresEveryDesiredReplica) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const auto descriptor = make_finalize_descriptor('a', 'b', 'c');
    const std::vector<std::string> placement{"m8-node-a", "m8-node-b", "m8-node-c"};

    repository.create_artifact(Artifact{artifact_id, "desired-replica-" + marker.str(), "m8s5"});
    create_verified_upload_session(
        repository, make_multi_node_finalize_session(session_id, artifact_id, 2U, placement, marker.str()));

    for (const ChunkRef& chunk : descriptor.layout().chunks()) {
        const std::vector<std::string> desired = select_replica_nodes(chunk.chunk_id, placement, 2U);
        ASSERT_EQ(desired.size(), 2U);
        register_chunk_on_node(repository, chunk, desired.front(), StorageLocationState::Available);
    }

    try {
        (void)repository.finalize_upload(session_id, descriptor);
        FAIL() << "expected FinalizeUploadError";
    } catch (const FinalizeUploadError& error) {
        EXPECT_EQ(error.kind(), FinalizeUploadErrorKind::ChunkUnderReplicated);
        ASSERT_TRUE(error.chunk_id().has_value());
    }
}

TEST(PostgresMetadataRepositoryTest, FinalizeIgnoresExtraNonDesiredReplica) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const auto descriptor = make_finalize_descriptor('d', 'e', 'f');
    const std::vector<std::string> placement{"m8-node-a", "m8-node-b", "m8-node-c"};

    repository.create_artifact(Artifact{artifact_id, "extra-replica-" + marker.str(), "m8s5"});
    create_verified_upload_session(
        repository, make_multi_node_finalize_session(session_id, artifact_id, 2U, placement, marker.str()));

    for (const ChunkRef& chunk : descriptor.layout().chunks()) {
        const std::vector<std::string> desired = select_replica_nodes(chunk.chunk_id, placement, 2U);
        register_chunk_on_node(repository, chunk, desired.front(), StorageLocationState::Available);
        register_chunk_on_node(repository, chunk, "m8-node-extra", StorageLocationState::Available);
    }

    try {
        (void)repository.finalize_upload(session_id, descriptor);
        FAIL() << "expected FinalizeUploadError";
    } catch (const FinalizeUploadError& error) {
        EXPECT_EQ(error.kind(), FinalizeUploadErrorKind::ChunkUnderReplicated);
    }
}

TEST(PostgresMetadataRepositoryTest, StartsReplicationRunWithDeterministicSnapshot) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 run_id = UuidV7::generate();
    const auto descriptor = make_finalize_descriptor('1', '2', '3');

    register_storage_node(repository, "m8-node-a", "127.0.0.1", 9101);
    register_storage_node(repository, "m8-node-b", "127.0.0.2", 9102);
    register_storage_node(repository, "m8-node-c", "127.0.0.3", 9103);

    repository.create_artifact(Artifact{artifact_id, "replication-start-" + marker.str(), "m8s5"});
    const ArtifactVersion version = register_replication_version(repository, artifact_id, descriptor, marker.str());

    const auto run = repository.start_replication_run(run_id, version.version_id(), 2U);

    EXPECT_EQ(run.run_id, run_id);
    EXPECT_EQ(run.version_id, version.version_id());
    EXPECT_EQ(run.layout_id, descriptor.layout_id());
    EXPECT_EQ(run.replication_factor, 2U);
    EXPECT_EQ(run.state, ReplicationRunState::Open);
    EXPECT_EQ(run.placement_node_ids, (std::vector<std::string>{"m8-node-a", "m8-node-b", "m8-node-c"}));
}

TEST(PostgresMetadataRepositoryTest, ReplicationRunRetryIsIdempotent) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 run_id = UuidV7::generate();
    const auto descriptor = make_finalize_descriptor('4', '5', '6');

    register_storage_node(repository, "m8-node-a", "127.0.0.1", 9101);
    register_storage_node(repository, "m8-node-b", "127.0.0.2", 9102);
    register_storage_node(repository, "m8-node-c", "127.0.0.3", 9103);

    repository.create_artifact(Artifact{artifact_id, "replication-retry-" + marker.str(), "m8s5"});
    const ArtifactVersion version = register_replication_version(repository, artifact_id, descriptor, marker.str());

    const auto first = repository.start_replication_run(run_id, version.version_id(), 2U);
    const auto second = repository.start_replication_run(run_id, version.version_id(), 2U);

    EXPECT_EQ(first.run_id, second.run_id);
    EXPECT_EQ(first.version_id, second.version_id);
    EXPECT_EQ(first.layout_id, second.layout_id);
    EXPECT_EQ(first.replication_factor, second.replication_factor);
    EXPECT_EQ(first.placement_node_ids, second.placement_node_ids);
    EXPECT_EQ(first.state, second.state);
}

TEST(PostgresMetadataRepositoryTest, CompletesReplicationOnlyAfterDesiredLocationsExist) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 run_id = UuidV7::generate();
    const auto descriptor = make_finalize_descriptor('7', '8', '9');
    const std::vector<std::string> placement{"m8-node-a", "m8-node-b", "m8-node-c"};

    register_storage_node(repository, "m8-node-a", "127.0.0.1", 9101);
    register_storage_node(repository, "m8-node-b", "127.0.0.2", 9102);
    register_storage_node(repository, "m8-node-c", "127.0.0.3", 9103);

    repository.create_artifact(Artifact{artifact_id, "replication-complete-" + marker.str(), "m8s5"});
    repository.register_object(descriptor.object());
    repository.register_object_layout(descriptor);

    for (const ChunkRef& chunk : descriptor.layout().chunks()) {
        const std::vector<std::string> desired = select_replica_nodes(chunk.chunk_id, placement, 2U);
        register_chunk_on_node(repository, chunk, desired.front(), StorageLocationState::Available);
    }

    const ArtifactVersion version{
        artifact_id,
        descriptor.object_id(),
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{{"marker", marker.str()}},
        VersionState::Committed,
    };
    repository.create_version(version);

    (void)repository.start_replication_run(run_id, version.version_id(), 2U);

    try {
        (void)repository.complete_replication_run(run_id, ReplicationStats{});
        FAIL() << "expected ReplicationError";
    } catch (const ReplicationError& error) {
        EXPECT_EQ(error.kind(), ReplicationErrorKind::UnderReplicated);
    }

    for (const ChunkRef& chunk : descriptor.layout().chunks()) {
        const std::vector<std::string> desired = select_replica_nodes(chunk.chunk_id, placement, 2U);
        for (const std::string& node_id : desired) {
            register_chunk_on_node(repository, chunk, node_id, StorageLocationState::Available);
        }
    }

    const ReplicationStats stats{
        .chunks_scanned = 2,
        .replicas_verified = 4,
    };
    const auto completed = repository.complete_replication_run(run_id, stats);

    EXPECT_EQ(completed.state, ReplicationRunState::Completed);
    EXPECT_EQ(completed.stats.chunks_scanned, 2U);
    EXPECT_EQ(completed.stats.replicas_verified, 4U);
}

TEST(PostgresMetadataRepositoryTest, GcAndReplicationRunsAreMutuallyExclusive) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 replication_run_id = UuidV7::generate();
    const UuidV7 gc_run_id = UuidV7::generate();
    const auto descriptor = make_finalize_descriptor('1', '2', '3');

    register_storage_node(repository, "m8-node-a", "127.0.0.1", 9101);
    register_storage_node(repository, "m8-node-b", "127.0.0.2", 9102);
    register_storage_node(repository, "m8-node-c", "127.0.0.3", 9103);

    repository.create_artifact(Artifact{artifact_id, "gc-replication-" + marker.str(), "m8s5"});
    const ArtifactVersion version = register_replication_version(repository, artifact_id, descriptor, marker.str());
    (void)repository.start_replication_run(replication_run_id, version.version_id(), 2U);

    try {
        (void)repository.start_gc_run(make_open_gc_run(gc_run_id, "m8-node-a"));
        FAIL() << "expected GcError";
    } catch (const GcError& error) {
        EXPECT_EQ(error.kind(), GcErrorKind::ReplicationInProgress);
    }

    reset_metadata_data();
    register_storage_node(repository, "m8-node-a", "127.0.0.1", 9101);
    register_storage_node(repository, "m8-node-b", "127.0.0.2", 9102);
    register_storage_node(repository, "m8-node-c", "127.0.0.3", 9103);
    repository.create_artifact(Artifact{artifact_id, "gc-replication-" + marker.str(), "m8s5"});
    const ArtifactVersion version_after_reset =
        register_replication_version(repository, artifact_id, descriptor, marker.str());
    (void)repository.start_gc_run(make_open_gc_run(gc_run_id, "m8-node-a"));

    try {
        (void)repository.start_replication_run(replication_run_id, version_after_reset.version_id(), 2U);
        FAIL() << "expected ReplicationError";
    } catch (const ReplicationError& error) {
        EXPECT_EQ(error.kind(), ReplicationErrorKind::GcInProgress);
    }
}

TEST(PostgresMetadataRepositoryTest, PersistsAndLoadsLifecyclePolicy) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 policy_id = UuidV7::generate();
    const LifecyclePolicy policy = make_uniform_lifecycle_policy(policy_id, 3U, 86400ULL);

    repository.register_lifecycle_policy(policy);

    const auto loaded = repository.get_lifecycle_policy(policy_id);

    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->policy_id, policy_id);
    EXPECT_EQ(loaded->name, policy.name);
    EXPECT_EQ(loaded->rules, policy.rules);
}

TEST(PostgresMetadataRepositoryTest, LifecyclePolicyRegistrationIsIdempotentAndConflictsOnMismatch) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 policy_id = UuidV7::generate();
    const LifecyclePolicy policy = make_uniform_lifecycle_policy(policy_id, 2U);

    repository.register_lifecycle_policy(policy);
    EXPECT_NO_THROW(repository.register_lifecycle_policy(policy));

    LifecyclePolicy conflicting = policy;
    conflicting.rules.at(ArtifactKind::Generic).keep_last_n = 5U;

    try {
        repository.register_lifecycle_policy(conflicting);
        FAIL() << "expected LifecycleError";
    } catch (const LifecycleError& error) {
        EXPECT_EQ(error.kind(), LifecycleErrorKind::PolicyConflict);
    }
}

TEST(PostgresMetadataRepositoryTest, PinsAndUnpinsCommittedVersion) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 artifact_id = UuidV7::generate();

    repository.create_artifact(Artifact{artifact_id, "lifecycle-pin", "m9"});
    const ArtifactVersion version = create_committed_version_with_kind(repository, artifact_id, 'p', "generic");

    repository.pin_version(version.version_id(), "hold for eval");

    const auto pin = repository.get_version_pin(version.version_id());

    ASSERT_TRUE(pin.has_value());
    EXPECT_EQ(*pin, "hold for eval");
    EXPECT_TRUE(repository.unpin_version(version.version_id()));
    EXPECT_FALSE(repository.get_version_pin(version.version_id()).has_value());
}

TEST(PostgresMetadataRepositoryTest, RejectsPinningMissingOrRetiredVersion) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 policy_id = UuidV7::generate();

    repository.create_artifact(Artifact{artifact_id, "lifecycle-pin-reject", "m9"});
    const ArtifactVersion version = create_committed_version_with_kind(repository, artifact_id, 'r', "generic");
    repository.register_lifecycle_policy(make_uniform_lifecycle_policy(policy_id, 0U, 0ULL));

    try {
        repository.pin_version(std::string(64, 'f'), "reason");
        FAIL() << "expected LifecycleError";
    } catch (const LifecycleError& error) {
        EXPECT_EQ(error.kind(), LifecycleErrorKind::VersionNotFound);
    }

    retire_version_via_lifecycle(repository, version.version_id(), policy_id);

    try {
        repository.pin_version(version.version_id(), "reason");
        FAIL() << "expected LifecycleError";
    } catch (const LifecycleError& error) {
        EXPECT_EQ(error.kind(), LifecycleErrorKind::VersionRetired);
    }
}

TEST(PostgresMetadataRepositoryTest, DryRunAppliesKindSpecificKeepLastAndAgeRules) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 policy_id = UuidV7::generate();
    const UuidV7 run_id = UuidV7::generate();

    repository.create_artifact(Artifact{artifact_id, "lifecycle-dry-run", "m9"});

    LifecyclePolicy policy = make_uniform_lifecycle_policy(policy_id, 0U, 0ULL);
    policy.rules.at(ArtifactKind::Generic).keep_last_n = 1U;
    policy.rules.at(ArtifactKind::ModelCheckpoint).keep_last_n = 2U;
    policy.rules.at(ArtifactKind::DatasetSnapshot).max_age_seconds = 86400ULL;
    repository.register_lifecycle_policy(policy);

    const ArtifactVersion generic_old = create_committed_version_with_kind(repository, artifact_id, '1', "generic");
    const ArtifactVersion generic_new = create_committed_version_with_kind(repository, artifact_id, '2', "generic");
    const ArtifactVersion checkpoint_old =
        create_committed_version_with_kind(repository, artifact_id, '3', "model-checkpoint");
    const ArtifactVersion checkpoint_mid =
        create_committed_version_with_kind(repository, artifact_id, '4', "model-checkpoint");
    const ArtifactVersion checkpoint_new =
        create_committed_version_with_kind(repository, artifact_id, '5', "model-checkpoint");
    const ArtifactVersion dataset_old =
        create_committed_version_with_kind(repository, artifact_id, '6', "dataset-snapshot");
    const ArtifactVersion dataset_new =
        create_committed_version_with_kind(repository, artifact_id, '7', "dataset-snapshot");

    set_version_created_at(dataset_old.version_id(), "2020-01-01T00:00:00Z");

    const LifecycleRun run = repository.run_lifecycle(run_id, policy_id, LifecycleRunMode::DryRun);
    const std::vector<LifecycleDecision> decisions = repository.list_lifecycle_decisions(run_id, std::nullopt, 256U);

    EXPECT_EQ(run.mode, LifecycleRunMode::DryRun);
    EXPECT_EQ(run.stats.versions_scanned, 7U);
    EXPECT_EQ(run.stats.versions_retired, 0U);

    const LifecycleDecision* generic_kept = find_lifecycle_decision(decisions, generic_new.version_id());
    const LifecycleDecision* generic_retire = find_lifecycle_decision(decisions, generic_old.version_id());
    ASSERT_NE(generic_kept, nullptr);
    ASSERT_NE(generic_retire, nullptr);
    EXPECT_FALSE(generic_kept->retire);
    EXPECT_EQ(generic_kept->reason, LifecycleDecisionReason::KeepLastN);
    EXPECT_TRUE(generic_retire->retire);
    EXPECT_EQ(generic_retire->reason, LifecycleDecisionReason::PolicyRetire);

    const LifecycleDecision* checkpoint_mid_decision = find_lifecycle_decision(decisions, checkpoint_mid.version_id());
    const LifecycleDecision* checkpoint_old_decision = find_lifecycle_decision(decisions, checkpoint_old.version_id());
    ASSERT_NE(checkpoint_mid_decision, nullptr);
    ASSERT_NE(checkpoint_old_decision, nullptr);
    EXPECT_FALSE(checkpoint_mid_decision->retire);
    EXPECT_EQ(checkpoint_mid_decision->reason, LifecycleDecisionReason::KeepLastN);
    EXPECT_TRUE(checkpoint_old_decision->retire);
    EXPECT_EQ(checkpoint_old_decision->reason, LifecycleDecisionReason::PolicyRetire);

    const LifecycleDecision* dataset_old_decision = find_lifecycle_decision(decisions, dataset_old.version_id());
    const LifecycleDecision* dataset_new_decision = find_lifecycle_decision(decisions, dataset_new.version_id());
    ASSERT_NE(dataset_old_decision, nullptr);
    ASSERT_NE(dataset_new_decision, nullptr);
    EXPECT_TRUE(dataset_old_decision->retire);
    EXPECT_EQ(dataset_old_decision->reason, LifecycleDecisionReason::PolicyRetire);
    EXPECT_FALSE(dataset_new_decision->retire);
    EXPECT_EQ(dataset_new_decision->reason, LifecycleDecisionReason::AgeNotReached);
}

TEST(PostgresMetadataRepositoryTest, TagsAndManifestsProtectLifecycleCandidates) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 policy_id = UuidV7::generate();
    const UuidV7 run_id = UuidV7::generate();

    repository.create_artifact(Artifact{artifact_id, "lifecycle-protect", "m9"});
    repository.register_lifecycle_policy(make_uniform_lifecycle_policy(policy_id, 0U, 0ULL));

    const ArtifactVersion tagged = create_committed_version_with_kind(repository, artifact_id, 't', "generic");
    const ArtifactVersion manifest_version =
        create_committed_version_with_kind(repository, artifact_id, 'm', "generic");
    const ArtifactVersion retire_candidate =
        create_committed_version_with_kind(repository, artifact_id, 'c', "generic");

    repository.set_tag(artifact_id, "protected", tagged.version_id());

    const Manifest manifest{
        Manifest::Entries{
            {"entry", manifest_version.version_id()},
        },
    };
    repository.register_manifest(manifest);

    const LifecycleRun run = repository.run_lifecycle(run_id, policy_id, LifecycleRunMode::Apply);
    const std::vector<LifecycleDecision> decisions = repository.list_lifecycle_decisions(run_id, std::nullopt, 256U);

    const LifecycleDecision* tagged_decision = find_lifecycle_decision(decisions, tagged.version_id());
    const LifecycleDecision* manifest_decision = find_lifecycle_decision(decisions, manifest_version.version_id());
    const LifecycleDecision* retire_decision = find_lifecycle_decision(decisions, retire_candidate.version_id());
    ASSERT_NE(tagged_decision, nullptr);
    ASSERT_NE(manifest_decision, nullptr);
    ASSERT_NE(retire_decision, nullptr);

    EXPECT_FALSE(tagged_decision->retire);
    EXPECT_EQ(tagged_decision->reason, LifecycleDecisionReason::Tagged);
    EXPECT_FALSE(manifest_decision->retire);
    EXPECT_EQ(manifest_decision->reason, LifecycleDecisionReason::ManifestReferenced);
    EXPECT_TRUE(retire_decision->retire);
    EXPECT_EQ(retire_decision->reason, LifecycleDecisionReason::PolicyRetire);
    EXPECT_EQ(run.stats.versions_protected, 2U);
    EXPECT_EQ(run.stats.versions_retired, 1U);
}

TEST(PostgresMetadataRepositoryTest, LifecycleDecisionReasonsUseDeterministicPriority) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 policy_id = UuidV7::generate();
    const UuidV7 run_id = UuidV7::generate();

    repository.create_artifact(Artifact{artifact_id, "lifecycle-priority", "m9"});

    LifecyclePolicy policy = make_uniform_lifecycle_policy(policy_id, 1U, 0ULL);
    repository.register_lifecycle_policy(policy);

    const ArtifactVersion retire_candidate =
        create_committed_version_with_kind(repository, artifact_id, 'c', "generic");
    const ArtifactVersion pinned = create_committed_version_with_kind(repository, artifact_id, 'p', "generic");
    const ArtifactVersion tagged = create_committed_version_with_kind(repository, artifact_id, 't', "generic");
    const ArtifactVersion manifest_version =
        create_committed_version_with_kind(repository, artifact_id, 'm', "generic");
    const ArtifactVersion keep_last = create_committed_version_with_kind(repository, artifact_id, 'k', "generic");

    repository.pin_version(pinned.version_id(), "priority-test");
    repository.set_tag(artifact_id, "priority-tag", tagged.version_id());

    const Manifest manifest{
        Manifest::Entries{
            {"priority", manifest_version.version_id()},
        },
    };
    repository.register_manifest(manifest);

    repository.pin_version(manifest_version.version_id(), "also-pinned");

    const LifecycleRun run = repository.run_lifecycle(run_id, policy_id, LifecycleRunMode::DryRun);
    const std::vector<LifecycleDecision> decisions = repository.list_lifecycle_decisions(run_id, std::nullopt, 256U);

    const LifecycleDecision* pinned_decision = find_lifecycle_decision(decisions, pinned.version_id());
    const LifecycleDecision* tagged_decision = find_lifecycle_decision(decisions, tagged.version_id());
    const LifecycleDecision* manifest_decision = find_lifecycle_decision(decisions, manifest_version.version_id());
    const LifecycleDecision* keep_last_decision = find_lifecycle_decision(decisions, keep_last.version_id());
    const LifecycleDecision* retire_decision = find_lifecycle_decision(decisions, retire_candidate.version_id());
    ASSERT_NE(pinned_decision, nullptr);
    ASSERT_NE(tagged_decision, nullptr);
    ASSERT_NE(manifest_decision, nullptr);
    ASSERT_NE(keep_last_decision, nullptr);
    ASSERT_NE(retire_decision, nullptr);

    EXPECT_EQ(pinned_decision->reason, LifecycleDecisionReason::Pinned);
    EXPECT_EQ(tagged_decision->reason, LifecycleDecisionReason::Tagged);
    EXPECT_EQ(manifest_decision->reason, LifecycleDecisionReason::Pinned);
    EXPECT_EQ(keep_last_decision->reason, LifecycleDecisionReason::KeepLastN);
    EXPECT_EQ(retire_decision->reason, LifecycleDecisionReason::PolicyRetire);
    EXPECT_TRUE(retire_decision->retire);
    EXPECT_EQ(run.stats.versions_protected, 3U);
    EXPECT_EQ(run.stats.versions_retained_by_policy, 1U);
}

TEST(PostgresMetadataRepositoryTest, ApplyRetiresCandidatesAtomicallyAndPersistsDecisions) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 policy_id = UuidV7::generate();
    const UuidV7 run_id = UuidV7::generate();

    repository.create_artifact(Artifact{artifact_id, "lifecycle-apply", "m9"});
    repository.register_lifecycle_policy(make_uniform_lifecycle_policy(policy_id, 1U, 0ULL));

    const ArtifactVersion retired = create_committed_version_with_kind(repository, artifact_id, 'r', "generic");
    const ArtifactVersion kept = create_committed_version_with_kind(repository, artifact_id, 'k', "generic");

    const LifecycleRun run = repository.run_lifecycle(run_id, policy_id, LifecycleRunMode::Apply);
    const std::vector<LifecycleDecision> decisions = repository.list_lifecycle_decisions(run_id, std::nullopt, 256U);

    const LifecycleDecision* kept_decision = find_lifecycle_decision(decisions, kept.version_id());
    const LifecycleDecision* retired_decision = find_lifecycle_decision(decisions, retired.version_id());
    ASSERT_NE(kept_decision, nullptr);
    ASSERT_NE(retired_decision, nullptr);

    EXPECT_FALSE(kept_decision->retire);
    EXPECT_TRUE(retired_decision->retire);
    EXPECT_EQ(run.stats.versions_retired, 1U);
    EXPECT_EQ(run.stats.logical_bytes_retired, retired_decision->logical_size_bytes);

    pqxx::connection connection{test_database_connection_string()};
    pqxx::work transaction{connection};

    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) FROM lifecycle_run_decisions WHERE run_id = $1::uuid",
                                                 pqxx::params{run_id.str()}),
              2);
    EXPECT_EQ(
        transaction.query_value<long long>("SELECT COUNT(*) FROM artifact_version_retirements WHERE version_id = $1",
                                           pqxx::params{retired.version_id()}),
        1);
    EXPECT_EQ(transaction.query_value<std::string>(
                  "SELECT lifecycle_run_id::text FROM artifact_version_retirements WHERE version_id = $1",
                  pqxx::params{retired.version_id()}),
              run_id.str());

    transaction.commit();
}

TEST(PostgresMetadataRepositoryTest, LifecycleRunRetryIsIdempotent) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 policy_id = UuidV7::generate();
    const UuidV7 run_id = UuidV7::generate();
    const UuidV7 other_policy_id = UuidV7::generate();

    repository.create_artifact(Artifact{artifact_id, "lifecycle-idempotent", "m9"});
    repository.register_lifecycle_policy(make_uniform_lifecycle_policy(policy_id, 1U, 0ULL));
    repository.register_lifecycle_policy(make_uniform_lifecycle_policy(other_policy_id, 2U, 0ULL));
    (void)create_committed_version_with_kind(repository, artifact_id, 'i', "generic");

    const LifecycleRun first = repository.run_lifecycle(run_id, policy_id, LifecycleRunMode::DryRun);
    const LifecycleRun second = repository.run_lifecycle(run_id, policy_id, LifecycleRunMode::DryRun);

    EXPECT_EQ(second.run_id, first.run_id);
    EXPECT_EQ(second.policy_id, first.policy_id);
    EXPECT_EQ(second.mode, first.mode);
    EXPECT_EQ(second.stats, first.stats);

    try {
        (void)repository.run_lifecycle(run_id, other_policy_id, LifecycleRunMode::DryRun);
        FAIL() << "expected LifecycleError";
    } catch (const LifecycleError& error) {
        EXPECT_EQ(error.kind(), LifecycleErrorKind::RunConflict);
    }
}

TEST(PostgresMetadataRepositoryTest, LifecycleRunRejectsOpenUploadSession) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const UuidV7 policy_id = UuidV7::generate();
    const UuidV7 run_id = UuidV7::generate();

    repository.create_artifact(Artifact{artifact_id, "lifecycle-open-session", "m9"});
    repository.register_lifecycle_policy(make_uniform_lifecycle_policy(policy_id, 1U, 0ULL));
    register_storage_node(repository, "m9-node", "127.0.0.1", 9201);
    create_verified_upload_session(repository, UploadSession{
                                                   session_id,
                                                   artifact_id,
                                                   "m9-node",
                                                   ChunkingStrategy::FixedSize,
                                                   4,
                                                   std::nullopt,
                                                   UploadSession::ImmutableMetadata{},
                                                   UploadSessionState::Open,
                                                   std::nullopt,
                                               });

    try {
        (void)repository.run_lifecycle(run_id, policy_id, LifecycleRunMode::DryRun);
        FAIL() << "expected LifecycleError";
    } catch (const LifecycleError& error) {
        EXPECT_EQ(error.kind(), LifecycleErrorKind::BlockedByOpenUploadSessions);
    }
}

TEST(PostgresMetadataRepositoryTest, LifecycleRunRejectsOpenGcAndReplicationRun) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 policy_id = UuidV7::generate();
    const UuidV7 lifecycle_run_id = UuidV7::generate();
    const UuidV7 gc_run_id = UuidV7::generate();
    const UuidV7 replication_run_id = UuidV7::generate();
    const auto descriptor = make_finalize_descriptor('a', 'b', 'c');

    register_storage_node(repository, "m8-node-a", "127.0.0.1", 9101);
    register_storage_node(repository, "m8-node-b", "127.0.0.2", 9102);
    register_storage_node(repository, "m8-node-c", "127.0.0.3", 9103);

    repository.create_artifact(Artifact{artifact_id, "lifecycle-gc-repl-" + marker.str(), "m9"});
    repository.register_lifecycle_policy(make_uniform_lifecycle_policy(policy_id, 1U, 0ULL));
    const ArtifactVersion version = register_replication_version(repository, artifact_id, descriptor, marker.str());

    (void)repository.start_gc_run(make_open_gc_run(gc_run_id, "m8-node-a"));

    try {
        (void)repository.run_lifecycle(lifecycle_run_id, policy_id, LifecycleRunMode::DryRun);
        FAIL() << "expected LifecycleError";
    } catch (const LifecycleError& error) {
        EXPECT_EQ(error.kind(), LifecycleErrorKind::BlockedByGc);
    }

    reset_metadata_data();
    register_storage_node(repository, "m8-node-a", "127.0.0.1", 9101);
    register_storage_node(repository, "m8-node-b", "127.0.0.2", 9102);
    register_storage_node(repository, "m8-node-c", "127.0.0.3", 9103);
    repository.create_artifact(Artifact{artifact_id, "lifecycle-gc-repl-" + marker.str(), "m9"});
    repository.register_lifecycle_policy(make_uniform_lifecycle_policy(policy_id, 1U, 0ULL));
    const ArtifactVersion version_after_reset =
        register_replication_version(repository, artifact_id, descriptor, marker.str());
    (void)repository.start_replication_run(replication_run_id, version_after_reset.version_id(), 2U);

    try {
        (void)repository.run_lifecycle(lifecycle_run_id, policy_id, LifecycleRunMode::DryRun);
        FAIL() << "expected LifecycleError";
    } catch (const LifecycleError& error) {
        EXPECT_EQ(error.kind(), LifecycleErrorKind::BlockedByReplication);
    }

    (void)version;
}

TEST(PostgresMetadataRepositoryTest, PolicyRankingIsDeterministicAcrossTimestampTies) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 policy_id = UuidV7::generate();
    const UuidV7 run_id = UuidV7::generate();

    repository.create_artifact(Artifact{artifact_id, "lifecycle-ranking", "m9"});
    repository.register_lifecycle_policy(make_uniform_lifecycle_policy(policy_id, 1U, 0ULL));

    const ArtifactVersion first = create_committed_version_with_kind(repository, artifact_id, 'a', "generic");
    const ArtifactVersion second = create_committed_version_with_kind(repository, artifact_id, 'b', "generic");

    set_version_created_at(first.version_id(), "2024-06-01T12:00:00Z");
    set_version_created_at(second.version_id(), "2024-06-01T12:00:00Z");

    const std::string_view kept_version_id =
        first.version_id() < second.version_id() ? first.version_id() : second.version_id();
    const std::string_view retired_version_id =
        first.version_id() < second.version_id() ? second.version_id() : first.version_id();

    const LifecycleRun run = repository.run_lifecycle(run_id, policy_id, LifecycleRunMode::DryRun);
    const std::vector<LifecycleDecision> decisions = repository.list_lifecycle_decisions(run_id, std::nullopt, 256U);

    const LifecycleDecision* kept_decision = find_lifecycle_decision(decisions, kept_version_id);
    const LifecycleDecision* retired_decision = find_lifecycle_decision(decisions, retired_version_id);
    ASSERT_NE(kept_decision, nullptr);
    ASSERT_NE(retired_decision, nullptr);

    EXPECT_FALSE(kept_decision->retire);
    EXPECT_EQ(kept_decision->reason, LifecycleDecisionReason::KeepLastN);
    EXPECT_TRUE(retired_decision->retire);
    EXPECT_EQ(retired_decision->reason, LifecycleDecisionReason::PolicyRetire);
    EXPECT_EQ(run.stats.versions_retained_by_policy, 1U);
}

TEST(PostgresMetadataRepositoryTest, FinalizeRejectsExistingRetiredVersion) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const UuidV7 policy_id = UuidV7::generate();
    const auto descriptor = make_finalize_descriptor('a', 'b', 'c');

    repository.create_artifact(Artifact{artifact_id, "lifecycle-finalize-" + marker.str(), "m9"});
    repository.register_lifecycle_policy(make_uniform_lifecycle_policy(policy_id, 0U, 0ULL));
    register_storage_node(repository, "m9-finalize", "127.0.0.1", 9201);
    create_verified_upload_session(repository,
                                   make_finalize_session(session_id, artifact_id, "m9-finalize", marker.str()));
    register_finalize_chunks(repository, descriptor, "m9-finalize", StorageLocationState::Available);

    const auto result = repository.finalize_upload(session_id, descriptor);
    retire_version_via_lifecycle(repository, result.version_id, policy_id);

    {
        pqxx::connection connection{test_database_connection_string()};
        pqxx::work transaction{connection};
        transaction.exec("DELETE FROM object_layouts WHERE layout_id = $1", pqxx::params{result.layout_id}).no_rows();
        transaction.commit();
    }

    {
        pqxx::connection connection{test_database_connection_string()};
        pqxx::work transaction{connection};
        EXPECT_TRUE(transaction.query_value<bool>(
            "SELECT EXISTS (SELECT 1 FROM upload_session_finalizations WHERE session_id = $1::uuid)",
            pqxx::params{session_id.str()}));
        EXPECT_TRUE(transaction.query_value<bool>(
            "SELECT layout_id IS NULL FROM upload_session_finalizations WHERE session_id = $1::uuid",
            pqxx::params{session_id.str()}));
        transaction.commit();
    }

    try {
        (void)repository.finalize_upload(session_id, descriptor);
        FAIL() << "expected FinalizeUploadError";
    } catch (const FinalizeUploadError& error) {
        EXPECT_EQ(error.kind(), FinalizeUploadErrorKind::VersionRetired);
    }
}

TEST(PostgresMetadataRepositoryTest, SetTagAndRegisterManifestRejectRetiredVersion) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 policy_id = UuidV7::generate();
    const UuidV7 run_id = UuidV7::generate();

    repository.create_artifact(Artifact{artifact_id, "lifecycle-tag-manifest", "m9"});
    repository.register_lifecycle_policy(make_uniform_lifecycle_policy(policy_id, 0U, 0ULL));

    const ArtifactVersion tagged_target = create_committed_version_with_kind(repository, artifact_id, 't', "generic");
    const ArtifactVersion manifest_target = create_committed_version_with_kind(repository, artifact_id, 'm', "generic");

    (void)repository.run_lifecycle(run_id, policy_id, LifecycleRunMode::Apply);

    try {
        repository.set_tag(artifact_id, "latest", tagged_target.version_id());
        FAIL() << "expected LifecycleError";
    } catch (const LifecycleError& error) {
        EXPECT_EQ(error.kind(), LifecycleErrorKind::VersionRetired);
    }

    const Manifest manifest{
        Manifest::Entries{
            {"model", manifest_target.version_id()},
        },
    };

    try {
        repository.register_manifest(manifest);
        FAIL() << "expected LifecycleError";
    } catch (const LifecycleError& error) {
        EXPECT_EQ(error.kind(), LifecycleErrorKind::VersionRetired);
    }
}

TEST(PostgresMetadataRepositoryTest, RestorePlansRejectRetiredVersion) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 policy_id = UuidV7::generate();
    const std::string source_node = "m9-restore";

    register_storage_node(repository, source_node, "127.0.0.1", 9201);
    register_storage_node(repository, "m9-restore-b", "127.0.0.2", 9202);

    repository.create_artifact(Artifact{artifact_id, "lifecycle-restore-" + marker.str(), "m9"});
    repository.register_lifecycle_policy(make_uniform_lifecycle_policy(policy_id, 0U, 0ULL));
    repository.register_object(make_object());

    const auto layout = make_object_layout_descriptor();
    repository.register_object_layout(layout);
    register_finalize_chunks(repository, layout, source_node, StorageLocationState::Available);
    register_finalize_chunks(repository, layout, "m9-restore-b", StorageLocationState::Available);

    const ArtifactVersion version{
        artifact_id,
        kObjectId,
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{{"marker", marker.str()}},
        VersionState::Committed,
    };
    repository.create_version(version);
    retire_version_via_lifecycle(repository, version.version_id(), policy_id);

    try {
        (void)repository.resolve_restore_plan(version.version_id(), source_node);
        FAIL() << "expected RestorePlanError";
    } catch (const RestorePlanError& error) {
        EXPECT_EQ(error.kind(), RestorePlanErrorKind::VersionRetired);
    }

    try {
        (void)repository.resolve_multi_node_restore_plan(version.version_id());
        FAIL() << "expected RestorePlanError";
    } catch (const RestorePlanError& error) {
        EXPECT_EQ(error.kind(), RestorePlanErrorKind::VersionRetired);
    }
}

TEST(PostgresMetadataRepositoryTest, ReplicationRunRejectsRetiredVersion) {
    reset_metadata_data();
    PostgresMetadataRepository repository{test_database_connection_string()};
    const UuidV7 marker = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 policy_id = UuidV7::generate();
    const UuidV7 replication_run_id = UuidV7::generate();
    const auto descriptor = make_finalize_descriptor('d', 'e', 'f');

    register_storage_node(repository, "m8-node-a", "127.0.0.1", 9101);
    register_storage_node(repository, "m8-node-b", "127.0.0.2", 9102);
    register_storage_node(repository, "m8-node-c", "127.0.0.3", 9103);

    repository.create_artifact(Artifact{artifact_id, "lifecycle-replication-" + marker.str(), "m9"});
    repository.register_lifecycle_policy(make_uniform_lifecycle_policy(policy_id, 0U, 0ULL));
    const ArtifactVersion version = register_replication_version(repository, artifact_id, descriptor, marker.str());
    retire_version_via_lifecycle(repository, version.version_id(), policy_id);

    try {
        (void)repository.start_replication_run(replication_run_id, version.version_id(), 2U);
        FAIL() << "expected ReplicationError";
    } catch (const ReplicationError& error) {
        EXPECT_EQ(error.kind(), ReplicationErrorKind::VersionRetired);
    }
}

}  // namespace
