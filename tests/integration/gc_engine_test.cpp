#include "aistore/gc/gc_engine.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <optional>
#include <pqxx/pqxx>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "aistore/client/client_error.hpp"
#include "aistore/client/metadata_client.hpp"
#include "aistore/client/storage_node_client.hpp"
#include "aistore/hashing/sha256.hpp"
#include "aistore/http/http_client.hpp"
#include "aistore/http/http_server.hpp"
#include "aistore/metadata/artifact_model.hpp"
#include "aistore/metadata/chunk_metadata.hpp"
#include "aistore/metadata/chunking.hpp"
#include "aistore/metadata/gc.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/postgres_metadata_repository.hpp"
#include "aistore/metadata/storage_location.hpp"
#include "aistore/metadata/uuid_v7.hpp"
#include "aistore/service/metadata_service.hpp"
#include "aistore/service/storage_node_service.hpp"
#include "aistore/storage/local_chunk_store.hpp"

namespace {

using aistore::client::MetadataClient;
using aistore::client::StorageNodeClient;
using aistore::gc::GcEngine;
using aistore::gc::GcRequest;
using aistore::http::HttpClientConfig;
using aistore::http::HttpEndpoint;
using aistore::http::HttpRequest;
using aistore::http::HttpServer;
using aistore::http::HttpServerConfig;
using aistore::metadata::Artifact;
using aistore::metadata::ArtifactVersion;
using aistore::metadata::ChunkingStrategy;
using aistore::metadata::ChunkMetadata;
using aistore::metadata::ChunkRef;
using aistore::metadata::GcRun;
using aistore::metadata::GcRunMode;
using aistore::metadata::GcRunState;
using aistore::metadata::Object;
using aistore::metadata::ObjectLayout;
using aistore::metadata::ObjectLayoutDescriptor;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::StorageLocation;
using aistore::metadata::StorageLocationState;
using aistore::metadata::UuidV7;
using aistore::metadata::VersionState;
using aistore::service::MetadataService;
using aistore::service::StorageNodeService;
using aistore::storage::LocalChunkStore;

constexpr std::uint64_t kMaxBodyBytes = 8ULL * 1024ULL * 1024ULL;
constexpr std::string_view kTargetNode = "m7gc-target";

constexpr std::string_view kSharedContent = "aaa";
constexpr std::string_view kLiveOnlyContent = "bbb";
constexpr std::string_view kOrphanOnlyContent = "ccc";
constexpr std::string_view kUntrackedContent = "ddd";
constexpr std::string_view kRetryOrphanContent = "orph";
constexpr std::string_view kCompletedRetryContent = "gone";

std::string test_database_connection_string() {
    const char* configured = std::getenv("AISTORE_TEST_DB_URL");

    if (configured != nullptr && configured[0] != '\0') {
        return configured;
    }

    return "dbname=ai_artifact_store_test";
}

class TemporaryDirectory {
   public:
    TemporaryDirectory() {
        static std::atomic<std::uint64_t> counter{0};
        const auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();
        path_ = std::filesystem::temp_directory_path() /
                ("aistore-gc-" + std::to_string(timestamp) + "-" + std::to_string(counter.fetch_add(1)));
        std::filesystem::create_directories(path_);
    }

    ~TemporaryDirectory() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

   private:
    std::filesystem::path path_;
};

class RunningHttpServer {
   public:
    explicit RunningHttpServer(aistore::http::RequestHandler handler)
        : server_(
              HttpServerConfig{
                  .bind_address = "127.0.0.1", .port = 0, .worker_threads = 2, .max_request_body_bytes = kMaxBodyBytes},
              std::move(handler)),
          worker_([this] { server_.run(); }) {}

    ~RunningHttpServer() {
        server_.stop();
        if (worker_.joinable()) {
            worker_.join();
        }
    }

    RunningHttpServer(const RunningHttpServer&) = delete;
    RunningHttpServer& operator=(const RunningHttpServer&) = delete;

    [[nodiscard]] std::uint16_t port() const noexcept { return server_.port(); }

   private:
    HttpServer server_;
    std::thread worker_;
};

std::string digest_to_hex(const aistore::hashing::Sha256::Digest& digest) {
    constexpr std::array<char, 16> kHexDigits{
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    };
    std::string result;
    result.reserve(digest.size() * 2U);
    for (const std::byte byte : digest) {
        const auto value = std::to_integer<unsigned int>(byte);
        result.push_back(kHexDigits[(value >> 4U) & 0x0FU]);
        result.push_back(kHexDigits[value & 0x0FU]);
    }
    return result;
}

std::string sha256_hex(std::span<const std::byte> bytes) {
    aistore::hashing::Sha256 hasher;
    hasher.update(bytes);
    return digest_to_hex(hasher.finalize());
}

std::string sha256_hex(std::string_view text) { return sha256_hex(std::as_bytes(std::span{text.data(), text.size()})); }

ObjectLayoutDescriptor make_live_layout(const std::string& shared_chunk_id, const std::string& live_only_chunk_id) {
    return ObjectLayoutDescriptor{
        Object{std::string(64, '1'), 6},
        ChunkingStrategy::FixedSize,
        ObjectLayout{{
            ChunkRef{.chunk_id = shared_chunk_id, .offset = 0, .size = kSharedContent.size()},
            ChunkRef{.chunk_id = live_only_chunk_id, .offset = kSharedContent.size(), .size = kLiveOnlyContent.size()},
        }},
    };
}

ObjectLayoutDescriptor make_dead_layout(const std::string& shared_chunk_id, const std::string& orphan_only_chunk_id) {
    return ObjectLayoutDescriptor{
        Object{std::string(64, '2'), 6},
        ChunkingStrategy::FixedSize,
        ObjectLayout{{
            ChunkRef{.chunk_id = shared_chunk_id, .offset = 0, .size = kSharedContent.size()},
            ChunkRef{
                .chunk_id = orphan_only_chunk_id, .offset = kSharedContent.size(), .size = kOrphanOnlyContent.size()},
        }},
    };
}

class GcEngineFixture : public ::testing::Test {
   protected:
    void SetUp() override {
        shared_chunk_id_ = sha256_hex(kSharedContent);
        live_only_chunk_id_ = sha256_hex(kLiveOnlyContent);
        orphan_only_chunk_id_ = sha256_hex(kOrphanOnlyContent);
        untracked_chunk_id_ = sha256_hex(kUntrackedContent);
        retry_orphan_chunk_id_ = sha256_hex(kRetryOrphanContent);
        completed_retry_chunk_id_ = sha256_hex(kCompletedRetryContent);

        artifact_id_ = UuidV7::generate();
        unique_tag_ = UuidV7::generate().str();

        repository_.emplace(test_database_connection_string());
        repository_->create_artifact(
            Artifact{artifact_id_, std::string{"m7gc-artifact-"} + artifact_id_.str(), "m7gc-project"});

        metadata_service_.emplace(*repository_);
        metadata_server_.emplace(
            [&](const HttpRequest& request) { return metadata_service_->handle_request(request); });
        metadata_client_.emplace(HttpClientConfig{
            .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = metadata_server_->port()},
        });

        storage_root_.emplace();
        chunk_store_.emplace(storage_root_->path().string());
        start_storage_server();
    }

    void TearDown() override {
        gc_engine_.reset();
        storage_client_.reset();
        stop_storage_server();
        chunk_store_.reset();
        storage_root_.reset();
        metadata_client_.reset();
        metadata_server_.reset();
        metadata_service_.reset();

        if (repository_.has_value()) {
            cleanup_owned_rows();
            repository_.reset();
        }
    }

    void start_storage_server() {
        storage_service_.emplace(*chunk_store_);
        storage_server_.emplace([&](const HttpRequest& request) { return storage_service_->handle_request(request); });
        storage_client_.emplace(HttpClientConfig{
            .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = storage_server_->port()},
        });
    }

    void stop_storage_server() {
        storage_client_.reset();
        storage_server_.reset();
        storage_service_.reset();
    }

    void cleanup_owned_rows() {
        pqxx::connection connection{test_database_connection_string()};
        pqxx::work transaction{connection};

        for (const UuidV7& gc_run_id : owned_gc_run_ids_) {
            transaction.exec("DELETE FROM gc_runs WHERE run_id = $1::uuid", pqxx::params{gc_run_id.str()}).no_rows();
        }

        transaction.exec("DELETE FROM artifact_versions WHERE artifact_id = $1::uuid", pqxx::params{artifact_id_.str()})
            .no_rows();

        for (const std::string& object_id : owned_object_ids_) {
            transaction
                .exec(
                    "DELETE FROM object_layout_chunks WHERE layout_id IN "
                    "(SELECT layout_id FROM object_layouts WHERE object_id = $1)",
                    pqxx::params{object_id})
                .no_rows();
            transaction.exec("DELETE FROM object_layouts WHERE object_id = $1", pqxx::params{object_id}).no_rows();
            transaction.exec("DELETE FROM objects WHERE object_id = $1", pqxx::params{object_id}).no_rows();
        }

        transaction.exec("DELETE FROM storage_locations WHERE node_id = $1", pqxx::params{std::string{kTargetNode}})
            .no_rows();

        for (const std::string& chunk_id : owned_chunk_ids_) {
            transaction.exec("DELETE FROM storage_locations WHERE chunk_id = $1", pqxx::params{chunk_id}).no_rows();
            transaction.exec("DELETE FROM chunks WHERE chunk_id = $1", pqxx::params{chunk_id}).no_rows();
        }

        transaction.exec("DELETE FROM artifacts WHERE artifact_id = $1::uuid", pqxx::params{artifact_id_.str()})
            .no_rows();
        transaction.commit();
    }

    void track_gc_run(UuidV7 gc_run_id) { owned_gc_run_ids_.push_back(std::move(gc_run_id)); }

    void track_chunk(std::string chunk_id) { owned_chunk_ids_.push_back(std::move(chunk_id)); }

    void track_object(std::string object_id) { owned_object_ids_.push_back(std::move(object_id)); }

    void register_live_metadata(const ObjectLayoutDescriptor& live_layout) {
        repository_->register_object(live_layout.object());
        repository_->register_object_layout(live_layout);
        repository_->create_version(ArtifactVersion{
            artifact_id_,
            live_layout.object_id(),
            std::nullopt,
            ArtifactVersion::ImmutableMetadata{{"marker", unique_tag_}},
            VersionState::Committed,
        });
        track_object(live_layout.object_id());

        for (const ChunkRef& chunk : live_layout.layout().chunks()) {
            if (!repository_->get_chunk_size(chunk.chunk_id).has_value()) {
                repository_->register_chunks({ChunkMetadata{.chunk_id = chunk.chunk_id, .size_bytes = chunk.size}});
            }
            track_chunk(chunk.chunk_id);
            repository_->register_storage_location(StorageLocation{
                .chunk_id = chunk.chunk_id,
                .node_id = std::string{kTargetNode},
                .storage_path = std::string{"/v1/chunks/"} + chunk.chunk_id,
                .state = StorageLocationState::Available,
            });
        }
    }

    void register_dead_metadata(const ObjectLayoutDescriptor& dead_layout) {
        repository_->register_object(dead_layout.object());
        repository_->register_object_layout(dead_layout);
        track_object(dead_layout.object_id());

        for (const ChunkRef& chunk : dead_layout.layout().chunks()) {
            if (chunk.chunk_id == shared_chunk_id_) {
                continue;
            }

            if (!repository_->get_chunk_size(chunk.chunk_id).has_value()) {
                repository_->register_chunks({ChunkMetadata{.chunk_id = chunk.chunk_id, .size_bytes = chunk.size}});
            }
            track_chunk(chunk.chunk_id);
            repository_->register_storage_location(StorageLocation{
                .chunk_id = chunk.chunk_id,
                .node_id = std::string{kTargetNode},
                .storage_path = std::string{"/v1/chunks/"} + chunk.chunk_id,
                .state = StorageLocationState::Available,
            });
        }
    }

    void put_physical_chunk(std::string_view chunk_id, std::string_view contents) {
        track_chunk(std::string{chunk_id});
        chunk_store_->put(std::string{chunk_id}, std::as_bytes(std::span{contents.data(), contents.size()}));
    }

    GcEngine& engine() {
        if (!gc_engine_.has_value()) {
            gc_engine_.emplace(*metadata_client_, *storage_client_, std::string{kTargetNode});
        }
        return *gc_engine_;
    }

    UuidV7 artifact_id_{UuidV7::generate()};
    std::string unique_tag_;
    std::string shared_chunk_id_;
    std::string live_only_chunk_id_;
    std::string orphan_only_chunk_id_;
    std::string untracked_chunk_id_;
    std::string retry_orphan_chunk_id_;
    std::string completed_retry_chunk_id_;
    std::vector<UuidV7> owned_gc_run_ids_;
    std::vector<std::string> owned_chunk_ids_;
    std::vector<std::string> owned_object_ids_;

    std::optional<PostgresMetadataRepository> repository_;
    std::optional<MetadataService> metadata_service_;
    std::optional<RunningHttpServer> metadata_server_;
    std::optional<MetadataClient> metadata_client_;

    std::optional<TemporaryDirectory> storage_root_;
    std::optional<LocalChunkStore> chunk_store_;
    std::optional<StorageNodeService> storage_service_;
    std::optional<RunningHttpServer> storage_server_;
    std::optional<StorageNodeClient> storage_client_;
    std::optional<GcEngine> gc_engine_;
};

TEST_F(GcEngineFixture, DryRunInventoriesWithoutDeleting) {
    const ObjectLayoutDescriptor live_layout = make_live_layout(shared_chunk_id_, live_only_chunk_id_);
    const ObjectLayoutDescriptor dead_layout = make_dead_layout(shared_chunk_id_, orphan_only_chunk_id_);
    register_live_metadata(live_layout);
    register_dead_metadata(dead_layout);

    put_physical_chunk(shared_chunk_id_, kSharedContent);
    put_physical_chunk(live_only_chunk_id_, kLiveOnlyContent);
    put_physical_chunk(orphan_only_chunk_id_, kOrphanOnlyContent);
    put_physical_chunk(untracked_chunk_id_, kUntrackedContent);

    const UuidV7 gc_run_id = UuidV7::generate();
    track_gc_run(gc_run_id);

    const GcRun completed = engine().collect(GcRequest{.run_id = gc_run_id, .dry_run = true});

    EXPECT_EQ(completed.mode, GcRunMode::DryRun);
    EXPECT_EQ(completed.state, GcRunState::Completed);
    EXPECT_GE(completed.physical_stats.physical_chunks_scanned, 2U);
    EXPECT_GE(completed.physical_stats.collectible_chunks, 1U);
    EXPECT_EQ(completed.physical_stats.physically_deleted_chunks, 0U);
    EXPECT_EQ(completed.physical_stats.physically_deleted_bytes, 0U);

    EXPECT_TRUE(chunk_store_->contains(shared_chunk_id_));
    EXPECT_TRUE(chunk_store_->contains(live_only_chunk_id_));
    EXPECT_TRUE(chunk_store_->contains(orphan_only_chunk_id_));
    EXPECT_TRUE(chunk_store_->contains(untracked_chunk_id_));
}

TEST_F(GcEngineFixture, ApplyDeletesCollectibleAndPreservesLiveChunk) {
    const ObjectLayoutDescriptor live_layout = make_live_layout(shared_chunk_id_, live_only_chunk_id_);
    const ObjectLayoutDescriptor dead_layout = make_dead_layout(shared_chunk_id_, orphan_only_chunk_id_);
    register_live_metadata(live_layout);
    register_dead_metadata(dead_layout);

    put_physical_chunk(shared_chunk_id_, kSharedContent);
    put_physical_chunk(live_only_chunk_id_, kLiveOnlyContent);
    put_physical_chunk(orphan_only_chunk_id_, kOrphanOnlyContent);

    const UuidV7 gc_run_id = UuidV7::generate();
    track_gc_run(gc_run_id);

    const GcRun completed = engine().collect(GcRequest{.run_id = gc_run_id, .dry_run = false});

    EXPECT_EQ(completed.mode, GcRunMode::Apply);
    EXPECT_EQ(completed.state, GcRunState::Completed);
    EXPECT_GE(completed.physical_stats.collectible_chunks, 1U);
    EXPECT_GE(completed.physical_stats.physically_deleted_chunks, 1U);

    EXPECT_TRUE(chunk_store_->contains(shared_chunk_id_));
    EXPECT_TRUE(chunk_store_->contains(live_only_chunk_id_));
    EXPECT_FALSE(chunk_store_->contains(orphan_only_chunk_id_));
}

TEST_F(GcEngineFixture, ApplyDeletesPhysicalChunkMissingFromMetadata) {
    put_physical_chunk(untracked_chunk_id_, kUntrackedContent);

    const UuidV7 gc_run_id = UuidV7::generate();
    track_gc_run(gc_run_id);

    const GcRun completed = engine().collect(GcRequest{.run_id = gc_run_id, .dry_run = false});

    EXPECT_EQ(completed.state, GcRunState::Completed);
    EXPECT_EQ(completed.physical_stats.collectible_chunks, 1U);
    EXPECT_EQ(completed.physical_stats.physically_deleted_chunks, 1U);
    EXPECT_FALSE(chunk_store_->contains(untracked_chunk_id_));
}

TEST_F(GcEngineFixture, StorageFailureLeavesGcOpenAndRetryCompletes) {
    put_physical_chunk(retry_orphan_chunk_id_, kRetryOrphanContent);

    const UuidV7 gc_run_id = UuidV7::generate();
    track_gc_run(gc_run_id);

    const std::uint16_t storage_port = storage_server_->port();
    stop_storage_server();
    gc_engine_.reset();

    StorageNodeClient dead_storage{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = storage_port},
    }};
    GcEngine failing_engine{*metadata_client_, dead_storage, std::string{kTargetNode}};
    EXPECT_THROW((void)failing_engine.collect(GcRequest{.run_id = gc_run_id, .dry_run = false}), std::exception);

    const std::optional<GcRun> open_run = metadata_client_->get_gc_run(gc_run_id);
    ASSERT_TRUE(open_run.has_value());
    EXPECT_EQ(open_run->state, GcRunState::Open);

    start_storage_server();
    gc_engine_.reset();

    GcEngine retry_engine{*metadata_client_, *storage_client_, std::string{kTargetNode}};
    const GcRun completed = retry_engine.collect(GcRequest{.run_id = gc_run_id, .dry_run = false});

    EXPECT_EQ(completed.state, GcRunState::Completed);
    EXPECT_FALSE(chunk_store_->contains(retry_orphan_chunk_id_));
}

TEST_F(GcEngineFixture, CompletedRunRetryDoesNotContactStorageNode) {
    put_physical_chunk(completed_retry_chunk_id_, kCompletedRetryContent);

    const UuidV7 gc_run_id = UuidV7::generate();
    track_gc_run(gc_run_id);

    const GcRun completed = engine().collect(GcRequest{.run_id = gc_run_id, .dry_run = false});
    EXPECT_EQ(completed.state, GcRunState::Completed);

    stop_storage_server();
    gc_engine_.reset();

    StorageNodeClient unreachable_storage{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = 1},
    }};

    GcEngine offline_engine{*metadata_client_, unreachable_storage, std::string{kTargetNode}};
    const GcRun retried = offline_engine.collect(GcRequest{.run_id = gc_run_id, .dry_run = false});

    EXPECT_EQ(retried.state, GcRunState::Completed);
    EXPECT_EQ(retried.run_id, gc_run_id);
    EXPECT_EQ(retried.physical_stats.physically_deleted_chunks, completed.physical_stats.physically_deleted_chunks);
}

}  // namespace
