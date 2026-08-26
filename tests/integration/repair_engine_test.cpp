#include "aistore/replication/repair_engine.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <boost/beast/http.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
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
#include "aistore/client/storage_node_client_pool.hpp"
#include "aistore/hashing/sha256.hpp"
#include "aistore/http/http_client.hpp"
#include "aistore/http/http_server.hpp"
#include "aistore/metadata/artifact_model.hpp"
#include "aistore/metadata/chunk_metadata.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/placement.hpp"
#include "aistore/metadata/postgres_metadata_repository.hpp"
#include "aistore/metadata/replication.hpp"
#include "aistore/metadata/storage_location.hpp"
#include "aistore/metadata/storage_node.hpp"
#include "aistore/metadata/uuid_v7.hpp"
#include "aistore/service/metadata_service.hpp"
#include "aistore/service/storage_node_service.hpp"
#include "aistore/storage/local_chunk_store.hpp"

namespace {

using aistore::client::MetadataClient;
using aistore::client::RemoteApiError;
using aistore::client::StorageNodeClient;
using aistore::client::StorageNodeClientPool;
using aistore::http::HttpClientConfig;
using aistore::http::HttpEndpoint;
using aistore::http::HttpRequest;
using aistore::http::HttpResponse;
using aistore::http::HttpServer;
using aistore::http::HttpServerConfig;
using aistore::metadata::Artifact;
using aistore::metadata::ArtifactVersion;
using aistore::metadata::ChunkingStrategy;
using aistore::metadata::ChunkMetadata;
using aistore::metadata::ChunkRef;
using aistore::metadata::Object;
using aistore::metadata::ObjectLayout;
using aistore::metadata::ObjectLayoutDescriptor;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::ReplicationRunState;
using aistore::metadata::select_replica_nodes;
using aistore::metadata::StorageLocation;
using aistore::metadata::StorageLocationState;
using aistore::metadata::StorageNode;
using aistore::metadata::StorageNodeState;
using aistore::metadata::UuidV7;
using aistore::metadata::VersionState;
using aistore::replication::RepairEngine;
using aistore::replication::RepairRequest;
using aistore::replication::RepairResult;
using aistore::service::MetadataService;
using aistore::service::StorageNodeService;
using aistore::storage::LocalChunkStore;

namespace beast_http = aistore::http::beast_http;

constexpr std::uint64_t kMaxBodyBytes = 8ULL * 1024ULL * 1024ULL;
constexpr std::string_view kNodeA = "m8-repair-a";
constexpr std::string_view kNodeB = "m8-repair-b";
constexpr std::string_view kNodeC = "m8-repair-c";

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
                ("aistore-repair-" + std::to_string(timestamp) + "-" + std::to_string(counter.fetch_add(1)));
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

std::string sha256_hex(std::string_view text) {
    aistore::hashing::Sha256 hasher;
    hasher.update(std::as_bytes(std::span{text.data(), text.size()}));
    return digest_to_hex(hasher.finalize());
}

ObjectLayoutDescriptor make_repair_descriptor(std::string_view unique_tag) {
    const std::string chunk_a_bytes = std::string{"ABCD"} + std::string{unique_tag};
    const std::string chunk_b_bytes = std::string{"AB"} + std::string{unique_tag};
    const std::string object_bytes = chunk_a_bytes + chunk_b_bytes;
    return ObjectLayoutDescriptor{
        Object{sha256_hex(object_bytes), static_cast<std::uint64_t>(object_bytes.size())},
        ChunkingStrategy::FixedSize,
        ObjectLayout{{
            ChunkRef{.chunk_id = sha256_hex(chunk_a_bytes),
                     .offset = 0,
                     .size = static_cast<std::uint64_t>(chunk_a_bytes.size())},
            ChunkRef{.chunk_id = sha256_hex(chunk_b_bytes),
                     .offset = static_cast<std::uint64_t>(chunk_a_bytes.size()),
                     .size = static_cast<std::uint64_t>(chunk_b_bytes.size())},
        }},
    };
}

class RepairEngineFixture : public ::testing::Test {
   protected:
    struct NodeStack {
        explicit NodeStack(std::string id)
            : node_id(std::move(id)), store(root.path().string()), service(store, node_id) {}

        TemporaryDirectory root;
        std::string node_id;
        LocalChunkStore store;
        StorageNodeService service;
        RunningHttpServer server{[this](const HttpRequest& request) { return service.handle_request(request); }};
        StorageNodeClient client{HttpClientConfig{
            .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = server.port()},
        }};
    };

    void SetUp() override {
        artifact_id_ = UuidV7::generate();
        unique_tag_ = UuidV7::generate().str();

        repository_.emplace(test_database_connection_string());
        {
            pqxx::connection connection{test_database_connection_string()};
            pqxx::work transaction{connection};
            transaction
                .exec(
                    "DELETE FROM upload_session_finalizations WHERE session_id IN "
                    "(SELECT session_id FROM upload_sessions WHERE state = 'open')")
                .no_rows();
            transaction
                .exec(
                    "DELETE FROM upload_session_metadata WHERE session_id IN "
                    "(SELECT session_id FROM upload_sessions WHERE state = 'open')")
                .no_rows();
            transaction.exec("DELETE FROM upload_sessions WHERE state = 'open'").no_rows();
            transaction.exec("DELETE FROM replication_runs").no_rows();
            transaction.exec("DELETE FROM gc_runs").no_rows();
            transaction.exec("DELETE FROM storage_nodes").no_rows();
            transaction.commit();
        }
        repository_->create_artifact(
            Artifact{artifact_id_, std::string{"m8-repair-artifact-"} + artifact_id_.str(), "m8-repair-project"});

        metadata_service_.emplace(*repository_);
        metadata_server_.emplace(
            [&](const HttpRequest& request) { return metadata_service_->handle_request(request); });
        metadata_client_.emplace(HttpClientConfig{
            .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = metadata_server_->port()},
        });

        node_a_ = std::make_unique<NodeStack>(std::string{kNodeA});
        node_b_ = std::make_unique<NodeStack>(std::string{kNodeB});
        node_c_ = std::make_unique<NodeStack>(std::string{kNodeC});

        for (NodeStack* node : {node_a_.get(), node_b_.get(), node_c_.get()}) {
            metadata_client_->register_storage_node(StorageNode{
                .node_id = node->node_id,
                .address = "127.0.0.1",
                .port = node->server.port(),
                .state = StorageNodeState::Active,
            });
        }

        storage_pool_.emplace(std::vector<std::pair<std::string, StorageNodeClient>>{
            {std::string{kNodeA}, node_a_->client},
            {std::string{kNodeB}, node_b_->client},
            {std::string{kNodeC}, node_c_->client},
        });
    }

    void TearDown() override {
        repair_engine_.reset();
        storage_pool_.reset();
        node_c_.reset();
        node_b_.reset();
        node_a_.reset();
        metadata_client_.reset();
        metadata_server_.reset();
        metadata_service_.reset();

        if (repository_.has_value()) {
            cleanup_owned_rows();
            repository_.reset();
        }
    }

    void cleanup_owned_rows() {
        pqxx::connection connection{test_database_connection_string()};
        pqxx::work transaction{connection};

        for (const UuidV7& run_id : owned_run_ids_) {
            transaction.exec("DELETE FROM replication_runs WHERE run_id = $1::uuid", pqxx::params{run_id.str()})
                .no_rows();
        }

        for (const std::string& version_id : owned_version_ids_) {
            transaction.exec("DELETE FROM replication_runs WHERE version_id = $1", pqxx::params{version_id}).no_rows();
            transaction.exec("DELETE FROM artifact_version_metadata WHERE version_id = $1", pqxx::params{version_id})
                .no_rows();
            transaction.exec("DELETE FROM artifact_versions WHERE version_id = $1", pqxx::params{version_id}).no_rows();
        }

        for (const std::string& object_id : owned_object_ids_) {
            transaction
                .exec(
                    "DELETE FROM artifact_version_metadata WHERE version_id IN "
                    "(SELECT version_id FROM artifact_versions WHERE root_object_id = $1)",
                    pqxx::params{object_id})
                .no_rows();
            transaction.exec("DELETE FROM artifact_versions WHERE root_object_id = $1", pqxx::params{object_id})
                .no_rows();
            transaction
                .exec(
                    "DELETE FROM object_layout_chunks WHERE layout_id IN "
                    "(SELECT layout_id FROM object_layouts WHERE object_id = $1)",
                    pqxx::params{object_id})
                .no_rows();
            transaction.exec("DELETE FROM object_layouts WHERE object_id = $1", pqxx::params{object_id}).no_rows();
            transaction.exec("DELETE FROM objects WHERE object_id = $1", pqxx::params{object_id}).no_rows();
        }

        transaction.exec("DELETE FROM storage_locations WHERE node_id LIKE 'm8-repair-%'").no_rows();

        for (const std::string& chunk_id : owned_chunk_ids_) {
            transaction.exec("DELETE FROM storage_locations WHERE chunk_id = $1", pqxx::params{chunk_id}).no_rows();
            transaction.exec("DELETE FROM chunks WHERE chunk_id = $1", pqxx::params{chunk_id}).no_rows();
        }

        transaction.exec("DELETE FROM storage_nodes").no_rows();
        transaction.exec("DELETE FROM artifacts WHERE artifact_id = $1::uuid", pqxx::params{artifact_id_.str()})
            .no_rows();
        transaction.commit();
    }

    void track_chunk(std::string chunk_id) { owned_chunk_ids_.push_back(std::move(chunk_id)); }

    void track_object(std::string object_id) { owned_object_ids_.push_back(std::move(object_id)); }

    void track_version(std::string version_id) { owned_version_ids_.push_back(std::move(version_id)); }

    void track_run(UuidV7 run_id) { owned_run_ids_.push_back(std::move(run_id)); }

    void put_chunk_on_node(NodeStack& node, std::string_view chunk_id, std::string_view bytes) {
        track_chunk(std::string{chunk_id});
        node.store.put(std::string{chunk_id}, std::as_bytes(std::span{bytes.data(), bytes.size()}));
        metadata_client_->register_storage_location(StorageLocation{
            .chunk_id = std::string{chunk_id},
            .node_id = node.node_id,
            .storage_path = std::string{"/v1/chunks/"} + std::string{chunk_id},
            .state = StorageLocationState::Available,
        });
    }

    [[nodiscard]] std::pair<ArtifactVersion, ObjectLayoutDescriptor> create_under_replicated_version() {
        const ObjectLayoutDescriptor descriptor = make_repair_descriptor(unique_tag_);
        track_object(descriptor.object_id());

        repository_->register_object(descriptor.object());
        repository_->register_object_layout(descriptor);

        const std::vector<std::string> placement{std::string{kNodeA}, std::string{kNodeB}, std::string{kNodeC}};
        const std::string chunk_a_bytes = std::string{"ABCD"} + unique_tag_;
        const std::string chunk_b_bytes = std::string{"AB"} + unique_tag_;

        for (const ChunkRef& chunk : descriptor.layout().chunks()) {
            repository_->register_chunks({ChunkMetadata{.chunk_id = chunk.chunk_id, .size_bytes = chunk.size}});
            const std::vector<std::string> desired = select_replica_nodes(chunk.chunk_id, placement, 2U);
            const std::string bytes = chunk.chunk_id == sha256_hex(chunk_a_bytes) ? chunk_a_bytes : chunk_b_bytes;
            put_chunk_on_node(*node_for(desired.front()), chunk.chunk_id, bytes);
        }

        ArtifactVersion version{artifact_id_, descriptor.object_id(), std::nullopt,
                                ArtifactVersion::ImmutableMetadata{{"marker", unique_tag_}}, VersionState::Committed};
        repository_->create_version(version);
        track_version(version.version_id());
        return {version, descriptor};
    }

    [[nodiscard]] NodeStack* node_for(std::string_view node_id) {
        if (node_id == kNodeA) {
            return node_a_.get();
        }
        if (node_id == kNodeB) {
            return node_b_.get();
        }
        return node_c_.get();
    }

    RepairEngine& engine() {
        if (!repair_engine_.has_value()) {
            repair_engine_.emplace(*metadata_client_, *storage_pool_);
        }
        return *repair_engine_;
    }

    UuidV7 artifact_id_{UuidV7::generate()};
    std::string unique_tag_;
    std::vector<std::string> owned_chunk_ids_;
    std::vector<std::string> owned_object_ids_;
    std::vector<std::string> owned_version_ids_;
    std::vector<UuidV7> owned_run_ids_;

    std::optional<PostgresMetadataRepository> repository_;
    std::optional<MetadataService> metadata_service_;
    std::optional<RunningHttpServer> metadata_server_;
    std::optional<MetadataClient> metadata_client_;

    std::unique_ptr<NodeStack> node_a_;
    std::unique_ptr<NodeStack> node_b_;
    std::unique_ptr<NodeStack> node_c_;
    std::optional<StorageNodeClientPool> storage_pool_;
    std::optional<RepairEngine> repair_engine_;
};

TEST_F(RepairEngineFixture, RepairsUnderReplicatedChunkFromHealthyReplica) {
    const auto [version, descriptor] = create_under_replicated_version();
    (void)descriptor;
    const UuidV7 run_id = UuidV7::generate();
    track_run(run_id);

    const RepairResult result = engine().repair(RepairRequest{
        .run_id = run_id,
        .version_id = version.version_id(),
        .replication_factor = 2U,
    });

    EXPECT_EQ(result.run_id, run_id);
    EXPECT_GE(result.stats.replicas_written, 2U);

    const auto completed = metadata_client_->get_replication_run(run_id);
    ASSERT_TRUE(completed.has_value());
    EXPECT_EQ(completed->state, ReplicationRunState::Completed);
}

TEST_F(RepairEngineFixture, RepairsStaleAvailableMetadataAfterHeadMiss) {
    const auto [version, descriptor] = create_under_replicated_version();
    const std::vector<std::string> placement{std::string{kNodeA}, std::string{kNodeB}, std::string{kNodeC}};

    for (const ChunkRef& chunk : descriptor.layout().chunks()) {
        const std::vector<std::string> desired = select_replica_nodes(chunk.chunk_id, placement, 2U);
        metadata_client_->register_storage_location(StorageLocation{
            .chunk_id = chunk.chunk_id,
            .node_id = desired.back(),
            .storage_path = std::string{"/v1/chunks/"} + chunk.chunk_id,
            .state = StorageLocationState::Available,
        });
    }

    const UuidV7 run_id = UuidV7::generate();
    track_run(run_id);

    const RepairResult result = engine().repair(RepairRequest{
        .run_id = run_id,
        .version_id = version.version_id(),
        .replication_factor = 2U,
    });

    EXPECT_GE(result.stats.replicas_written, 1U);
    EXPECT_GE(result.stats.replicas_verified, 0U);
}

TEST_F(RepairEngineFixture, InterruptedRepairResumesWithoutRepeatingCompletedCopies) {
    const auto [version, descriptor] = create_under_replicated_version();
    const UuidV7 run_id = UuidV7::generate();
    track_run(run_id);

    const std::vector<std::string> placement{std::string{kNodeA}, std::string{kNodeB}, std::string{kNodeC}};
    const std::string chunk_a_bytes = std::string{"ABCD"} + unique_tag_;
    const std::string chunk_b_bytes = std::string{"AB"} + unique_tag_;

    (void)metadata_client_->start_replication_run(run_id, version.version_id(), 2U);

    for (const ChunkRef& chunk : descriptor.layout().chunks()) {
        const std::vector<std::string> desired = select_replica_nodes(chunk.chunk_id, placement, 2U);
        const std::string bytes = chunk.chunk_id == sha256_hex(chunk_a_bytes) ? chunk_a_bytes : chunk_b_bytes;
        put_chunk_on_node(*node_for(desired.back()), chunk.chunk_id, bytes);
    }

    const RepairResult resumed = engine().repair(RepairRequest{
        .run_id = run_id,
        .version_id = version.version_id(),
        .replication_factor = 2U,
    });

    EXPECT_EQ(resumed.run_id, run_id);
    EXPECT_EQ(resumed.stats.replicas_written, 0U);
    const auto completed = metadata_client_->get_replication_run(run_id);
    ASSERT_TRUE(completed.has_value());
    EXPECT_EQ(completed->state, ReplicationRunState::Completed);
}

TEST_F(RepairEngineFixture, CompletedRepairRetryNeedsNoStorageNode) {
    const auto [version, descriptor] = create_under_replicated_version();
    (void)descriptor;
    const UuidV7 run_id = UuidV7::generate();
    track_run(run_id);

    (void)engine().repair(RepairRequest{
        .run_id = run_id,
        .version_id = version.version_id(),
        .replication_factor = 2U,
    });

    MetadataClient unreachable_metadata{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = metadata_server_->port()},
    }};
    StorageNodeClient unreachable_storage{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = 1},
    }};
    StorageNodeClientPool empty_pool{std::vector<std::pair<std::string, StorageNodeClient>>{
        {std::string{kNodeA}, std::move(unreachable_storage)},
    }};
    RepairEngine retry_engine{unreachable_metadata, empty_pool};

    const RepairResult retry = retry_engine.repair(RepairRequest{
        .run_id = run_id,
        .version_id = version.version_id(),
        .replication_factor = 2U,
    });

    EXPECT_EQ(retry.run_id, run_id);
    EXPECT_EQ(retry.version_id, version.version_id());
}

}  // namespace
