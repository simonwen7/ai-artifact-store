#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>
#include <pqxx/pqxx>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "aistore/client/client_error.hpp"
#include "aistore/client/metadata_client.hpp"
#include "aistore/client/storage_node_client_pool.hpp"
#include "aistore/http/http_client.hpp"
#include "aistore/http/http_server.hpp"
#include "aistore/metadata/artifact_model.hpp"
#include "aistore/metadata/finalize_upload.hpp"
#include "aistore/metadata/postgres_metadata_repository.hpp"
#include "aistore/metadata/storage_location.hpp"
#include "aistore/metadata/storage_node.hpp"
#include "aistore/metadata/upload_session.hpp"
#include "aistore/push/push_engine.hpp"
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
using aistore::http::HttpServer;
using aistore::http::HttpServerConfig;
using aistore::metadata::Artifact;
using aistore::metadata::ChunkingStrategy;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::StorageLocation;
using aistore::metadata::StorageLocationState;
using aistore::metadata::UploadSession;
using aistore::metadata::UploadSessionState;
using aistore::metadata::UuidV7;
using aistore::push::PreparedPush;
using aistore::push::PushEngine;
using aistore::push::PushRequest;
using aistore::service::MetadataService;
using aistore::service::StorageNodeService;
using aistore::storage::LocalChunkStore;

constexpr std::uint64_t kMaxBodyBytes = 8ULL * 1024ULL * 1024ULL;
constexpr std::string_view kTargetNode = "m4s5-push-target";

std::string test_database_connection_string() {
    const char* configured = std::getenv("AISTORE_TEST_DB_URL");
    return configured != nullptr && configured[0] != '\0' ? configured : "dbname=ai_artifact_store_test";
}

class TemporaryDirectory {
   public:
    TemporaryDirectory() {
        static std::atomic<std::uint64_t> counter{0};
        path_ =
            std::filesystem::temp_directory_path() /
            ("aistore-push-finalize-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
             "-" + std::to_string(counter.fetch_add(1)));
        std::filesystem::create_directories(path_);
    }
    ~TemporaryDirectory() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }
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
    [[nodiscard]] std::uint16_t port() const noexcept { return server_.port(); }

   private:
    HttpServer server_;
    std::thread worker_;
};

std::filesystem::path write_temp_file(const std::filesystem::path& directory, std::string_view name,
                                      std::string_view contents) {
    const auto path = directory / std::string{name};
    std::ofstream output{path, std::ios::binary};
    output.write(contents.data(), static_cast<std::streamsize>(contents.size()));
    return path;
}

class PushFinalizeTest : public ::testing::Test {
   protected:
    void SetUp() override {
        marker_ = UuidV7::generate().str();
        artifact_id_ = UuidV7::generate();
        session_id_ = UuidV7::generate();
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
            transaction.commit();
        }
        repository_->create_artifact(Artifact{artifact_id_, "m4s5-push-" + marker_, "m4s5"});
        metadata_service_.emplace(*repository_);
        metadata_server_.emplace(
            [&](const HttpRequest& request) { return metadata_service_->handle_request(request); });
        metadata_client_.emplace(HttpClientConfig{
            .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = metadata_server_->port()},
        });
        storage_root_.emplace();
        chunk_store_.emplace(storage_root_->path().string());
        storage_service_.emplace(*chunk_store_);
        storage_server_.emplace([&](const HttpRequest& request) { return storage_service_->handle_request(request); });
        storage_client_.emplace(HttpClientConfig{
            .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = storage_server_->port()},
        });
        storage_pool_.emplace(std::vector<std::pair<std::string, StorageNodeClient>>{
            {std::string{kTargetNode}, *storage_client_},
        });
        source_root_.emplace();
        engine_.emplace(*metadata_client_, *storage_pool_);
    }

    void TearDown() override {
        engine_.reset();
        storage_pool_.reset();
        storage_client_.reset();
        storage_server_.reset();
        storage_service_.reset();
        chunk_store_.reset();
        storage_root_.reset();
        metadata_client_.reset();
        metadata_server_.reset();
        metadata_service_.reset();

        pqxx::connection connection{test_database_connection_string()};
        pqxx::work transaction{connection};
        transaction.exec("DELETE FROM upload_sessions WHERE artifact_id = $1::uuid", pqxx::params{artifact_id_.str()})
            .no_rows();
        transaction.exec("DELETE FROM artifact_versions WHERE artifact_id = $1::uuid", pqxx::params{artifact_id_.str()})
            .no_rows();
        for (const std::string& object_id : object_ids_) {
            transaction
                .exec(
                    "DELETE FROM object_layout_chunks WHERE layout_id IN "
                    "(SELECT layout_id FROM object_layouts WHERE object_id = $1)",
                    pqxx::params{object_id})
                .no_rows();
            transaction.exec("DELETE FROM object_layouts WHERE object_id = $1", pqxx::params{object_id}).no_rows();
            transaction.exec("DELETE FROM objects WHERE object_id = $1", pqxx::params{object_id}).no_rows();
        }
        for (const std::string& chunk_id : chunk_ids_) {
            transaction.exec("DELETE FROM storage_locations WHERE chunk_id = $1", pqxx::params{chunk_id}).no_rows();
            transaction.exec("DELETE FROM chunks WHERE chunk_id = $1", pqxx::params{chunk_id}).no_rows();
        }
        transaction.exec("DELETE FROM artifacts WHERE artifact_id = $1::uuid", pqxx::params{artifact_id_.str()})
            .no_rows();
        transaction.commit();
        repository_.reset();
        source_root_.reset();
    }

    void create_session() {
        metadata_client_->register_storage_node(aistore::metadata::StorageNode{
            .node_id = std::string{kTargetNode},
            .address = "127.0.0.1",
            .port = 8081,
            .state = aistore::metadata::StorageNodeState::Active,
        });
        (void)metadata_client_->create_upload_session(UploadSession{session_id_,
                                                                    artifact_id_,
                                                                    std::string{kTargetNode},
                                                                    ChunkingStrategy::FixedSize,
                                                                    4,
                                                                    std::nullopt,
                                                                    {{"source", marker_}},
                                                                    UploadSessionState::Open,
                                                                    std::nullopt});
    }

    PreparedPush push(std::string_view contents, std::string_view filename) {
        const auto source = write_temp_file(source_root_->path(), filename, contents);
        PreparedPush prepared = engine_->push(PushRequest{.source_path = source, .session_id = session_id_});
        object_ids_.push_back(prepared.layout_descriptor.object_id());
        for (const auto& chunk : prepared.layout_descriptor.layout().chunks()) {
            chunk_ids_.push_back(chunk.chunk_id);
        }
        return prepared;
    }

    std::string marker_;
    UuidV7 artifact_id_{UuidV7::generate()};
    UuidV7 session_id_{UuidV7::generate()};
    std::vector<std::string> object_ids_;
    std::vector<std::string> chunk_ids_;
    std::optional<PostgresMetadataRepository> repository_;
    std::optional<MetadataService> metadata_service_;
    std::optional<RunningHttpServer> metadata_server_;
    std::optional<MetadataClient> metadata_client_;
    std::optional<TemporaryDirectory> storage_root_;
    std::optional<LocalChunkStore> chunk_store_;
    std::optional<StorageNodeService> storage_service_;
    std::optional<RunningHttpServer> storage_server_;
    std::optional<StorageNodeClient> storage_client_;
    std::optional<StorageNodeClientPool> storage_pool_;
    std::optional<TemporaryDirectory> source_root_;
    std::optional<PushEngine> engine_;
};

TEST_F(PushFinalizeTest, PushThenFinalizeCommitsArtifactVersion) {
    create_session();
    const PreparedPush prepared = push("abcdefghij-" + marker_.substr(0, 4), "commit.bin");
    const auto result = metadata_client_->finalize_upload(session_id_, prepared.layout_descriptor);
    const auto version = repository_->get_version(result.version_id);
    const auto session = repository_->get_upload_session(session_id_);
    ASSERT_TRUE(version.has_value());
    ASSERT_TRUE(session.has_value());
    EXPECT_EQ(version->root_object_id(), prepared.layout_descriptor.object_id());
    EXPECT_EQ(version->immutable_metadata().at("source"), marker_);
    EXPECT_EQ(session->state(), UploadSessionState::Committed);
    EXPECT_EQ(session->finalized_version_id(), result.version_id);
}

TEST_F(PushFinalizeTest, PushThenFinalizeRetryIsIdempotent) {
    create_session();
    const PreparedPush prepared = push("retry-data-" + marker_.substr(0, 5), "retry.bin");
    const auto first = metadata_client_->finalize_upload(session_id_, prepared.layout_descriptor);
    const auto second = metadata_client_->finalize_upload(session_id_, prepared.layout_descriptor);
    EXPECT_EQ(second.version_id, first.version_id);
    EXPECT_EQ(second.layout_id, first.layout_id);
    pqxx::connection connection{test_database_connection_string()};
    pqxx::work transaction{connection};
    EXPECT_EQ(transaction.query_value<long long>("SELECT COUNT(*) FROM artifact_versions WHERE version_id = $1",
                                                 pqxx::params{first.version_id}),
              1);
    transaction.commit();
}

TEST_F(PushFinalizeTest, FailedFinalizeLeavesSessionOpenAndPushDataDurable) {
    create_session();
    const PreparedPush prepared = push("durable-" + marker_.substr(0, 5), "durable.bin");
    ASSERT_FALSE(prepared.layout_descriptor.layout().chunks().empty());
    const auto& first_chunk = prepared.layout_descriptor.layout().chunks().front();
    repository_->register_storage_location(StorageLocation{
        .chunk_id = first_chunk.chunk_id,
        .node_id = std::string{kTargetNode},
        .storage_path = std::string{"/v1/chunks/"} + first_chunk.chunk_id,
        .state = StorageLocationState::Missing,
    });

    try {
        (void)metadata_client_->finalize_upload(session_id_, prepared.layout_descriptor);
        FAIL() << "expected RemoteApiError";
    } catch (const RemoteApiError& error) {
        EXPECT_EQ(error.status_code(), 409U);
        EXPECT_EQ(error.error_code(), "chunk_under_replicated");
    }

    const auto session = repository_->get_upload_session(session_id_);
    ASSERT_TRUE(session.has_value());
    EXPECT_EQ(session->state(), UploadSessionState::Open);
    EXPECT_FALSE(session->finalized_version_id().has_value());
    for (const auto& chunk : prepared.layout_descriptor.layout().chunks()) {
        EXPECT_TRUE(chunk_store_->contains(chunk.chunk_id));
        EXPECT_EQ(repository_->get_chunk_size(chunk.chunk_id), chunk.size);
    }
}

}  // namespace
