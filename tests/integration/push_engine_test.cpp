#include "aistore/push/push_engine.hpp"

#include <gtest/gtest.h>

#include <array>
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
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "aistore/chunking/fastcdc_chunker.hpp"
#include "aistore/client/client_error.hpp"
#include "aistore/client/metadata_client.hpp"
#include "aistore/client/storage_node_client_pool.hpp"
#include "aistore/hashing/sha256.hpp"
#include "aistore/http/http_client.hpp"
#include "aistore/http/http_server.hpp"
#include "aistore/metadata/artifact_model.hpp"
#include "aistore/metadata/chunk_metadata.hpp"
#include "aistore/metadata/chunking.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/placement.hpp"
#include "aistore/metadata/storage_location.hpp"
#include "aistore/metadata/storage_node.hpp"
#include "aistore/metadata/upload_session.hpp"
#include "aistore/metadata/uuid_v7.hpp"
#include "aistore/service/metadata_service.hpp"
#include "aistore/service/storage_node_service.hpp"
#include "aistore/storage/local_chunk_store.hpp"

namespace {

using aistore::chunking::ChunkBuffer;
using aistore::chunking::FastCdcChunker;
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
using aistore::metadata::ChunkingStrategy;
using aistore::metadata::ChunkMetadata;
using aistore::metadata::ChunkRef;
using aistore::metadata::FastCdcParameters;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::select_replica_nodes;
using aistore::metadata::StorageLocation;
using aistore::metadata::StorageLocationState;
using aistore::metadata::StorageNode;
using aistore::metadata::StorageNodeState;
using aistore::metadata::UploadSession;
using aistore::metadata::UploadSessionState;
using aistore::metadata::UuidV7;
using aistore::push::PreparedPush;
using aistore::push::PushEngine;
using aistore::push::PushRequest;
using aistore::service::MetadataService;
using aistore::service::StorageNodeService;
using aistore::storage::LocalChunkStore;

namespace beast_http = aistore::http::beast_http;

constexpr std::uint64_t kMaxBodyBytes = 8ULL * 1024ULL * 1024ULL;
constexpr std::string_view kTargetNode = "m4s4-target";
constexpr std::string_view kOtherNode = "m4s4-other";

constexpr FastCdcParameters kTestFastCdcParams{
    .min_chunk_size_bytes = 64U,
    .avg_chunk_size_bytes = 128U,
    .max_chunk_size_bytes = 256U,
};

std::vector<std::byte> make_fastcdc_golden_fixture() {
    std::vector<std::byte> fixture;
    fixture.reserve(4096U);

    for (std::size_t index = 0; index < 4096U; ++index) {
        fixture.push_back(static_cast<std::byte>(index % 251U));
    }

    return fixture;
}

std::vector<ChunkBuffer> chunk_bytes_fastcdc(std::span<const std::byte> data, const FastCdcParameters& parameters) {
    FastCdcChunker chunker{static_cast<std::size_t>(parameters.min_chunk_size_bytes),
                           static_cast<std::size_t>(parameters.avg_chunk_size_bytes),
                           static_cast<std::size_t>(parameters.max_chunk_size_bytes)};

    std::vector<ChunkBuffer> chunks;
    const auto collect = [&chunks](ChunkBuffer chunk) { chunks.push_back(std::move(chunk)); };

    chunker.update(data, collect);
    chunker.finalize(collect);
    return chunks;
}

std::filesystem::path write_temp_bytes(const std::filesystem::path& directory, std::string_view name,
                                       std::span<const std::byte> bytes) {
    const std::filesystem::path path = directory / std::string{name};
    std::ofstream output{path, std::ios::binary};
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    output.close();
    return path;
}

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
                ("aistore-push-" + std::to_string(timestamp) + "-" + std::to_string(counter.fetch_add(1)));
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

std::filesystem::path write_temp_file(const std::filesystem::path& directory, std::string_view name,
                                      std::string_view contents) {
    const std::filesystem::path path = directory / std::string{name};
    std::ofstream output{path, std::ios::binary};
    output.write(contents.data(), static_cast<std::streamsize>(contents.size()));
    output.close();
    return path;
}

class PushEngineFixture : public ::testing::Test {
   protected:
    void SetUp() override {
        artifact_id_ = UuidV7::generate();
        session_id_ = UuidV7::generate();
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
            transaction
                .exec(
                    "DELETE FROM upload_session_nodes WHERE session_id IN "
                    "(SELECT session_id FROM upload_sessions WHERE state = 'open')")
                .no_rows();
            transaction.exec("DELETE FROM upload_sessions WHERE state = 'open'").no_rows();
            transaction.exec("DELETE FROM replication_runs").no_rows();
            transaction.exec("DELETE FROM gc_runs").no_rows();
            transaction.exec("DELETE FROM storage_nodes").no_rows();
            transaction.commit();
        }
        repository_->create_artifact(
            Artifact{artifact_id_, std::string{"m4s4-artifact-"} + artifact_id_.str(), "m4s4-project"});

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

        source_dir_.emplace();
    }

    void TearDown() override {
        push_engine_.reset();
        storage_pool_.reset();
        storage_client_.reset();
        storage_server_.reset();
        storage_service_.reset();
        chunk_store_.reset();
        storage_root_.reset();
        metadata_client_.reset();
        metadata_server_.reset();
        metadata_service_.reset();

        if (repository_.has_value()) {
            cleanup_owned_rows();
            repository_.reset();
        }

        source_dir_.reset();
    }

    void cleanup_owned_rows() {
        pqxx::connection connection{test_database_connection_string()};
        pqxx::work transaction{connection};

        transaction.exec("DELETE FROM upload_sessions WHERE session_id = $1::uuid", pqxx::params{session_id_.str()})
            .no_rows();
        transaction.exec("DELETE FROM storage_locations WHERE node_id LIKE 'm4s4-%'").no_rows();

        for (const std::string& chunk_id : owned_chunk_ids_) {
            transaction.exec("DELETE FROM storage_locations WHERE chunk_id = $1", pqxx::params{chunk_id}).no_rows();
            transaction.exec("DELETE FROM chunks WHERE chunk_id = $1", pqxx::params{chunk_id}).no_rows();
        }

        transaction.exec("DELETE FROM artifacts WHERE artifact_id = $1::uuid", pqxx::params{artifact_id_.str()})
            .no_rows();
        transaction.commit();
    }

    void track_chunk(std::string chunk_id) { owned_chunk_ids_.push_back(std::move(chunk_id)); }

    void create_open_session(std::uint64_t chunk_size_bytes, std::string_view target_node = kTargetNode) {
        const UploadSession session{
            session_id_,      artifact_id_, std::string{target_node}, ChunkingStrategy::FixedSize,
            chunk_size_bytes, std::nullopt, {{"source", "m4s4"}},     UploadSessionState::Open,
            std::nullopt};
        metadata_client_->register_storage_node(aistore::metadata::StorageNode{
            .node_id = session.target_node_id(),
            .address = "127.0.0.1",
            .port = 8081,
            .state = aistore::metadata::StorageNodeState::Active,
        });
        (void)metadata_client_->create_upload_session(session);
    }

    void create_open_fastcdc_session(const FastCdcParameters& parameters, std::string_view target_node = kTargetNode) {
        const UploadSession session{session_id_,
                                    artifact_id_,
                                    std::string{target_node},
                                    parameters,
                                    std::nullopt,
                                    {{"source", "m6-fastcdc"}},
                                    UploadSessionState::Open,
                                    std::nullopt};
        metadata_client_->register_storage_node(aistore::metadata::StorageNode{
            .node_id = session.target_node_id(),
            .address = "127.0.0.1",
            .port = 8081,
            .state = aistore::metadata::StorageNodeState::Active,
        });
        (void)metadata_client_->create_upload_session(session);
    }

    PushEngine& engine() {
        if (!push_engine_.has_value()) {
            push_engine_.emplace(*metadata_client_, *storage_pool_);
        }
        return *push_engine_;
    }

    UuidV7 artifact_id_{UuidV7::generate()};
    UuidV7 session_id_{UuidV7::generate()};
    std::string unique_tag_;
    std::vector<std::string> owned_chunk_ids_;

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

    std::optional<TemporaryDirectory> source_dir_;
    std::optional<PushEngine> push_engine_;
};

TEST_F(PushEngineFixture, PushesNewMultiChunkFileAndBuildsDescriptor) {
    create_open_session(4);
    const std::string contents = std::string{"abcdefghij"} + unique_tag_.substr(0, 2);
    const auto source = write_temp_file(source_dir_->path(), "multi.bin", contents);

    aistore::hashing::Sha256 hasher;
    hasher.update(std::as_bytes(std::span{contents.data(), contents.size()}));
    const std::string expected_object_id = digest_to_hex(hasher.finalize());

    const PreparedPush prepared = engine().push(PushRequest{.source_path = source, .session_id = session_id_});

    EXPECT_EQ(prepared.stats.bytes_read, contents.size());
    EXPECT_EQ(prepared.layout_descriptor.object_id(), expected_object_id);
    EXPECT_EQ(prepared.layout_descriptor.object().total_size(), contents.size());

    const auto& refs = prepared.layout_descriptor.layout().chunks();
    ASSERT_FALSE(refs.empty());

    std::uint64_t expected_offset = 0;
    for (const auto& ref : refs) {
        EXPECT_EQ(ref.offset, expected_offset);
        EXPECT_GT(ref.size, 0U);
        track_chunk(ref.chunk_id);
        EXPECT_TRUE(chunk_store_->contains(ref.chunk_id));

        const auto locations = repository_->get_storage_locations(ref.chunk_id);
        ASSERT_FALSE(locations.empty());
        bool found_target = false;
        for (const auto& location : locations) {
            if (location.node_id == kTargetNode) {
                found_target = true;
                EXPECT_EQ(location.state, StorageLocationState::Available);
                EXPECT_EQ(location.storage_path, std::string{"/v1/chunks/"} + ref.chunk_id);
            }
        }
        EXPECT_TRUE(found_target);
        expected_offset += ref.size;
    }

    EXPECT_EQ(expected_offset, contents.size());
    EXPECT_EQ(prepared.stats.total_chunks, refs.size());
    EXPECT_EQ(prepared.stats.put_requests, prepared.stats.unique_chunks);
    EXPECT_EQ(prepared.stats.bytes_sent_to_storage, contents.size());
}

TEST_F(PushEngineFixture, PushesEmptyFileWithoutChunks) {
    create_open_session(4);
    const auto source = write_temp_file(source_dir_->path(), "empty.bin", "");

    const PreparedPush prepared = engine().push(PushRequest{.source_path = source, .session_id = session_id_});

    EXPECT_EQ(prepared.layout_descriptor.object_id(), sha256_hex(""));
    EXPECT_EQ(prepared.layout_descriptor.object().total_size(), 0U);
    EXPECT_TRUE(prepared.layout_descriptor.layout().empty());
    EXPECT_EQ(prepared.stats.total_chunks, 0U);
    EXPECT_EQ(prepared.stats.unique_chunks, 0U);
    EXPECT_EQ(prepared.stats.put_requests, 0U);
    EXPECT_EQ(prepared.stats.bytes_sent_to_storage, 0U);
}

TEST_F(PushEngineFixture, ReusesVerifiedTargetChunkWithoutPut) {
    create_open_session(4);
    const std::string contents = "abcd";
    const std::string chunk_id = sha256_hex(contents);
    track_chunk(chunk_id);

    repository_->register_chunks({ChunkMetadata{.chunk_id = chunk_id, .size_bytes = 4}});
    chunk_store_->put(chunk_id, std::as_bytes(std::span{contents.data(), contents.size()}));
    repository_->register_storage_location(StorageLocation{
        .chunk_id = chunk_id,
        .node_id = std::string{kTargetNode},
        .storage_path = std::string{"/v1/chunks/"} + chunk_id,
        .state = StorageLocationState::Available,
    });

    const auto source = write_temp_file(source_dir_->path(), "verified.bin", contents);
    const PreparedPush prepared = engine().push(PushRequest{.source_path = source, .session_id = session_id_});

    EXPECT_EQ(prepared.stats.verified_target_chunks, 1U);
    EXPECT_EQ(prepared.stats.put_requests, 0U);
    EXPECT_EQ(prepared.stats.repaired_target_chunks, 0U);
}

TEST_F(PushEngineFixture, RepairsStaleAvailableTargetLocation) {
    create_open_session(4);
    const std::string contents = "efgh";
    const std::string chunk_id = sha256_hex(contents);
    track_chunk(chunk_id);

    repository_->register_chunks({ChunkMetadata{.chunk_id = chunk_id, .size_bytes = 4}});
    repository_->register_storage_location(StorageLocation{
        .chunk_id = chunk_id,
        .node_id = std::string{kTargetNode},
        .storage_path = "/old/stale/path",
        .state = StorageLocationState::Available,
    });

    const auto source = write_temp_file(source_dir_->path(), "repair.bin", contents);
    const PreparedPush prepared = engine().push(PushRequest{.source_path = source, .session_id = session_id_});

    EXPECT_EQ(prepared.stats.put_requests, 1U);
    EXPECT_EQ(prepared.stats.repaired_target_chunks, 1U);
    EXPECT_TRUE(chunk_store_->contains(chunk_id));

    const auto locations = repository_->get_storage_locations(chunk_id);
    ASSERT_EQ(locations.size(), 1U);
    EXPECT_EQ(locations[0].storage_path, std::string{"/v1/chunks/"} + chunk_id);
    EXPECT_EQ(locations[0].state, StorageLocationState::Available);
}

TEST_F(PushEngineFixture, CopiesAvailableElsewhereToRequiredTarget) {
    create_open_session(4);
    const std::string contents = "ijkl";
    const std::string chunk_id = sha256_hex(contents);
    track_chunk(chunk_id);

    repository_->register_chunks({ChunkMetadata{.chunk_id = chunk_id, .size_bytes = 4}});
    repository_->register_storage_location(StorageLocation{
        .chunk_id = chunk_id,
        .node_id = std::string{kOtherNode},
        .storage_path = "/elsewhere/path",
        .state = StorageLocationState::Available,
    });

    const auto source = write_temp_file(source_dir_->path(), "elsewhere.bin", contents);
    const PreparedPush prepared = engine().push(PushRequest{.source_path = source, .session_id = session_id_});

    EXPECT_EQ(prepared.stats.put_requests, 1U);
    EXPECT_EQ(prepared.stats.repaired_target_chunks, 0U);
    EXPECT_TRUE(chunk_store_->contains(chunk_id));

    const auto locations = repository_->get_storage_locations(chunk_id);
    ASSERT_EQ(locations.size(), 2U);

    bool saw_other = false;
    bool saw_target = false;
    for (const auto& location : locations) {
        if (location.node_id == kOtherNode) {
            saw_other = true;
            EXPECT_EQ(location.state, StorageLocationState::Available);
        }
        if (location.node_id == kTargetNode) {
            saw_target = true;
            EXPECT_EQ(location.storage_path, std::string{"/v1/chunks/"} + chunk_id);
            EXPECT_EQ(location.state, StorageLocationState::Available);
        }
    }
    EXPECT_TRUE(saw_other);
    EXPECT_TRUE(saw_target);
}

TEST_F(PushEngineFixture, UploadsKnownChunkWithNoAvailableLocation) {
    create_open_session(4);
    const std::string contents = "mnop";
    const std::string chunk_id = sha256_hex(contents);
    track_chunk(chunk_id);

    repository_->register_chunks({ChunkMetadata{.chunk_id = chunk_id, .size_bytes = 4}});

    const auto source = write_temp_file(source_dir_->path(), "known.bin", contents);
    const PreparedPush prepared = engine().push(PushRequest{.source_path = source, .session_id = session_id_});

    EXPECT_EQ(prepared.stats.put_requests, 1U);
    EXPECT_TRUE(chunk_store_->contains(chunk_id));

    const auto locations = repository_->get_storage_locations(chunk_id);
    ASSERT_EQ(locations.size(), 1U);
    EXPECT_EQ(locations[0].node_id, kTargetNode);
    EXPECT_EQ(locations[0].state, StorageLocationState::Available);
}

TEST_F(PushEngineFixture, DuplicateChunkContentUploadsUniqueChunkOnce) {
    create_open_session(4);
    const std::string contents = "xyzwxyzw";
    const auto source = write_temp_file(source_dir_->path(), "dup.bin", contents);

    const PreparedPush prepared = engine().push(PushRequest{.source_path = source, .session_id = session_id_});

    ASSERT_EQ(prepared.layout_descriptor.layout().chunks().size(), 2U);
    EXPECT_EQ(prepared.layout_descriptor.layout().chunks()[0].offset, 0U);
    EXPECT_EQ(prepared.layout_descriptor.layout().chunks()[1].offset, 4U);
    EXPECT_EQ(prepared.layout_descriptor.layout().chunks()[0].chunk_id,
              prepared.layout_descriptor.layout().chunks()[1].chunk_id);
    EXPECT_EQ(prepared.stats.total_chunks, 2U);
    EXPECT_EQ(prepared.stats.unique_chunks, 1U);
    EXPECT_EQ(prepared.stats.put_requests, 1U);

    track_chunk(prepared.layout_descriptor.layout().chunks()[0].chunk_id);
}

TEST_F(PushEngineFixture, RetrySameOpenSessionReusesUploadedChunks) {
    create_open_session(4);
    const std::string contents = std::string{"retry"} + unique_tag_.substr(0, 3);
    const auto source = write_temp_file(source_dir_->path(), "retry.bin", contents);

    const PreparedPush first = engine().push(PushRequest{.source_path = source, .session_id = session_id_});
    for (const auto& ref : first.layout_descriptor.layout().chunks()) {
        track_chunk(ref.chunk_id);
    }

    const PreparedPush second = engine().push(PushRequest{.source_path = source, .session_id = session_id_});

    EXPECT_EQ(second.layout_descriptor.object_id(), first.layout_descriptor.object_id());
    EXPECT_EQ(second.layout_descriptor.layout_id(), first.layout_descriptor.layout_id());
    EXPECT_EQ(second.stats.put_requests, 0U);
    EXPECT_EQ(second.stats.verified_target_chunks, first.stats.unique_chunks);

    const auto session = metadata_client_->get_upload_session(session_id_);
    ASSERT_TRUE(session.has_value());
    EXPECT_EQ(session->state(), UploadSessionState::Open);
}

TEST_F(PushEngineFixture, WorkerFailurePropagatesAndSessionRemainsOpen) {
    create_open_session(4);

    RunningHttpServer failing_storage{[](const HttpRequest& request) {
        if (request.method() == beast_http::verb::put) {
            HttpResponse response{beast_http::status::internal_server_error, request.version()};
            response.set(beast_http::field::content_type, "application/json");
            response.body() = R"({"error":"injected_storage_failure"})";
            response.prepare_payload();
            return response;
        }

        HttpResponse response{beast_http::status::not_found, request.version()};
        response.set(beast_http::field::content_type, "application/json");
        response.body() = R"({"error":"chunk_not_found"})";
        response.prepare_payload();
        return response;
    }};

    StorageNodeClient failing_client{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = failing_storage.port()},
    }};
    StorageNodeClientPool failing_pool{std::vector<std::pair<std::string, StorageNodeClient>>{
        {std::string{kTargetNode}, std::move(failing_client)},
    }};
    PushEngine failing_engine{*metadata_client_, failing_pool};

    const std::string contents = "fail";
    const auto source = write_temp_file(source_dir_->path(), "fail.bin", contents);
    track_chunk(sha256_hex(contents));

    try {
        (void)failing_engine.push(PushRequest{.source_path = source, .session_id = session_id_});
        FAIL() << "expected RemoteApiError";
    } catch (const RemoteApiError& error) {
        EXPECT_EQ(error.status_code(), 500U);
        EXPECT_EQ(error.error_code(), "injected_storage_failure");
    }

    const auto session = metadata_client_->get_upload_session(session_id_);
    ASSERT_TRUE(session.has_value());
    EXPECT_EQ(session->state(), UploadSessionState::Open);
}

TEST_F(PushEngineFixture, RejectsStorageClientNodeMismatchBeforeStorageIo) {
    create_open_session(4, "m4s4-target-a");

    StorageNodeClient unused_client{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = 1},
    }};
    StorageNodeClientPool mismatched_pool{std::vector<std::pair<std::string, StorageNodeClient>>{
        {"m4s4-target-b", std::move(unused_client)},
    }};
    PushEngine mismatched{*metadata_client_, mismatched_pool};

    const auto source = write_temp_file(source_dir_->path(), "mismatch.bin", "abcd");
    EXPECT_THROW((void)mismatched.push(PushRequest{.source_path = source, .session_id = session_id_}),
                 std::invalid_argument);
}

TEST_F(PushEngineFixture, RejectsChunkSizeAboveM4StorageLimit) {
    const UploadSession session{session_id_,
                                artifact_id_,
                                std::string{kTargetNode},
                                ChunkingStrategy::FixedSize,
                                PushEngine::kMaxM4ChunkSize + 1U,
                                std::nullopt,
                                {},
                                UploadSessionState::Open,
                                std::nullopt};
    metadata_client_->register_storage_node(aistore::metadata::StorageNode{
        .node_id = session.target_node_id(),
        .address = "127.0.0.1",
        .port = 8081,
        .state = aistore::metadata::StorageNodeState::Active,
    });
    (void)metadata_client_->create_upload_session(session);

    const auto source = write_temp_file(source_dir_->path(), "toolarge.bin", "abcd");
    EXPECT_THROW((void)engine().push(PushRequest{.source_path = source, .session_id = session_id_}),
                 std::invalid_argument);
}

TEST(PushEngineRecoveryTest, PrepareCommittedRetryReconstructsDescriptorWithoutRemoteIO) {
    constexpr std::string_view kRecoveryNode = "m4s6-recovery-node";

    MetadataClient unused_metadata{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = 1},
    }};
    StorageNodeClient unused_storage{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = 2},
    }};
    StorageNodeClientPool unused_pool{std::vector<std::pair<std::string, StorageNodeClient>>{
        {std::string{kRecoveryNode}, std::move(unused_storage)},
    }};

    PushEngine engine{unused_metadata, unused_pool};

    TemporaryDirectory source_dir;
    const std::string contents = "ABCDABCDEFGH";
    const auto source = write_temp_file(source_dir.path(), "recovery.bin", contents);

    const UuidV7 session_id = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const std::string finalized_version_id = std::string(64, 'a');

    const UploadSession committed_session{
        session_id,   artifact_id, std::string{kRecoveryNode},    ChunkingStrategy::FixedSize, 4U,
        std::nullopt, {},          UploadSessionState::Committed, finalized_version_id};

    const PreparedPush prepared =
        engine.prepare_committed_retry(PushRequest{.source_path = source, .session_id = session_id}, committed_session);

    EXPECT_EQ(prepared.session_id, session_id);
    EXPECT_EQ(prepared.stats.bytes_read, 12U);
    EXPECT_EQ(prepared.stats.total_chunks, 3U);
    EXPECT_EQ(prepared.stats.unique_chunks, 2U);
    EXPECT_EQ(prepared.stats.put_requests, 0U);
    EXPECT_EQ(prepared.stats.verified_target_chunks, 0U);
    EXPECT_EQ(prepared.stats.repaired_target_chunks, 0U);
    EXPECT_EQ(prepared.stats.bytes_sent_to_storage, 0U);

    const auto& refs = prepared.layout_descriptor.layout().chunks();
    ASSERT_EQ(refs.size(), 3U);
    EXPECT_EQ(refs[0].offset, 0U);
    EXPECT_EQ(refs[1].offset, 4U);
    EXPECT_EQ(refs[2].offset, 8U);
    EXPECT_EQ(refs[0].size, 4U);
    EXPECT_EQ(refs[1].size, 4U);
    EXPECT_EQ(refs[2].size, 4U);
    EXPECT_EQ(refs[0].chunk_id, refs[1].chunk_id);
    EXPECT_NE(refs[2].chunk_id, refs[0].chunk_id);
    EXPECT_EQ(refs[0].chunk_id, sha256_hex("ABCD"));
    EXPECT_EQ(refs[2].chunk_id, sha256_hex("EFGH"));
    EXPECT_EQ(prepared.layout_descriptor.object_id(), sha256_hex(contents));
    EXPECT_EQ(prepared.layout_descriptor.object().total_size(), 12U);
}

TEST_F(PushEngineFixture, PushesFastCdcFileAndBuildsVariableLayout) {
    create_open_fastcdc_session(kTestFastCdcParams);
    const std::vector<std::byte> fixture = make_fastcdc_golden_fixture();
    const auto source = write_temp_bytes(source_dir_->path(), "fastcdc.bin", fixture);

    const PreparedPush prepared = engine().push(PushRequest{.source_path = source, .session_id = session_id_});

    EXPECT_EQ(prepared.stats.bytes_read, fixture.size());
    EXPECT_EQ(prepared.layout_descriptor.chunking_strategy(), ChunkingStrategy::FastCdc);
    ASSERT_TRUE(prepared.layout_descriptor.fastcdc_parameters().has_value());
    EXPECT_EQ(*prepared.layout_descriptor.fastcdc_parameters(), kTestFastCdcParams);

    const auto expected_chunks = chunk_bytes_fastcdc(fixture, kTestFastCdcParams);
    ASSERT_FALSE(expected_chunks.empty());
    ASSERT_EQ(prepared.layout_descriptor.layout().chunks().size(), expected_chunks.size());

    std::size_t non_final_count = 0;
    std::size_t non_avg_count = 0;
    for (std::size_t index = 0; index < expected_chunks.size(); ++index) {
        const auto& ref = prepared.layout_descriptor.layout().chunks()[index];
        const auto& expected = expected_chunks[index];

        EXPECT_EQ(ref.offset, expected.offset);
        EXPECT_EQ(ref.size, expected.bytes.size());
        EXPECT_EQ(ref.chunk_id, sha256_hex(expected.bytes));
        track_chunk(ref.chunk_id);

        if (index + 1U < expected_chunks.size()) {
            ++non_final_count;
            if (ref.size != kTestFastCdcParams.avg_chunk_size_bytes) {
                ++non_avg_count;
            }
        }
    }

    EXPECT_GT(non_final_count, 0U);
    EXPECT_GT(non_avg_count, 0U);
    EXPECT_EQ(prepared.stats.put_requests, prepared.stats.unique_chunks);
}

TEST_F(PushEngineFixture, FastCdcPushReusesKnownChunks) {
    create_open_fastcdc_session(kTestFastCdcParams);
    const std::vector<std::byte> fixture = make_fastcdc_golden_fixture();
    const std::vector<ChunkBuffer> expected_chunks = chunk_bytes_fastcdc(fixture, kTestFastCdcParams);
    ASSERT_GE(expected_chunks.size(), 2U);

    const std::string chunk_id = sha256_hex(expected_chunks.front().bytes);
    track_chunk(chunk_id);

    repository_->register_chunks(
        {ChunkMetadata{.chunk_id = chunk_id, .size_bytes = expected_chunks.front().bytes.size()}});
    chunk_store_->put(chunk_id, expected_chunks.front().bytes);
    repository_->register_storage_location(StorageLocation{
        .chunk_id = chunk_id,
        .node_id = std::string{kTargetNode},
        .storage_path = std::string{"/v1/chunks/"} + chunk_id,
        .state = StorageLocationState::Available,
    });

    const auto source = write_temp_bytes(source_dir_->path(), "fastcdc-reuse.bin", fixture);
    const PreparedPush prepared = engine().push(PushRequest{.source_path = source, .session_id = session_id_});

    EXPECT_EQ(prepared.stats.verified_target_chunks, 1U);
    EXPECT_LT(prepared.stats.put_requests, prepared.stats.unique_chunks);
}

TEST(PushEngineRecoveryTest, FastCdcCommittedRetryReconstructsExactDescriptorWithoutRemoteIo) {
    constexpr std::string_view kRecoveryNode = "m6-fastcdc-recovery-node";

    MetadataClient unused_metadata{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = 1},
    }};
    StorageNodeClient unused_storage{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = 2},
    }};
    StorageNodeClientPool unused_pool{std::vector<std::pair<std::string, StorageNodeClient>>{
        {std::string{kRecoveryNode}, std::move(unused_storage)},
    }};

    PushEngine engine{unused_metadata, unused_pool};

    TemporaryDirectory source_dir;
    const std::vector<std::byte> fixture = make_fastcdc_golden_fixture();
    const auto source = write_temp_bytes(source_dir.path(), "fastcdc-recovery.bin", fixture);

    const UuidV7 session_id = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const std::string finalized_version_id = std::string(64, 'b');

    const UploadSession committed_session{
        session_id,   artifact_id, std::string{kRecoveryNode},    kTestFastCdcParams,
        std::nullopt, {},          UploadSessionState::Committed, finalized_version_id};

    const PreparedPush prepared =
        engine.prepare_committed_retry(PushRequest{.source_path = source, .session_id = session_id}, committed_session);

    EXPECT_EQ(prepared.session_id, session_id);
    EXPECT_EQ(prepared.stats.bytes_read, fixture.size());
    EXPECT_EQ(prepared.stats.put_requests, 0U);
    EXPECT_EQ(prepared.stats.verified_target_chunks, 0U);
    EXPECT_EQ(prepared.stats.repaired_target_chunks, 0U);
    EXPECT_EQ(prepared.stats.bytes_sent_to_storage, 0U);
    EXPECT_EQ(prepared.layout_descriptor.chunking_strategy(), ChunkingStrategy::FastCdc);
    ASSERT_TRUE(prepared.layout_descriptor.fastcdc_parameters().has_value());
    EXPECT_EQ(*prepared.layout_descriptor.fastcdc_parameters(), kTestFastCdcParams);

    const auto expected_chunks = chunk_bytes_fastcdc(fixture, kTestFastCdcParams);
    const auto& refs = prepared.layout_descriptor.layout().chunks();
    ASSERT_EQ(refs.size(), expected_chunks.size());

    for (std::size_t index = 0; index < refs.size(); ++index) {
        EXPECT_EQ(refs[index].offset, expected_chunks[index].offset);
        EXPECT_EQ(refs[index].size, expected_chunks[index].bytes.size());
        EXPECT_EQ(refs[index].chunk_id, sha256_hex(expected_chunks[index].bytes));
    }

    EXPECT_EQ(prepared.layout_descriptor.object_id(), sha256_hex(fixture));
    EXPECT_EQ(prepared.layout_descriptor.object().total_size(), fixture.size());
}

TEST_F(PushEngineFixture, FastCdcPushRemainsOnePassAndBounded) {
    create_open_fastcdc_session(kTestFastCdcParams);
    const std::vector<std::byte> fixture = make_fastcdc_golden_fixture();
    const auto source = write_temp_bytes(source_dir_->path(), "fastcdc-bounded.bin", fixture);

    std::atomic<int> in_flight{0};
    std::atomic<int> max_in_flight{0};

    RunningHttpServer instrumented_storage{[&](const HttpRequest& request) {
        if (request.method() == beast_http::verb::put) {
            const int current = in_flight.fetch_add(1, std::memory_order_relaxed) + 1;
            int observed_max = max_in_flight.load(std::memory_order_relaxed);
            while (current > observed_max &&
                   !max_in_flight.compare_exchange_weak(observed_max, current, std::memory_order_relaxed)) {
            }

            HttpResponse response = storage_service_->handle_request(request);
            in_flight.fetch_sub(1, std::memory_order_relaxed);
            return response;
        }

        return storage_service_->handle_request(request);
    }};

    StorageNodeClient instrumented_client{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = instrumented_storage.port()},
    }};
    StorageNodeClientPool instrumented_pool{std::vector<std::pair<std::string, StorageNodeClient>>{
        {std::string{kTargetNode}, std::move(instrumented_client)},
    }};
    PushEngine instrumented_engine{*metadata_client_, instrumented_pool};

    const PreparedPush prepared =
        instrumented_engine.push(PushRequest{.source_path = source, .session_id = session_id_});

    for (const auto& ref : prepared.layout_descriptor.layout().chunks()) {
        track_chunk(ref.chunk_id);
    }

    EXPECT_EQ(prepared.stats.bytes_read, fixture.size());
    EXPECT_LE(max_in_flight.load(), static_cast<int>(PushEngine::kWorkerCount));
    EXPECT_LE(max_in_flight.load(), static_cast<int>(PushEngine::kQueueCapacity));
}

class MultiNodePushEngineFixture : public ::testing::Test {
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
            transaction
                .exec(
                    "DELETE FROM upload_session_nodes WHERE session_id IN "
                    "(SELECT session_id FROM upload_sessions WHERE state = 'open')")
                .no_rows();
            transaction.exec("DELETE FROM upload_sessions WHERE state = 'open'").no_rows();
            transaction.exec("DELETE FROM replication_runs").no_rows();
            transaction.exec("DELETE FROM gc_runs").no_rows();
            transaction.exec("DELETE FROM storage_nodes").no_rows();
            transaction.commit();
        }
        repository_->create_artifact(
            Artifact{artifact_id_, std::string{"m8-push-artifact-"} + artifact_id_.str(), "m8-push-project"});

        metadata_service_.emplace(*repository_);
        metadata_server_.emplace(
            [&](const HttpRequest& request) { return metadata_service_->handle_request(request); });
        metadata_client_.emplace(HttpClientConfig{
            .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = metadata_server_->port()},
        });

        node_a_ = std::make_unique<NodeStack>("m8-push-a");
        node_b_ = std::make_unique<NodeStack>("m8-push-b");
        node_c_ = std::make_unique<NodeStack>("m8-push-c");

        for (NodeStack* node : {node_a_.get(), node_b_.get(), node_c_.get()}) {
            metadata_client_->register_storage_node(StorageNode{
                .node_id = node->node_id,
                .address = "127.0.0.1",
                .port = node->server.port(),
                .state = StorageNodeState::Active,
            });
        }

        storage_pool_.emplace(std::vector<std::pair<std::string, StorageNodeClient>>{
            {"m8-push-a", node_a_->client},
            {"m8-push-b", node_b_->client},
            {"m8-push-c", node_c_->client},
        });

        source_dir_.emplace();
    }

    void TearDown() override {
        push_engine_.reset();
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

        source_dir_.reset();
    }

    void cleanup_owned_rows() {
        pqxx::connection connection{test_database_connection_string()};
        pqxx::work transaction{connection};

        transaction
            .exec("DELETE FROM upload_session_finalizations WHERE session_id = $1::uuid",
                  pqxx::params{session_id_.str()})
            .no_rows();
        transaction
            .exec("DELETE FROM upload_session_metadata WHERE session_id = $1::uuid", pqxx::params{session_id_.str()})
            .no_rows();
        transaction
            .exec("DELETE FROM upload_session_nodes WHERE session_id = $1::uuid", pqxx::params{session_id_.str()})
            .no_rows();
        transaction.exec("DELETE FROM upload_sessions WHERE session_id = $1::uuid", pqxx::params{session_id_.str()})
            .no_rows();
        transaction.exec("DELETE FROM replication_runs").no_rows();
        transaction.exec("DELETE FROM gc_runs").no_rows();
        transaction.exec("DELETE FROM storage_locations WHERE node_id LIKE 'm8-push-%'").no_rows();

        for (const std::string& chunk_id : owned_chunk_ids_) {
            transaction.exec("DELETE FROM object_layout_chunks WHERE chunk_id = $1", pqxx::params{chunk_id}).no_rows();
            transaction.exec("DELETE FROM storage_locations WHERE chunk_id = $1", pqxx::params{chunk_id}).no_rows();
            transaction.exec("DELETE FROM chunks WHERE chunk_id = $1", pqxx::params{chunk_id}).no_rows();
        }

        transaction.exec("DELETE FROM storage_nodes WHERE node_id LIKE 'm8-push-%'").no_rows();
        transaction.exec("DELETE FROM artifacts WHERE artifact_id = $1::uuid", pqxx::params{artifact_id_.str()})
            .no_rows();
        transaction.commit();
    }

    void track_chunk(std::string chunk_id) { owned_chunk_ids_.push_back(std::move(chunk_id)); }

    void create_open_multi_node_session() {
        const std::vector<std::string> placement{"m8-push-a", "m8-push-b", "m8-push-c"};
        const UploadSession session{session_id_,
                                    artifact_id_,
                                    2U,
                                    placement,
                                    ChunkingStrategy::FixedSize,
                                    4U,
                                    std::nullopt,
                                    {{"source", "m8-push"}},
                                    UploadSessionState::Open,
                                    std::nullopt};
        try {
            (void)metadata_client_->create_upload_session(session);
        } catch (const RemoteApiError& error) {
            if (error.status_code() == 409U && error.error_code() == "upload_session_conflict") {
                const std::optional<UploadSession> existing = metadata_client_->get_upload_session(session_id_);
                if (!existing.has_value() || existing->state() != UploadSessionState::Open) {
                    FAIL() << "upload session conflict with non-open session: " << error.response_body();
                }
                return;
            }

            FAIL() << "create_upload_session failed: status=" << error.status_code() << " code=" << error.error_code()
                   << " body=" << error.response_body();
        }
    }

    PushEngine& engine() {
        if (!push_engine_.has_value()) {
            push_engine_.emplace(*metadata_client_, *storage_pool_);
        }
        return *push_engine_;
    }

    [[nodiscard]] NodeStack* node_for(std::string_view node_id) {
        if (node_id == "m8-push-a") {
            return node_a_.get();
        }
        if (node_id == "m8-push-b") {
            return node_b_.get();
        }
        return node_c_.get();
    }

    UuidV7 artifact_id_{UuidV7::generate()};
    UuidV7 session_id_{UuidV7::generate()};
    std::vector<std::string> owned_chunk_ids_;

    std::optional<PostgresMetadataRepository> repository_;
    std::optional<MetadataService> metadata_service_;
    std::optional<RunningHttpServer> metadata_server_;
    std::optional<MetadataClient> metadata_client_;

    std::unique_ptr<NodeStack> node_a_;
    std::unique_ptr<NodeStack> node_b_;
    std::unique_ptr<NodeStack> node_c_;
    std::optional<StorageNodeClientPool> storage_pool_;
    std::optional<TemporaryDirectory> source_dir_;
    std::optional<PushEngine> push_engine_;
};

TEST_F(MultiNodePushEngineFixture, MultiNodePushReusesExistingDesiredReplica) {
    create_open_multi_node_session();
    const std::string contents = "WXYZWXYZUVUV";
    const auto source = write_temp_file(source_dir_->path(), "reuse.bin", contents);

    const std::string chunk_a = sha256_hex("WXYZ");
    track_chunk(chunk_a);
    const std::vector<std::string> placement{"m8-push-a", "m8-push-b", "m8-push-c"};
    const std::vector<std::string> desired = select_replica_nodes(chunk_a, placement, 2U);
    NodeStack* seeded_node = node_for(desired.front());
    seeded_node->store.put(chunk_a, std::as_bytes(std::span{contents.data(), 4U}));
    repository_->register_chunks({ChunkMetadata{.chunk_id = chunk_a, .size_bytes = 4U}});
    metadata_client_->register_storage_location(StorageLocation{
        .chunk_id = chunk_a,
        .node_id = seeded_node->node_id,
        .storage_path = std::string{"/v1/chunks/"} + chunk_a,
        .state = StorageLocationState::Available,
    });

    const PreparedPush prepared = engine().push(PushRequest{.source_path = source, .session_id = session_id_});

    for (const ChunkRef& chunk : prepared.layout_descriptor.layout().chunks()) {
        track_chunk(chunk.chunk_id);
    }

    EXPECT_GE(prepared.stats.verified_target_chunks, 1U);
    EXPECT_LT(prepared.stats.put_requests, 4U);
}

TEST_F(MultiNodePushEngineFixture, RfTwoPushWritesEveryDesiredReplica) {
    create_open_multi_node_session();
    const std::string contents = "ABCDABCDEFGH";
    const auto source = write_temp_file(source_dir_->path(), "rf2.bin", contents);

    const PreparedPush prepared = engine().push(PushRequest{.source_path = source, .session_id = session_id_});

    EXPECT_EQ(prepared.stats.unique_chunks, 2U);
    EXPECT_EQ(prepared.stats.put_requests, 4U);

    const std::vector<std::string> placement{"m8-push-a", "m8-push-b", "m8-push-c"};
    for (const auto& ref : prepared.layout_descriptor.layout().chunks()) {
        track_chunk(ref.chunk_id);
        const std::vector<std::string> desired = select_replica_nodes(ref.chunk_id, placement, 2U);
        for (const std::string& node_id : desired) {
            EXPECT_TRUE(node_for(node_id)->client.has_chunk(ref.chunk_id));
        }
    }
}

TEST_F(MultiNodePushEngineFixture, MultiNodePushFailsWhenOneDesiredReplicaCannotBeWritten) {
    create_open_multi_node_session();

    RunningHttpServer failing_storage{[](const HttpRequest& request) {
        if (request.method() == beast_http::verb::put) {
            HttpResponse response{beast_http::status::internal_server_error, request.version()};
            response.set(beast_http::field::content_type, "application/json");
            response.body() = R"({"error":"injected_storage_failure"})";
            response.prepare_payload();
            return response;
        }

        HttpResponse response{beast_http::status::not_found, request.version()};
        response.set(beast_http::field::content_type, "application/json");
        response.body() = R"({"error":"chunk_not_found"})";
        response.prepare_payload();
        return response;
    }};

    StorageNodeClient failing_client{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = failing_storage.port()},
    }};
    StorageNodeClientPool failing_pool{std::vector<std::pair<std::string, StorageNodeClient>>{
        {"m8-push-a", node_a_->client},
        {"m8-push-b", std::move(failing_client)},
        {"m8-push-c", node_c_->client},
    }};
    PushEngine failing_engine{*metadata_client_, failing_pool};

    const auto source = write_temp_file(source_dir_->path(), "fail.bin", "ABCDABCDEFGH");

    EXPECT_THROW((void)failing_engine.push(PushRequest{.source_path = source, .session_id = session_id_}),
                 RemoteApiError);
}

TEST(MultiNodePushEngineRecoveryTest, CommittedMultiNodeRetryPerformsZeroStorageIo) {
    MetadataClient unused_metadata{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = 1},
    }};
    StorageNodeClient unused_a{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = 2},
    }};
    StorageNodeClient unused_b{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = 3},
    }};
    StorageNodeClient unused_c{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = 4},
    }};
    StorageNodeClientPool unused_pool{std::vector<std::pair<std::string, StorageNodeClient>>{
        {"m8-push-a", std::move(unused_a)},
        {"m8-push-b", std::move(unused_b)},
        {"m8-push-c", std::move(unused_c)},
    }};

    PushEngine engine{unused_metadata, unused_pool};

    TemporaryDirectory source_dir;
    const std::string contents = "ABCDABCDEFGH";
    const auto source = write_temp_file(source_dir.path(), "committed.bin", contents);

    const UuidV7 session_id = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const std::vector<std::string> placement{"m8-push-a", "m8-push-b", "m8-push-c"};

    const UploadSession committed_session{session_id,
                                          artifact_id,
                                          2U,
                                          placement,
                                          ChunkingStrategy::FixedSize,
                                          4U,
                                          std::nullopt,
                                          {},
                                          UploadSessionState::Committed,
                                          std::string(64, 'a')};

    const PreparedPush prepared =
        engine.prepare_committed_retry(PushRequest{.source_path = source, .session_id = session_id}, committed_session);

    EXPECT_EQ(prepared.stats.put_requests, 0U);
    EXPECT_EQ(prepared.stats.bytes_sent_to_storage, 0U);
    EXPECT_EQ(prepared.stats.verified_target_chunks, 0U);
}

}  // namespace
