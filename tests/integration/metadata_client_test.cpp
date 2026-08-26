#include "aistore/client/metadata_client.hpp"

#include <gtest/gtest.h>

#include <array>
#include <boost/json.hpp>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <pqxx/pqxx>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "aistore/client/client_error.hpp"
#include "aistore/hashing/sha256.hpp"
#include "aistore/http/http_client.hpp"
#include "aistore/http/http_server.hpp"
#include "aistore/metadata/artifact_model.hpp"
#include "aistore/metadata/chunk_metadata.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/postgres_metadata_repository.hpp"
#include "aistore/metadata/storage_location.hpp"
#include "aistore/metadata/upload_session.hpp"
#include "aistore/metadata/uuid_v7.hpp"
#include "aistore/service/metadata_service.hpp"

namespace {

using aistore::client::ChunkAvailability;
using aistore::client::MetadataClient;
using aistore::client::RemoteApiError;
using aistore::client::RemoteProtocolError;
using aistore::http::HttpClientConfig;
using aistore::http::HttpEndpoint;
using aistore::http::HttpRequest;
using aistore::http::HttpResponse;
using aistore::http::HttpServer;
using aistore::http::HttpServerConfig;
using aistore::metadata::Artifact;
using aistore::metadata::ChunkingStrategy;
using aistore::metadata::ChunkMetadata;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::StorageLocation;
using aistore::metadata::StorageLocationState;
using aistore::metadata::UploadSession;
using aistore::metadata::UploadSessionState;
using aistore::metadata::UuidV7;
using aistore::service::MetadataService;

namespace beast_http = aistore::http::beast_http;

constexpr std::uint64_t kMaxBodyBytes = 8ULL * 1024ULL * 1024ULL;
constexpr std::uint64_t kChunkSizeBytes = 4194304ULL;

std::string test_database_connection_string() {
    const char* configured = std::getenv("AISTORE_TEST_DB_URL");

    if (configured != nullptr && configured[0] != '\0') {
        return configured;
    }

    return "dbname=ai_artifact_store_test";
}

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

class RunningHttpServer {
   public:
    explicit RunningHttpServer(aistore::http::RequestHandler handler)
        : server_(
              HttpServerConfig{
                  .bind_address = "127.0.0.1",
                  .port = 0,
                  .worker_threads = 2,
                  .max_request_body_bytes = kMaxBodyBytes,
              },
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

class MetadataClientFixture : public ::testing::Test {
   protected:
    void SetUp() override {
        artifact_id_ = UuidV7::generate();
        session_id_ = UuidV7::generate();
        repository_.emplace(test_database_connection_string());
        repository_->create_artifact(
            Artifact{artifact_id_, std::string{"m4s3-artifact-"} + artifact_id_.str(), "m4s3-project"});
        service_.emplace(*repository_);
        server_.emplace([&](const HttpRequest& request) { return service_->handle_request(request); });
        client_.emplace(HttpClientConfig{
            .endpoint =
                HttpEndpoint{
                    .address = "127.0.0.1",
                    .port = server_->port(),
                },
        });
    }

    void TearDown() override {
        client_.reset();
        server_.reset();
        service_.reset();

        if (repository_.has_value()) {
            cleanup_owned_rows();
            repository_.reset();
        }
    }

    void cleanup_owned_rows() {
        pqxx::connection connection{test_database_connection_string()};
        pqxx::work transaction{connection};

        transaction
            .exec(
                "DELETE FROM upload_sessions "
                "WHERE session_id = $1::uuid",
                pqxx::params{session_id_.str()})
            .no_rows();

        transaction.exec("DELETE FROM storage_locations WHERE node_id LIKE 'm4s3-%'").no_rows();

        for (const std::string& chunk_id : owned_chunk_ids_) {
            transaction
                .exec(
                    "DELETE FROM storage_locations "
                    "WHERE chunk_id = $1",
                    pqxx::params{chunk_id})
                .no_rows();
            transaction
                .exec(
                    "DELETE FROM chunks "
                    "WHERE chunk_id = $1",
                    pqxx::params{chunk_id})
                .no_rows();
        }

        transaction
            .exec(
                "DELETE FROM artifacts "
                "WHERE artifact_id = $1::uuid",
                pqxx::params{artifact_id_.str()})
            .no_rows();

        transaction.commit();
    }

    [[nodiscard]] UploadSession make_open_session(std::string_view target_node = "m4s3-target") const {
        return UploadSession{session_id_,
                             artifact_id_,
                             std::string{target_node},
                             ChunkingStrategy::FixedSize,
                             kChunkSizeBytes,
                             std::nullopt,
                             UploadSession::ImmutableMetadata{{"source", "m4s3"}},
                             UploadSessionState::Open,
                             std::nullopt};
    }

    void track_chunk(std::string chunk_id) { owned_chunk_ids_.push_back(std::move(chunk_id)); }

    UuidV7 artifact_id_{UuidV7::generate()};
    UuidV7 session_id_{UuidV7::generate()};
    std::vector<std::string> owned_chunk_ids_;
    std::optional<PostgresMetadataRepository> repository_;
    std::optional<MetadataService> service_;
    std::optional<RunningHttpServer> server_;
    std::optional<MetadataClient> client_;
};

TEST_F(MetadataClientFixture, CreatesAndGetsUploadSession) {
    const UploadSession created = client_->create_upload_session(make_open_session());

    EXPECT_EQ(created.session_id(), session_id_);
    EXPECT_EQ(created.artifact_id(), artifact_id_);
    EXPECT_EQ(created.target_node_id(), "m4s3-target");
    EXPECT_EQ(created.chunking_strategy(), ChunkingStrategy::FixedSize);
    EXPECT_EQ(created.chunk_size_bytes(), kChunkSizeBytes);
    EXPECT_FALSE(created.parent_version_id().has_value());
    EXPECT_EQ(created.immutable_metadata().at("source"), "m4s3");
    EXPECT_EQ(created.state(), UploadSessionState::Open);
    EXPECT_FALSE(created.finalized_version_id().has_value());

    const auto loaded = client_->get_upload_session(session_id_);
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->session_id(), created.session_id());
    EXPECT_EQ(loaded->artifact_id(), created.artifact_id());
    EXPECT_EQ(loaded->target_node_id(), created.target_node_id());
    EXPECT_EQ(loaded->state(), UploadSessionState::Open);
}

TEST_F(MetadataClientFixture, MissingUploadSessionReturnsNullopt) {
    const UuidV7 missing = UuidV7::generate();
    EXPECT_FALSE(client_->get_upload_session(missing).has_value());
}

TEST_F(MetadataClientFixture, ConflictingCreateProducesRemoteApiError) {
    (void)client_->create_upload_session(make_open_session("m4s3-target"));

    try {
        (void)client_->create_upload_session(make_open_session("m4s3-other"));
        FAIL() << "expected RemoteApiError";
    } catch (const RemoteApiError& error) {
        EXPECT_EQ(error.status_code(), 409U);
        EXPECT_EQ(error.error_code(), "upload_session_conflict");
    }
}

TEST(MetadataClientStandaloneTest, MissingArtifactProducesRemoteApiError) {
    PostgresMetadataRepository repository{test_database_connection_string()};
    MetadataService service{repository};
    RunningHttpServer server{[&](const HttpRequest& request) { return service.handle_request(request); }};
    MetadataClient client{HttpClientConfig{
        .endpoint =
            HttpEndpoint{
                .address = "127.0.0.1",
                .port = server.port(),
            },
    }};

    const UuidV7 session_id = UuidV7::generate();
    const UuidV7 missing_artifact = UuidV7::generate();
    const UploadSession session{
        session_id, missing_artifact,         "m4s3-target", ChunkingStrategy::FixedSize, kChunkSizeBytes, std::nullopt,
        {},         UploadSessionState::Open, std::nullopt};

    try {
        (void)client.create_upload_session(session);
        FAIL() << "expected RemoteApiError";
    } catch (const RemoteApiError& error) {
        EXPECT_EQ(error.status_code(), 404U);
        EXPECT_EQ(error.error_code(), "upload_session_prerequisite_not_found");
    }
}

TEST_F(MetadataClientFixture, AbortsUploadSession) {
    (void)client_->create_upload_session(make_open_session());
    const UploadSession aborted = client_->abort_upload_session(session_id_);

    EXPECT_EQ(aborted.state(), UploadSessionState::Aborted);

    const auto stored = repository_->get_upload_session(session_id_);
    ASSERT_TRUE(stored.has_value());
    EXPECT_EQ(stored->state(), UploadSessionState::Aborted);
}

TEST_F(MetadataClientFixture, NegotiatesPreviouslyUnknownChunks) {
    (void)client_->create_upload_session(make_open_session("m4s3-target"));

    const std::string chunk_a = sha256_hex(std::string{"m4s3-unknown-a-"} + UuidV7::generate().str());
    const std::string chunk_b = sha256_hex(std::string{"m4s3-unknown-b-"} + UuidV7::generate().str());
    track_chunk(chunk_a);
    track_chunk(chunk_b);

    const auto result = client_->negotiate_chunks(
        session_id_, {{.chunk_id = chunk_a, .size_bytes = 11}, {.chunk_id = chunk_b, .size_bytes = 22}});

    EXPECT_EQ(result.session_id, session_id_);
    EXPECT_EQ(result.target_node_id, "m4s3-target");
    ASSERT_EQ(result.chunks.size(), 2U);

    for (const auto& entry : result.chunks) {
        EXPECT_FALSE(entry.metadata_was_known);
        EXPECT_EQ(entry.availability, ChunkAvailability::NoAvailableLocation);
        EXPECT_TRUE(entry.available_node_ids.empty());
    }

    EXPECT_EQ(repository_->get_chunk_size(chunk_a), 11U);
    EXPECT_EQ(repository_->get_chunk_size(chunk_b), 22U);
}

TEST_F(MetadataClientFixture, NegotiationParsesAllAvailabilityClasses) {
    (void)client_->create_upload_session(make_open_session("m4s3-target"));

    const std::string chunk_a = sha256_hex(std::string{"m4s3-avail-a-"} + UuidV7::generate().str());
    const std::string chunk_b = sha256_hex(std::string{"m4s3-avail-b-"} + UuidV7::generate().str());
    const std::string chunk_c = sha256_hex(std::string{"m4s3-avail-c-"} + UuidV7::generate().str());
    track_chunk(chunk_a);
    track_chunk(chunk_b);
    track_chunk(chunk_c);

    repository_->register_chunks({
        {.chunk_id = chunk_a, .size_bytes = 1},
        {.chunk_id = chunk_b, .size_bytes = 2},
        {.chunk_id = chunk_c, .size_bytes = 3},
    });

    repository_->register_storage_location(StorageLocation{
        .chunk_id = chunk_a,
        .node_id = "m4s3-target",
        .storage_path = "/m4s3/a",
        .state = StorageLocationState::Available,
    });
    repository_->register_storage_location(StorageLocation{
        .chunk_id = chunk_b,
        .node_id = "m4s3-other",
        .storage_path = "/m4s3/b",
        .state = StorageLocationState::Available,
    });
    repository_->register_storage_location(StorageLocation{
        .chunk_id = chunk_c,
        .node_id = "m4s3-missing",
        .storage_path = "/m4s3/c-missing",
        .state = StorageLocationState::Missing,
    });
    repository_->register_storage_location(StorageLocation{
        .chunk_id = chunk_c,
        .node_id = "m4s3-corrupt",
        .storage_path = "/m4s3/c-corrupt",
        .state = StorageLocationState::Corrupt,
    });

    const auto result = client_->negotiate_chunks(session_id_, {{.chunk_id = chunk_a, .size_bytes = 1},
                                                                {.chunk_id = chunk_b, .size_bytes = 2},
                                                                {.chunk_id = chunk_c, .size_bytes = 3}});

    ASSERT_EQ(result.chunks.size(), 3U);
    EXPECT_EQ(result.chunks[0].availability, ChunkAvailability::AvailableOnTarget);
    ASSERT_EQ(result.chunks[0].available_node_ids.size(), 1U);
    EXPECT_EQ(result.chunks[0].available_node_ids[0], "m4s3-target");

    EXPECT_EQ(result.chunks[1].availability, ChunkAvailability::AvailableElsewhere);
    ASSERT_EQ(result.chunks[1].available_node_ids.size(), 1U);
    EXPECT_EQ(result.chunks[1].available_node_ids[0], "m4s3-other");

    EXPECT_EQ(result.chunks[2].availability, ChunkAvailability::NoAvailableLocation);
    EXPECT_TRUE(result.chunks[2].available_node_ids.empty());
}

TEST_F(MetadataClientFixture, ChunkSizeConflictProducesRemoteApiError) {
    (void)client_->create_upload_session(make_open_session());

    const std::string chunk_id = sha256_hex(std::string{"m4s3-conflict-"} + UuidV7::generate().str());
    track_chunk(chunk_id);
    repository_->register_chunks({{.chunk_id = chunk_id, .size_bytes = 10}});

    try {
        (void)client_->negotiate_chunks(session_id_, {{.chunk_id = chunk_id, .size_bytes = 11}});
        FAIL() << "expected RemoteApiError";
    } catch (const RemoteApiError& error) {
        EXPECT_EQ(error.status_code(), 409U);
        EXPECT_EQ(error.error_code(), "chunk_size_conflict");
        EXPECT_NE(error.response_body().find(chunk_id), std::string::npos);
        EXPECT_NE(error.response_body().find("requested_size_bytes"), std::string::npos);
        EXPECT_NE(error.response_body().find("stored_size_bytes"), std::string::npos);
    }
}

TEST_F(MetadataClientFixture, AbortedSessionNegotiationProducesRemoteApiError) {
    (void)client_->create_upload_session(make_open_session());
    (void)client_->abort_upload_session(session_id_);

    const std::string chunk_id = sha256_hex(std::string{"m4s3-aborted-"} + UuidV7::generate().str());
    track_chunk(chunk_id);

    try {
        (void)client_->negotiate_chunks(session_id_, {{.chunk_id = chunk_id, .size_bytes = 1}});
        FAIL() << "expected RemoteApiError";
    } catch (const RemoteApiError& error) {
        EXPECT_EQ(error.status_code(), 409U);
        EXPECT_EQ(error.error_code(), "upload_session_not_open");
    }
}

TEST(MetadataClientStandaloneTest, MalformedSuccessfulResponseProducesProtocolError) {
    const UuidV7 session_id = UuidV7::generate();

    RunningHttpServer server{[session_id](const HttpRequest& request) {
        EXPECT_EQ(request.target(), std::string{"/v1/upload-sessions/"} + session_id.str());

        HttpResponse response{beast_http::status::ok, request.version()};
        response.set(beast_http::field::content_type, "application/json");
        response.body() = R"({"session_id":"broken"})";
        response.prepare_payload();
        return response;
    }};

    MetadataClient client{HttpClientConfig{
        .endpoint =
            HttpEndpoint{
                .address = "127.0.0.1",
                .port = server.port(),
            },
    }};

    EXPECT_THROW((void)client.get_upload_session(session_id), RemoteProtocolError);
}

TEST_F(MetadataClientFixture, RegistersStorageLocation) {
    const std::string chunk_id = sha256_hex(std::string{"m4s3-loc-reg-"} + UuidV7::generate().str());
    track_chunk(chunk_id);
    repository_->register_chunks({ChunkMetadata{.chunk_id = chunk_id, .size_bytes = 3}});

    const std::string storage_path = std::string{"/v1/chunks/"} + chunk_id;
    client_->register_storage_location(StorageLocation{
        .chunk_id = chunk_id,
        .node_id = "m4s3-loc-node",
        .storage_path = storage_path,
        .state = StorageLocationState::Available,
    });

    const auto locations = repository_->get_storage_locations(chunk_id);
    ASSERT_EQ(locations.size(), 1U);
    EXPECT_EQ(locations[0].chunk_id, chunk_id);
    EXPECT_EQ(locations[0].node_id, "m4s3-loc-node");
    EXPECT_EQ(locations[0].storage_path, storage_path);
    EXPECT_EQ(locations[0].state, StorageLocationState::Available);
}

TEST_F(MetadataClientFixture, MissingChunkLocationRegistrationProducesRemoteApiError) {
    const std::string chunk_id = sha256_hex(std::string{"m4s3-loc-missing-"} + UuidV7::generate().str());
    track_chunk(chunk_id);

    try {
        client_->register_storage_location(StorageLocation{
            .chunk_id = chunk_id,
            .node_id = "m4s3-loc-node",
            .storage_path = std::string{"/v1/chunks/"} + chunk_id,
            .state = StorageLocationState::Available,
        });
        FAIL() << "expected RemoteApiError";
    } catch (const RemoteApiError& error) {
        EXPECT_EQ(error.status_code(), 404U);
        EXPECT_EQ(error.error_code(), "chunk_not_found");
    }
}

}  // namespace
