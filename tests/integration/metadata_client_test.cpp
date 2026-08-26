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
#include "aistore/metadata/finalize_upload.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/postgres_metadata_repository.hpp"
#include "aistore/metadata/restore_plan.hpp"
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
using aistore::metadata::ArtifactVersion;
using aistore::metadata::ChunkingStrategy;
using aistore::metadata::ChunkMetadata;
using aistore::metadata::ChunkRef;
using aistore::metadata::Object;
using aistore::metadata::ObjectLayout;
using aistore::metadata::ObjectLayoutDescriptor;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::RestorePlan;
using aistore::metadata::StorageLocation;
using aistore::metadata::StorageLocationState;
using aistore::metadata::UploadSession;
using aistore::metadata::UploadSessionState;
using aistore::metadata::UuidV7;
using aistore::metadata::VersionState;
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

    [[nodiscard]] ArtifactVersion create_committed_version(const ObjectLayoutDescriptor& descriptor,
                                                           std::string_view source_node = "m4s3-target") {
        repository_->register_object(descriptor.object());
        repository_->register_object_layout(descriptor);
        for (const ChunkRef& chunk : descriptor.layout().chunks()) {
            repository_->register_chunks({ChunkMetadata{.chunk_id = chunk.chunk_id, .size_bytes = chunk.size}});
            repository_->register_storage_location(StorageLocation{
                .chunk_id = chunk.chunk_id,
                .node_id = std::string{source_node},
                .storage_path = std::string{"/v1/chunks/"} + chunk.chunk_id,
                .state = StorageLocationState::Available,
            });
        }
        ArtifactVersion version{
            artifact_id_,
            descriptor.object_id(),
            std::nullopt,
            ArtifactVersion::ImmutableMetadata{{"source", "m4s3"}},
            VersionState::Committed,
        };
        repository_->create_version(version);
        return version;
    }

    [[nodiscard]] ObjectLayoutDescriptor make_descriptor() {
        const std::string marker = UuidV7::generate().str();
        const std::string object_id = sha256_hex("m4s5-client-object-" + marker);
        const std::string chunk_id = sha256_hex("m4s5-client-chunk-" + marker);
        owned_object_ids_.push_back(object_id);
        track_chunk(chunk_id);
        return ObjectLayoutDescriptor{Object{object_id, 4}, ChunkingStrategy::FixedSize,
                                      ObjectLayout{{ChunkRef{.chunk_id = chunk_id, .offset = 0, .size = 4}}}};
    }

    UuidV7 artifact_id_{UuidV7::generate()};
    UuidV7 session_id_{UuidV7::generate()};
    std::vector<std::string> owned_chunk_ids_;
    std::vector<std::string> owned_object_ids_;
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

TEST_F(MetadataClientFixture, FinalizesUploadThroughMetadataClient) {
    (void)client_->create_upload_session(make_open_session());
    const ObjectLayoutDescriptor descriptor = make_descriptor();
    const ChunkRef& chunk = descriptor.layout().chunks().front();
    repository_->register_chunks({ChunkMetadata{.chunk_id = chunk.chunk_id, .size_bytes = chunk.size}});
    repository_->register_storage_location(StorageLocation{
        .chunk_id = chunk.chunk_id,
        .node_id = "m4s3-target",
        .storage_path = std::string{"/v1/chunks/"} + chunk.chunk_id,
        .state = StorageLocationState::Available,
    });

    const auto result = client_->finalize_upload(session_id_, descriptor);

    EXPECT_EQ(result.session_id, session_id_);
    EXPECT_EQ(result.object_id, descriptor.object_id());
    EXPECT_EQ(result.layout_id, descriptor.layout_id());
    ASSERT_TRUE(repository_->get_version(result.version_id).has_value());
    const auto session = repository_->get_upload_session(session_id_);
    ASSERT_TRUE(session.has_value());
    EXPECT_EQ(session->state(), UploadSessionState::Committed);
    EXPECT_EQ(session->finalized_version_id(), result.version_id);
}

TEST_F(MetadataClientFixture, FinalizeRemoteFailureProducesRemoteApiError) {
    (void)client_->create_upload_session(make_open_session());
    const ObjectLayoutDescriptor descriptor = make_descriptor();
    const ChunkRef& chunk = descriptor.layout().chunks().front();
    repository_->register_chunks({ChunkMetadata{.chunk_id = chunk.chunk_id, .size_bytes = chunk.size}});

    try {
        (void)client_->finalize_upload(session_id_, descriptor);
        FAIL() << "expected RemoteApiError";
    } catch (const RemoteApiError& error) {
        EXPECT_EQ(error.status_code(), 409U);
        EXPECT_EQ(error.error_code(), "chunk_not_available_on_target");
        EXPECT_NE(error.response_body().find(chunk.chunk_id), std::string::npos);
    }
}

TEST(MetadataClientStandaloneTest, MalformedFinalizeSuccessProducesProtocolError) {
    const UuidV7 session_id = UuidV7::generate();
    const std::string object_id = sha256_hex("m4s5-malformed-object-" + session_id.str());
    const ObjectLayoutDescriptor descriptor{
        Object{object_id, 0},
        ChunkingStrategy::FixedSize,
        ObjectLayout{{}},
    };

    RunningHttpServer server{[session_id](const HttpRequest& request) {
        EXPECT_EQ(request.method(), beast_http::verb::post);
        EXPECT_EQ(request.target(), std::string{"/v1/upload-sessions/"} + session_id.str() + "/finalize");
        HttpResponse response{beast_http::status::ok, request.version()};
        response.set(beast_http::field::content_type, "application/json");
        response.body() = R"({"session_id":"broken"})";
        response.prepare_payload();
        return response;
    }};
    MetadataClient client{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = server.port()},
    }};

    EXPECT_THROW((void)client.finalize_upload(session_id, descriptor), RemoteProtocolError);
}

TEST_F(MetadataClientFixture, GetRestorePlanParsesStrictSuccessfulResponse) {
    const ObjectLayoutDescriptor descriptor = make_descriptor();
    const ArtifactVersion version = create_committed_version(descriptor);
    const RestorePlan plan = client_->get_restore_plan(version.version_id(), "m4s3-target");

    EXPECT_EQ(plan.artifact_id, artifact_id_);
    EXPECT_EQ(plan.version_id, version.version_id());
    EXPECT_EQ(plan.source_node_id, "m4s3-target");
    EXPECT_EQ(plan.layout_descriptor.object_id(), descriptor.object_id());
    EXPECT_EQ(plan.layout_descriptor.layout_id(), descriptor.layout_id());
    EXPECT_EQ(plan.layout_descriptor.object().total_size(), 4U);
    ASSERT_EQ(plan.layout_descriptor.layout().chunks().size(), 1U);
    EXPECT_EQ(plan.layout_descriptor.layout().chunks().front().chunk_id, descriptor.layout().chunks().front().chunk_id);
}

TEST_F(MetadataClientFixture, GetRestorePlanPropagatesRemoteRestoreErrors) {
    const std::string missing_version = sha256_hex(std::string{"m5s5-missing-version-"} + UuidV7::generate().str());

    try {
        (void)client_->get_restore_plan(missing_version, "m4s3-target");
        FAIL() << "expected RemoteApiError";
    } catch (const RemoteApiError& error) {
        EXPECT_EQ(error.status_code(), 404U);
        EXPECT_EQ(error.error_code(), "artifact_version_not_found");
    }
}

TEST(MetadataClientStandaloneTest, GetRestorePlanRejectsMalformedSuccessfulResponse) {
    const std::string version_id = sha256_hex("m5s5-malformed-restore-" + UuidV7::generate().str());
    const std::string source_node = "m4s3-target";

    RunningHttpServer server{[version_id, source_node](const HttpRequest& request) {
        EXPECT_EQ(request.method(), beast_http::verb::get);
        EXPECT_EQ(request.target(),
                  std::string{"/v1/artifact-versions/"} + version_id + "/restore-plan/" + source_node);
        HttpResponse response{beast_http::status::ok, request.version()};
        response.set(beast_http::field::content_type, "application/json");
        response.body() = boost::json::serialize(boost::json::object{
            {"version_id", version_id},
            {"artifact_id", UuidV7::generate().str()},
            {"source_node_id", source_node},
            {"object_id", std::string(64, '1')},
            {"total_size_bytes", 4},
            {"layout_id", std::string(64, '2')},
            {"chunking_strategy", "fixed-size"},
            {"chunk_count", 1},
            {"chunks", boost::json::array{}},
            {"extra_field", "unexpected"},
        });
        response.prepare_payload();
        return response;
    }};

    MetadataClient client{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = server.port()},
    }};

    EXPECT_THROW((void)client.get_restore_plan(version_id, source_node), RemoteProtocolError);
}

TEST(MetadataClientStandaloneTest, GetRestorePlanValidatesArgumentsBeforeNetworkIo) {
    MetadataClient client{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = 1},
    }};

    EXPECT_THROW((void)client.get_restore_plan("not-a-version-id", "m4s3-target"), std::invalid_argument);
    EXPECT_THROW((void)client.get_restore_plan(std::string(64, 'a'), "not a node"), std::invalid_argument);
}

}  // namespace
