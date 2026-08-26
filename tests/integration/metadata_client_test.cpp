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
#include "aistore/metadata/chunk_metadata.hpp"
#include "aistore/metadata/chunking.hpp"
#include "aistore/metadata/finalize_upload.hpp"
#include "aistore/metadata/gc.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/placement.hpp"
#include "aistore/metadata/postgres_metadata_repository.hpp"
#include "aistore/metadata/replication.hpp"
#include "aistore/metadata/storage_location.hpp"
#include "aistore/metadata/storage_node.hpp"
#include "aistore/metadata/upload_session.hpp"
#include "aistore/metadata/uuid_v7.hpp"
#include "aistore/service/metadata_service.hpp"

namespace {

void ensure_active_node(aistore::metadata::PostgresMetadataRepository& repository, const std::string& node_id) {
    repository.register_storage_node(aistore::metadata::StorageNode{
        .node_id = node_id,
        .address = "127.0.0.1",
        .port = 8081,
        .state = aistore::metadata::StorageNodeState::Active,
    });
}

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
using aistore::metadata::FastCdcParameters;
using aistore::metadata::GcChunkDecision;
using aistore::metadata::GcPhysicalStats;
using aistore::metadata::GcRun;
using aistore::metadata::GcRunMode;
using aistore::metadata::GcRunState;
using aistore::metadata::MultiNodeRestorePlan;
using aistore::metadata::Object;
using aistore::metadata::ObjectLayout;
using aistore::metadata::ObjectLayoutDescriptor;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::ReplicationRun;
using aistore::metadata::ReplicationRunState;
using aistore::metadata::ReplicationStats;
using aistore::metadata::RestorePlan;
using aistore::metadata::select_replica_nodes;
using aistore::metadata::StorageLocation;
using aistore::metadata::StorageLocationState;
using aistore::metadata::StorageNode;
using aistore::metadata::StorageNodeState;
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
            transaction.exec("DELETE FROM gc_runs").no_rows();
            transaction.exec("DELETE FROM replication_runs").no_rows();
            transaction.commit();
        }
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

        for (const UuidV7& gc_run_id : owned_gc_run_ids_) {
            transaction.exec("DELETE FROM gc_runs WHERE run_id = $1::uuid", pqxx::params{gc_run_id.str()}).no_rows();
        }

        transaction.exec("DELETE FROM replication_runs").no_rows();

        transaction
            .exec(
                "DELETE FROM upload_sessions "
                "WHERE session_id = $1::uuid",
                pqxx::params{session_id_.str()})
            .no_rows();

        transaction.exec("DELETE FROM artifact_versions WHERE artifact_id = $1::uuid", pqxx::params{artifact_id_.str()})
            .no_rows();

        for (const std::string& object_id : owned_object_ids_) {
            transaction.exec("DELETE FROM artifact_versions WHERE root_object_id = $1", pqxx::params{object_id})
                .no_rows();
        }

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

        transaction.exec("DELETE FROM replication_runs WHERE run_id IS NOT NULL").no_rows();
        transaction
            .exec(
                "DELETE FROM storage_locations WHERE node_id LIKE 'm4s3-%' OR node_id LIKE 'm4gc-%' OR "
                "node_id LIKE 'm8-client-%'")
            .no_rows();
        transaction.exec("DELETE FROM storage_nodes").no_rows();

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

    [[nodiscard]] UploadSession make_open_session(std::string_view target_node = "m4s3-target") {
        ensure_active_node(*repository_, std::string{target_node});
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

    void track_gc_run(UuidV7 gc_run_id) { owned_gc_run_ids_.push_back(std::move(gc_run_id)); }

    [[nodiscard]] ArtifactVersion create_committed_version(const ObjectLayoutDescriptor& descriptor,
                                                           std::string_view source_node = "m4s3-target") {
        ensure_active_node(*repository_, std::string{source_node});
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

    [[nodiscard]] UploadSession make_fastcdc_open_session(std::string_view target_node = "m4s3-target") {
        ensure_active_node(*repository_, std::string{target_node});
        return UploadSession{
            session_id_,
            artifact_id_,
            std::string{target_node},
            FastCdcParameters{
                .min_chunk_size_bytes = 64,
                .avg_chunk_size_bytes = 256,
                .max_chunk_size_bytes = 1024,
            },
            std::nullopt,
            UploadSession::ImmutableMetadata{{"source", "m4s3-fastcdc"}},
            UploadSessionState::Open,
            std::nullopt,
        };
    }

    [[nodiscard]] ObjectLayoutDescriptor make_fastcdc_descriptor() {
        const std::string marker = UuidV7::generate().str();
        const std::string object_id = sha256_hex("m6-client-fastcdc-object-" + marker);
        const std::string chunk_id = sha256_hex("m6-client-fastcdc-chunk-" + marker);
        owned_object_ids_.push_back(object_id);
        track_chunk(chunk_id);
        return ObjectLayoutDescriptor{
            Object{object_id, 128},
            FastCdcParameters{
                .min_chunk_size_bytes = 64,
                .avg_chunk_size_bytes = 256,
                .max_chunk_size_bytes = 1024,
            },
            ObjectLayout{{ChunkRef{.chunk_id = chunk_id, .offset = 0, .size = 128}}},
        };
    }

    UuidV7 artifact_id_{UuidV7::generate()};
    UuidV7 session_id_{UuidV7::generate()};
    std::vector<std::string> owned_chunk_ids_;
    std::vector<std::string> owned_object_ids_;
    std::vector<UuidV7> owned_gc_run_ids_;
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
    ensure_active_node(repository, "m4s3-target");
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
        EXPECT_EQ(error.error_code(), "chunk_under_replicated");
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

TEST_F(MetadataClientFixture, CreatesAndParsesFastCdcUploadSession) {
    const UploadSession created = client_->create_upload_session(make_fastcdc_open_session());

    EXPECT_EQ(created.chunking_strategy(), ChunkingStrategy::FastCdc);
    ASSERT_TRUE(created.fastcdc_parameters().has_value());
    EXPECT_EQ(created.fastcdc_parameters()->avg_chunk_size_bytes, 256U);
    EXPECT_EQ(created.immutable_metadata().at("source"), "m4s3-fastcdc");

    const auto loaded = client_->get_upload_session(session_id_);
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->fastcdc_parameters(), created.fastcdc_parameters());
}

TEST_F(MetadataClientFixture, FinalizeSerializesFastCdcParameters) {
    (void)client_->create_upload_session(make_fastcdc_open_session());
    const ObjectLayoutDescriptor descriptor = make_fastcdc_descriptor();
    const ChunkRef& chunk = descriptor.layout().chunks().front();
    repository_->register_chunks({ChunkMetadata{.chunk_id = chunk.chunk_id, .size_bytes = chunk.size}});
    repository_->register_storage_location(StorageLocation{
        .chunk_id = chunk.chunk_id,
        .node_id = "m4s3-target",
        .storage_path = std::string{"/v1/chunks/"} + chunk.chunk_id,
        .state = StorageLocationState::Available,
    });

    const auto result = client_->finalize_upload(session_id_, descriptor);

    EXPECT_EQ(result.object_id, descriptor.object_id());
    EXPECT_EQ(result.layout_id, descriptor.layout_id());
    const auto layout = repository_->get_object_layout(descriptor.layout_id());
    ASSERT_TRUE(layout.has_value());
    EXPECT_EQ(layout->fastcdc_parameters(), descriptor.fastcdc_parameters());
}

TEST_F(MetadataClientFixture, RestorePlanParsesFastCdcParameters) {
    const ObjectLayoutDescriptor descriptor = make_fastcdc_descriptor();
    const ArtifactVersion version = create_committed_version(descriptor);
    const RestorePlan plan = client_->get_restore_plan(version.version_id(), "m4s3-target");

    EXPECT_EQ(plan.layout_descriptor.chunking_strategy(), ChunkingStrategy::FastCdc);
    ASSERT_TRUE(plan.layout_descriptor.fastcdc_parameters().has_value());
    EXPECT_EQ(plan.layout_descriptor.fastcdc_parameters()->avg_chunk_size_bytes, 256U);
    EXPECT_EQ(plan.layout_descriptor.layout_id(), descriptor.layout_id());
}

TEST(MetadataClientStandaloneTest, RejectsMalformedFastCdcSuccessfulConfiguration) {
    const UuidV7 session_id = UuidV7::generate();

    RunningHttpServer server{[session_id](const HttpRequest& request) {
        EXPECT_EQ(request.target(), std::string{"/v1/upload-sessions/"} + session_id.str());

        HttpResponse response{beast_http::status::ok, request.version()};
        response.set(beast_http::field::content_type, "application/json");
        response.body() = boost::json::serialize(boost::json::object{
            {"session_id", session_id.str()},
            {"artifact_id", UuidV7::generate().str()},
            {"target_node_id", "m4s3-target"},
            {"chunking_strategy", "fastcdc"},
            {"chunking_parameters",
             boost::json::object{
                 {"min_chunk_size_bytes", 64},
                 {"avg_chunk_size_bytes", 300},
                 {"max_chunk_size_bytes", 1024},
             }},
            {"parent_version_id", nullptr},
            {"immutable_metadata", boost::json::object{}},
            {"state", "open"},
            {"finalized_version_id", nullptr},
        });
        response.prepare_payload();
        return response;
    }};

    MetadataClient client{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = server.port()},
    }};

    EXPECT_THROW((void)client.get_upload_session(session_id), RemoteProtocolError);
}

TEST(MetadataClientStandaloneTest, GetRestorePlanValidatesArgumentsBeforeNetworkIo) {
    MetadataClient client{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = 1},
    }};

    EXPECT_THROW((void)client.get_restore_plan("not-a-version-id", "m4s3-target"), std::invalid_argument);
    EXPECT_THROW((void)client.get_restore_plan(std::string(64, 'a'), "not a node"), std::invalid_argument);
}

TEST_F(MetadataClientFixture, GcLifecycleRoundTrip) {
    const UuidV7 gc_run_id = UuidV7::generate();
    track_gc_run(gc_run_id);

    const GcRun started = client_->start_gc_run(gc_run_id, "m4gc-node-a", false);
    EXPECT_EQ(started.run_id, gc_run_id);
    EXPECT_EQ(started.target_node_id, "m4gc-node-a");
    EXPECT_EQ(started.mode, GcRunMode::Apply);
    EXPECT_EQ(started.state, GcRunState::Open);

    const auto loaded = client_->get_gc_run(gc_run_id);
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->run_id, gc_run_id);
    EXPECT_EQ(loaded->state, GcRunState::Open);

    const GcPhysicalStats physical_stats{
        .physical_chunks_scanned = 3,
        .physical_bytes_scanned = 12,
        .collectible_chunks = 2,
        .collectible_bytes = 8,
        .physically_deleted_chunks = 2,
        .physically_deleted_bytes = 8,
    };
    const GcRun completed = client_->complete_gc_run(gc_run_id, physical_stats);

    EXPECT_EQ(completed.run_id, gc_run_id);
    EXPECT_EQ(completed.state, GcRunState::Completed);
    EXPECT_EQ(completed.physical_stats, physical_stats);
}

TEST_F(MetadataClientFixture, GcClassificationParsesStrictly) {
    const UuidV7 gc_run_id = UuidV7::generate();
    track_gc_run(gc_run_id);

    const std::string live_only_chunk(64, 'b');
    const std::string shared_chunk(64, 'a');
    const std::string untracked_chunk(64, 'd');

    const ObjectLayoutDescriptor live_layout{
        Object{std::string(64, '1'), 6},
        ChunkingStrategy::FixedSize,
        ObjectLayout{{
            ChunkRef{.chunk_id = shared_chunk, .offset = 0, .size = 3},
            ChunkRef{.chunk_id = live_only_chunk, .offset = 3, .size = 3},
        }},
    };
    const ObjectLayoutDescriptor dead_layout{
        Object{std::string(64, '2'), 6},
        ChunkingStrategy::FixedSize,
        ObjectLayout{{
            ChunkRef{.chunk_id = shared_chunk, .offset = 0, .size = 3},
            ChunkRef{.chunk_id = std::string(64, 'c'), .offset = 3, .size = 3},
        }},
    };

    repository_->register_object(live_layout.object());
    repository_->register_object_layout(live_layout);
    repository_->create_version(ArtifactVersion{
        artifact_id_,
        live_layout.object_id(),
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{{"marker", gc_run_id.str()}},
        VersionState::Committed,
    });
    repository_->register_object(dead_layout.object());
    repository_->register_object_layout(dead_layout);
    owned_object_ids_.push_back(live_layout.object_id());
    owned_object_ids_.push_back(dead_layout.object_id());

    (void)client_->start_gc_run(gc_run_id, "m4gc-node-a", false);

    const std::vector<GcChunkDecision> decisions =
        client_->classify_gc_chunks(gc_run_id, {live_only_chunk, shared_chunk, untracked_chunk});

    ASSERT_EQ(decisions.size(), 3U);
    EXPECT_EQ(decisions[0].chunk_id, live_only_chunk);
    EXPECT_FALSE(decisions[0].collectible);
    EXPECT_EQ(decisions[1].chunk_id, shared_chunk);
    EXPECT_FALSE(decisions[1].collectible);
    EXPECT_EQ(decisions[2].chunk_id, untracked_chunk);
    EXPECT_TRUE(decisions[2].collectible);
}

TEST(MetadataClientStandaloneTest, GcMalformedSuccessfulResponseIsRejected) {
    const UuidV7 gc_run_id = UuidV7::generate();

    RunningHttpServer server{[gc_run_id](const HttpRequest& request) {
        if (request.target() == "/v1/gc-runs" && request.method() == beast_http::verb::post) {
            HttpResponse response{beast_http::status::ok, request.version()};
            response.set(beast_http::field::content_type, "application/json");
            response.body() = boost::json::serialize(boost::json::object{
                {"gc_run_id", gc_run_id.str()},
                {"target_node_id", "m4gc-target"},
                {"dry_run", false},
                {"state", "open"},
                {"physical_chunks_scanned", 0},
                {"extra_field", "unexpected"},
            });
            response.prepare_payload();
            return response;
        }

        HttpResponse response{beast_http::status::not_found, request.version()};
        response.set(beast_http::field::content_type, "application/json");
        response.body() = R"({"error":"not_found"})";
        response.prepare_payload();
        return response;
    }};

    MetadataClient client{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = server.port()},
    }};

    EXPECT_THROW((void)client.start_gc_run(gc_run_id, "m4gc-target", false), RemoteProtocolError);
}

TEST_F(MetadataClientFixture, StorageNodeRegistryRoundTrip) {
    client_->register_storage_node(StorageNode{
        .node_id = "m8-client-node-a",
        .address = "127.0.0.1",
        .port = 9201,
        .state = StorageNodeState::Active,
    });

    const auto loaded = client_->get_storage_node("m8-client-node-a");
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->node_id, "m8-client-node-a");
    EXPECT_EQ(loaded->port, 9201U);

    const std::vector<StorageNode> nodes = client_->list_storage_nodes();
    ASSERT_FALSE(nodes.empty());
}

TEST_F(MetadataClientFixture, MultiNodeUploadSessionRoundTrip) {
    for (const char* node_id : {"m8-client-a", "m8-client-b", "m8-client-c"}) {
        client_->register_storage_node(StorageNode{
            .node_id = node_id,
            .address = "127.0.0.1",
            .port = 9210,
            .state = StorageNodeState::Active,
        });
    }
    const std::vector<std::string> placement{"m8-client-a", "m8-client-b", "m8-client-c"};
    const UploadSession session{session_id_,
                                artifact_id_,
                                2U,
                                placement,
                                ChunkingStrategy::FixedSize,
                                kChunkSizeBytes,
                                std::nullopt,
                                UploadSession::ImmutableMetadata{{"source", "m8-client"}},
                                UploadSessionState::Open,
                                std::nullopt};

    const UploadSession created = client_->create_upload_session(session);
    EXPECT_EQ(created.replication_factor(), 2U);
    EXPECT_EQ(created.placement_node_ids(), placement);

    const auto loaded = client_->get_upload_session(session_id_);
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->placement_node_ids(), placement);
}

TEST_F(MetadataClientFixture, ParsesStrictAutomaticRestorePlan) {
    client_->register_storage_node(StorageNode{
        .node_id = "m8-client-restore-a",
        .address = "127.0.0.1",
        .port = 9202,
        .state = StorageNodeState::Active,
    });
    client_->register_storage_node(StorageNode{
        .node_id = "m8-client-restore-b",
        .address = "127.0.0.2",
        .port = 9203,
        .state = StorageNodeState::Active,
    });

    const std::string shared_chunk = sha256_hex("m8-client-restore-shared");
    const std::string second_chunk = sha256_hex("m8-client-restore-second");
    const std::string object_id = sha256_hex("m8-client-restore-object");
    const ObjectLayoutDescriptor descriptor{
        Object{object_id, 6},
        ChunkingStrategy::FixedSize,
        ObjectLayout{{
            ChunkRef{.chunk_id = shared_chunk, .offset = 0, .size = 3},
            ChunkRef{.chunk_id = second_chunk, .offset = 3, .size = 3},
        }},
    };

    repository_->register_chunks({
        ChunkMetadata{.chunk_id = shared_chunk, .size_bytes = 3},
        ChunkMetadata{.chunk_id = second_chunk, .size_bytes = 3},
    });
    repository_->register_object(descriptor.object());
    repository_->register_object_layout(descriptor);
    repository_->register_storage_location(StorageLocation{
        .chunk_id = shared_chunk,
        .node_id = "m8-client-restore-a",
        .storage_path = "/v1/chunks/" + shared_chunk,
        .state = StorageLocationState::Available,
    });
    repository_->register_storage_location(StorageLocation{
        .chunk_id = second_chunk,
        .node_id = "m8-client-restore-b",
        .storage_path = "/v1/chunks/" + second_chunk,
        .state = StorageLocationState::Available,
    });
    owned_object_ids_.push_back(descriptor.object_id());

    ArtifactVersion version{
        artifact_id_,
        descriptor.object_id(),
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{{"marker", "m8-client-restore"}},
        VersionState::Committed,
    };
    repository_->create_version(version);

    const MultiNodeRestorePlan plan = client_->get_multi_node_restore_plan(version.version_id());
    EXPECT_EQ(plan.version_id, version.version_id());
    EXPECT_EQ(plan.chunks.size(), 2U);
    EXPECT_FALSE(plan.chunks.front().sources.empty());
}

TEST_F(MetadataClientFixture, ReplicationLifecycleAndPlanRoundTrip) {
    client_->register_storage_node(StorageNode{
        .node_id = "m8-client-repl-a",
        .address = "127.0.0.1",
        .port = 9204,
        .state = StorageNodeState::Active,
    });
    client_->register_storage_node(StorageNode{
        .node_id = "m8-client-repl-b",
        .address = "127.0.0.2",
        .port = 9205,
        .state = StorageNodeState::Active,
    });
    client_->register_storage_node(StorageNode{
        .node_id = "m8-client-repl-c",
        .address = "127.0.0.3",
        .port = 9206,
        .state = StorageNodeState::Active,
    });

    const std::string shared_chunk = sha256_hex("m8-client-repl-shared");
    const std::string second_chunk = sha256_hex("m8-client-repl-second");
    const std::string object_id = sha256_hex("m8-client-repl-object");
    const ObjectLayoutDescriptor descriptor{
        Object{object_id, 6},
        ChunkingStrategy::FixedSize,
        ObjectLayout{{
            ChunkRef{.chunk_id = shared_chunk, .offset = 0, .size = 4},
            ChunkRef{.chunk_id = second_chunk, .offset = 4, .size = 2},
        }},
    };

    repository_->register_object(descriptor.object());
    repository_->register_object_layout(descriptor);
    owned_object_ids_.push_back(descriptor.object_id());

    const std::vector<std::string> placement{"m8-client-repl-a", "m8-client-repl-b", "m8-client-repl-c"};
    for (const ChunkRef& chunk : descriptor.layout().chunks()) {
        repository_->register_chunks({ChunkMetadata{.chunk_id = chunk.chunk_id, .size_bytes = chunk.size}});
        const std::vector<std::string> desired = select_replica_nodes(chunk.chunk_id, placement, 2U);
        for (const std::string& node_id : desired) {
            repository_->register_storage_location(StorageLocation{
                .chunk_id = chunk.chunk_id,
                .node_id = node_id,
                .storage_path = "/v1/chunks/" + chunk.chunk_id,
                .state = StorageLocationState::Available,
            });
        }
    }

    ArtifactVersion version{
        artifact_id_,
        descriptor.object_id(),
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{{"marker", "m8-client-repl"}},
        VersionState::Committed,
    };
    repository_->create_version(version);

    const UuidV7 run_id = UuidV7::generate();

    const ReplicationRun started = client_->start_replication_run(run_id, version.version_id(), 2U);
    EXPECT_EQ(started.run_id, run_id);
    EXPECT_EQ(started.state, ReplicationRunState::Open);

    (void)client_->get_replication_plan(run_id);

    const ReplicationStats stats{
        .chunks_scanned = 2,
        .replicas_verified = 2,
        .replicas_written = 2,
    };
    const ReplicationRun completed = client_->complete_replication_run(run_id, stats);
    EXPECT_EQ(completed.state, ReplicationRunState::Completed);
}

}  // namespace
