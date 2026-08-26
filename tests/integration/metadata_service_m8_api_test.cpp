#include <gtest/gtest.h>

#include <array>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>
#include <cstdint>
#include <cstdlib>
#include <pqxx/pqxx>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "aistore/hashing/sha256.hpp"
#include "aistore/http/http_server.hpp"
#include "aistore/metadata/artifact_model.hpp"
#include "aistore/metadata/chunking.hpp"
#include "aistore/metadata/object.hpp"
#include "aistore/metadata/object_layout.hpp"
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

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace beast_http = beast::http;

using tcp = asio::ip::tcp;

using aistore::hashing::Sha256;
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
using aistore::metadata::UploadSession;
using aistore::metadata::UploadSessionState;
using aistore::metadata::UuidV7;
using aistore::metadata::VersionState;
using aistore::service::MetadataService;

constexpr std::uint64_t kMaxRequestBodyBytes = 8ULL * 1024ULL * 1024ULL;
constexpr std::uint64_t kChunkSizeBytes = 4194304ULL;

std::string test_database_connection_string() {
    const char* configured = std::getenv("AISTORE_TEST_DB_URL");

    if (configured != nullptr && configured[0] != '\0') {
        return configured;
    }

    return "dbname=ai_artifact_store_test";
}

[[nodiscard]] std::uint64_t json_uint64(const boost::json::value& value) {
    if (value.is_uint64()) {
        return value.as_uint64();
    }

    if (value.is_int64()) {
        const std::int64_t signed_value = value.as_int64();
        EXPECT_GE(signed_value, 0);
        return static_cast<std::uint64_t>(signed_value);
    }

    ADD_FAILURE() << "expected JSON integer";
    return 0;
}

std::string sha256_hex(std::string_view text) {
    Sha256 hasher;
    hasher.update(std::as_bytes(std::span{text.data(), text.size()}));
    const Sha256::Digest digest = hasher.finalize();

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

HttpResponse http_exchange(std::uint16_t port, beast_http::verb method, std::string_view target,
                           std::optional<std::string> body = std::nullopt,
                           std::optional<std::string_view> content_type = std::nullopt) {
    asio::io_context ioc;

    tcp::resolver resolver{ioc};
    beast::tcp_stream stream{ioc};

    const auto endpoints = resolver.resolve("127.0.0.1", std::to_string(port));
    stream.connect(endpoints);

    HttpRequest request{
        method,
        target,
        11,
    };
    request.set(beast_http::field::host, "127.0.0.1");
    request.set(beast_http::field::user_agent, "aistore-metadata-service-m8-api-test");

    if (content_type.has_value()) {
        request.set(beast_http::field::content_type, *content_type);
    }

    if (body.has_value()) {
        request.body() = std::move(*body);
    }

    request.prepare_payload();

    beast_http::write(stream, request);

    beast::flat_buffer buffer;
    HttpResponse response;
    beast_http::read(stream, buffer, response);

    beast::error_code ignored;
    stream.socket().shutdown(tcp::socket::shutdown_both, ignored);

    return response;
}

class RunningHttpServer {
   public:
    explicit RunningHttpServer(MetadataService& service)
        : server_(
              HttpServerConfig{
                  .bind_address = "127.0.0.1",
                  .port = 0,
                  .worker_threads = 2,
                  .max_request_body_bytes = kMaxRequestBodyBytes,
              },
              [&service](const HttpRequest& request) { return service.handle_request(request); }),
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

ObjectLayoutDescriptor make_m8_layout(std::string_view object_seed, std::string_view first_chunk_seed,
                                      std::string_view second_chunk_seed) {
    const std::string first_chunk_id = sha256_hex(first_chunk_seed);
    const std::string second_chunk_id = sha256_hex(second_chunk_seed);
    const std::string object_id = sha256_hex(std::string{object_seed} + first_chunk_id + second_chunk_id);
    return ObjectLayoutDescriptor{
        Object{object_id, 6},
        ChunkingStrategy::FixedSize,
        ObjectLayout{{
            ChunkRef{.chunk_id = first_chunk_id, .offset = 0, .size = 4},
            ChunkRef{.chunk_id = second_chunk_id, .offset = 4, .size = 2},
        }},
    };
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

void register_chunk_on_node(PostgresMetadataRepository& repository, const ChunkRef& chunk, std::string_view node_id,
                            StorageLocationState state) {
    repository.register_chunks({ChunkMetadata{.chunk_id = chunk.chunk_id, .size_bytes = chunk.size}});
    repository.register_storage_location(StorageLocation{
        .chunk_id = chunk.chunk_id,
        .node_id = std::string{node_id},
        .storage_path = std::string{"/m8-http/"} + chunk.chunk_id + std::string{node_id},
        .state = state,
    });
}

ArtifactVersion register_committed_version(PostgresMetadataRepository& repository, const UuidV7& artifact_id,
                                           const ObjectLayoutDescriptor& descriptor, std::string marker) {
    repository.register_object(descriptor.object());
    repository.register_object_layout(descriptor);

    ArtifactVersion version{
        artifact_id,
        descriptor.object_id(),
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{{"marker", std::move(marker)}},
        VersionState::Committed,
    };
    repository.create_version(version);

    return version;
}

class MetadataServiceM8ApiTest : public ::testing::Test {
   protected:
    void SetUp() override {
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
        service_.emplace(*repository_);
        server_.emplace(*service_);
    }

    void TearDown() override {
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

        for (const UuidV7& replication_run_id : owned_replication_run_ids_) {
            transaction
                .exec("DELETE FROM replication_runs WHERE run_id = $1::uuid", pqxx::params{replication_run_id.str()})
                .no_rows();
        }

        for (const UuidV7& gc_run_id : owned_gc_run_ids_) {
            transaction.exec("DELETE FROM gc_runs WHERE run_id = $1::uuid", pqxx::params{gc_run_id.str()}).no_rows();
        }

        for (const UuidV7& session_id : owned_session_ids_) {
            transaction.exec("DELETE FROM upload_sessions WHERE session_id = $1::uuid", pqxx::params{session_id.str()})
                .no_rows();
        }

        for (const UuidV7& artifact_id : owned_artifact_ids_) {
            transaction
                .exec("DELETE FROM artifact_versions WHERE artifact_id = $1::uuid", pqxx::params{artifact_id.str()})
                .no_rows();
            transaction.exec("DELETE FROM artifacts WHERE artifact_id = $1::uuid", pqxx::params{artifact_id.str()})
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

        transaction.exec("DELETE FROM storage_locations WHERE node_id LIKE 'm8-http-%'").no_rows();
        transaction.exec("DELETE FROM storage_nodes").no_rows();
        transaction.commit();
    }

    [[nodiscard]] std::string make_multi_node_session_json(const UuidV7& session_id, const UuidV7& artifact_id,
                                                           std::uint8_t replication_factor,
                                                           const std::vector<std::string>& placement) const {
        boost::json::array placement_array;
        for (const std::string& node_id : placement) {
            placement_array.push_back(boost::json::value(node_id));
        }

        return boost::json::serialize(boost::json::object{
            {"session_id", session_id.str()},
            {"artifact_id", artifact_id.str()},
            {"target_node_id", placement.front()},
            {"replication_factor", replication_factor},
            {"placement_node_ids", std::move(placement_array)},
            {"chunking_strategy", "fixed-size"},
            {"chunking_parameters",
             boost::json::object{
                 {"chunk_size_bytes", kChunkSizeBytes},
             }},
            {"parent_version_id", nullptr},
            {"immutable_metadata",
             boost::json::object{
                 {"source", "m8-http"},
             }},
        });
    }

    void track_replication_run(UuidV7 run_id) { owned_replication_run_ids_.push_back(std::move(run_id)); }

    void track_gc_run(UuidV7 gc_run_id) { owned_gc_run_ids_.push_back(std::move(gc_run_id)); }

    void track_session(UuidV7 session_id) { owned_session_ids_.push_back(std::move(session_id)); }

    void track_artifact(UuidV7 artifact_id) { owned_artifact_ids_.push_back(std::move(artifact_id)); }

    void track_object(std::string object_id) { owned_object_ids_.push_back(std::move(object_id)); }

    std::vector<UuidV7> owned_replication_run_ids_;
    std::vector<UuidV7> owned_gc_run_ids_;
    std::vector<UuidV7> owned_session_ids_;
    std::vector<UuidV7> owned_artifact_ids_;
    std::vector<std::string> owned_object_ids_;
    std::optional<PostgresMetadataRepository> repository_;
    std::optional<MetadataService> service_;
    std::optional<RunningHttpServer> server_;
};

TEST_F(MetadataServiceM8ApiTest, StorageNodeRegistryHttpContract) {
    const std::string register_payload = boost::json::serialize(boost::json::object{
        {"address", "127.0.0.1"},
        {"port", 9101},
        {"state", "active"},
    });

    const HttpResponse registered =
        http_exchange(server_->port(), beast_http::verb::put, "/v1/storage-nodes/m8-http-node-a", register_payload,
                      "application/json");

    ASSERT_EQ(registered.result(), beast_http::status::ok);
    const boost::json::object registered_body = boost::json::parse(registered.body()).as_object();
    EXPECT_EQ(registered_body.size(), 4U);
    EXPECT_EQ(registered_body.at("node_id").as_string(), "m8-http-node-a");
    EXPECT_EQ(registered_body.at("address").as_string(), "127.0.0.1");
    EXPECT_EQ(json_uint64(registered_body.at("port")), 9101U);
    EXPECT_EQ(registered_body.at("state").as_string(), "active");

    const HttpResponse loaded =
        http_exchange(server_->port(), beast_http::verb::get, "/v1/storage-nodes/m8-http-node-a");
    ASSERT_EQ(loaded.result(), beast_http::status::ok);
    EXPECT_EQ(boost::json::parse(loaded.body()).at("node_id").as_string(), "m8-http-node-a");

    const HttpResponse listed = http_exchange(server_->port(), beast_http::verb::get, "/v1/storage-nodes");
    ASSERT_EQ(listed.result(), beast_http::status::ok);
    const boost::json::object listed_body = boost::json::parse(listed.body()).as_object();
    const boost::json::array& nodes = listed_body.at("nodes").as_array();
    ASSERT_EQ(nodes.size(), 1U);
    EXPECT_EQ(nodes.at(0).at("node_id").as_string(), "m8-http-node-a");
}

TEST_F(MetadataServiceM8ApiTest, MultiNodeUploadSessionHttpContract) {
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    track_artifact(artifact_id);
    track_session(session_id);

    register_storage_node(*repository_, "m8-http-node-a", "127.0.0.1", 9101);
    register_storage_node(*repository_, "m8-http-node-b", "127.0.0.2", 9102);
    register_storage_node(*repository_, "m8-http-node-c", "127.0.0.3", 9103);

    repository_->create_artifact(Artifact{artifact_id, "m8-http-upload-" + session_id.str(), "m8-http-project"});

    const std::vector<std::string> placement{"m8-http-node-a", "m8-http-node-b", "m8-http-node-c"};
    const std::string payload = make_multi_node_session_json(session_id, artifact_id, 2U, placement);
    const HttpResponse created =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions", payload, "application/json");

    ASSERT_EQ(created.result(), beast_http::status::ok);
    const boost::json::object body = boost::json::parse(created.body()).as_object();
    EXPECT_EQ(body.at("session_id").as_string(), session_id.str());
    EXPECT_EQ(json_uint64(body.at("replication_factor")), 2U);
    EXPECT_EQ(body.at("target_node_id").as_string(), "m8-http-node-a");

    const boost::json::array& placement_array = body.at("placement_node_ids").as_array();
    ASSERT_EQ(placement_array.size(), 3U);
    EXPECT_EQ(placement_array.at(0).as_string(), "m8-http-node-a");
    EXPECT_EQ(placement_array.at(1).as_string(), "m8-http-node-b");
    EXPECT_EQ(placement_array.at(2).as_string(), "m8-http-node-c");

    boost::json::object mismatch = boost::json::parse(payload).as_object();
    mismatch["target_node_id"] = "m8-http-node-b";
    const HttpResponse mismatched = http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                                                  boost::json::serialize(mismatch), "application/json");
    EXPECT_EQ(mismatched.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(mismatched.body()).at("error").as_string(), "invalid_request");

    const UuidV7 unknown_session_id = UuidV7::generate();
    track_session(unknown_session_id);
    const std::string unknown_payload =
        make_multi_node_session_json(unknown_session_id, artifact_id, 1U, {"m8-http-missing"});
    const HttpResponse unknown = http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                                               unknown_payload, "application/json");
    EXPECT_EQ(unknown.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(unknown.body()).at("error").as_string(), "invalid_request");

    register_storage_node(*repository_, "m8-http-node-drain", "127.0.0.1", 9104, StorageNodeState::Draining);
    const UuidV7 drain_session_id = UuidV7::generate();
    track_session(drain_session_id);
    const std::string drain_payload =
        make_multi_node_session_json(drain_session_id, artifact_id, 1U, {"m8-http-node-drain"});
    const HttpResponse draining = http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                                                drain_payload, "application/json");
    EXPECT_EQ(draining.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(draining.body()).at("error").as_string(), "invalid_request");

    register_storage_node(*repository_, "m8-http-node-disabled", "127.0.0.1", 9105, StorageNodeState::Disabled);
    const UuidV7 disabled_session_id = UuidV7::generate();
    track_session(disabled_session_id);
    const std::string disabled_payload =
        make_multi_node_session_json(disabled_session_id, artifact_id, 1U, {"m8-http-node-disabled"});
    const HttpResponse disabled = http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                                                disabled_payload, "application/json");
    EXPECT_EQ(disabled.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(disabled.body()).at("error").as_string(), "invalid_request");

    register_storage_node(*repository_, "m8-http-node-a", "127.0.0.1", 9101, StorageNodeState::Disabled);
    const HttpResponse resumed =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions", payload, "application/json");
    ASSERT_EQ(resumed.result(), beast_http::status::ok);
    EXPECT_EQ(boost::json::parse(resumed.body()).at("placement_node_ids").as_array().at(0).as_string(),
              "m8-http-node-a");
}

TEST_F(MetadataServiceM8ApiTest, AutomaticMultiNodeRestorePlanHttpContract) {
    const UuidV7 artifact_id = UuidV7::generate();
    track_artifact(artifact_id);

    register_storage_node(*repository_, "m8-http-node-a", "127.0.0.1", 9101);
    register_storage_node(*repository_, "m8-http-node-b", "127.0.0.2", 9102);
    register_storage_node(*repository_, "m8-http-node-c", "127.0.0.3", 9103);

    repository_->create_artifact(Artifact{artifact_id, "m8-http-restore-" + artifact_id.str(), "m8-http-project"});

    const auto descriptor =
        make_m8_layout("m8-http-restore-object", "m8-http-restore-chunk-a", "m8-http-restore-chunk-b");
    track_object(descriptor.object_id());

    const std::vector<std::string> placement{"m8-http-node-a", "m8-http-node-b", "m8-http-node-c"};

    for (const ChunkRef& chunk : descriptor.layout().chunks()) {
        const std::vector<std::string> desired = select_replica_nodes(chunk.chunk_id, placement, 2U);
        for (const std::string& node_id : desired) {
            register_chunk_on_node(*repository_, chunk, node_id, StorageLocationState::Available);
        }
    }

    const ArtifactVersion version =
        register_committed_version(*repository_, artifact_id, descriptor, artifact_id.str());

    const HttpResponse response =
        http_exchange(server_->port(), beast_http::verb::get,
                      std::string{"/v1/artifact-versions/"} + version.version_id() + "/restore-plan");

    ASSERT_EQ(response.result(), beast_http::status::ok);
    const boost::json::object body = boost::json::parse(response.body()).as_object();
    EXPECT_EQ(body.size(), 9U);
    EXPECT_EQ(body.at("version_id").as_string(), version.version_id());
    EXPECT_EQ(body.at("artifact_id").as_string(), artifact_id.str());
    EXPECT_EQ(json_uint64(body.at("chunk_count")), 2U);

    const boost::json::array& chunks = body.at("chunks").as_array();
    ASSERT_EQ(chunks.size(), 2U);
    EXPECT_FALSE(chunks.at(0).at("sources").as_array().empty());
}

TEST_F(MetadataServiceM8ApiTest, ReplicationRunLifecycleHttpContract) {
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 run_id = UuidV7::generate();
    track_artifact(artifact_id);
    track_replication_run(run_id);

    register_storage_node(*repository_, "m8-http-node-a", "127.0.0.1", 9101);
    register_storage_node(*repository_, "m8-http-node-b", "127.0.0.2", 9102);
    register_storage_node(*repository_, "m8-http-node-c", "127.0.0.3", 9103);

    repository_->create_artifact(Artifact{artifact_id, "m8-http-replication-" + artifact_id.str(), "m8-http-project"});

    const auto descriptor =
        make_m8_layout("m8-http-replication-object", "m8-http-replication-chunk-c", "m8-http-replication-chunk-d");
    track_object(descriptor.object_id());

    const std::vector<std::string> placement{"m8-http-node-a", "m8-http-node-b", "m8-http-node-c"};

    for (const ChunkRef& chunk : descriptor.layout().chunks()) {
        const std::vector<std::string> desired = select_replica_nodes(chunk.chunk_id, placement, 2U);
        register_chunk_on_node(*repository_, chunk, desired.front(), StorageLocationState::Available);
    }

    const ArtifactVersion version =
        register_committed_version(*repository_, artifact_id, descriptor, artifact_id.str());

    const std::string start_payload = boost::json::serialize(boost::json::object{
        {"replication_run_id", run_id.str()},
        {"version_id", version.version_id()},
        {"replication_factor", 2},
    });

    const HttpResponse started = http_exchange(server_->port(), beast_http::verb::post, "/v1/replication-runs",
                                               start_payload, "application/json");

    ASSERT_EQ(started.result(), beast_http::status::ok);
    const boost::json::object started_body = boost::json::parse(started.body()).as_object();
    EXPECT_EQ(started_body.size(), 12U);
    EXPECT_EQ(started_body.at("replication_run_id").as_string(), run_id.str());
    EXPECT_EQ(started_body.at("state").as_string(), "open");

    const HttpResponse plan = http_exchange(server_->port(), beast_http::verb::get,
                                            std::string{"/v1/replication-runs/"} + run_id.str() + "/plan");
    ASSERT_EQ(plan.result(), beast_http::status::ok);
    EXPECT_EQ(boost::json::parse(plan.body()).at("replication_run_id").as_string(), run_id.str());

    for (const ChunkRef& chunk : descriptor.layout().chunks()) {
        const std::vector<std::string> desired = select_replica_nodes(chunk.chunk_id, placement, 2U);
        for (const std::string& node_id : desired) {
            register_chunk_on_node(*repository_, chunk, node_id, StorageLocationState::Available);
        }
    }

    const std::string complete_payload = boost::json::serialize(boost::json::object{
        {"chunks_scanned", 2},
        {"chunks_under_replicated", 2},
        {"replicas_verified", 0},
        {"replicas_written", 4},
        {"bytes_copied", 12},
        {"source_failovers", 0},
    });

    const HttpResponse completed = http_exchange(server_->port(), beast_http::verb::post,
                                                 std::string{"/v1/replication-runs/"} + run_id.str() + "/complete",
                                                 complete_payload, "application/json");

    ASSERT_EQ(completed.result(), beast_http::status::ok);
    EXPECT_EQ(boost::json::parse(completed.body()).at("state").as_string(), "completed");
}

TEST_F(MetadataServiceM8ApiTest, MultiNodeAndReplicationErrorsAreMappedStrictly) {
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 run_id = UuidV7::generate();
    const UuidV7 gc_run_id = UuidV7::generate();
    track_artifact(artifact_id);
    track_replication_run(run_id);
    track_gc_run(gc_run_id);

    register_storage_node(*repository_, "m8-http-node-a", "127.0.0.1", 9101);
    register_storage_node(*repository_, "m8-http-node-b", "127.0.0.2", 9102);
    register_storage_node(*repository_, "m8-http-node-c", "127.0.0.3", 9103);

    repository_->create_artifact(Artifact{artifact_id, "m8-http-errors-" + artifact_id.str(), "m8-http-project"});

    const auto descriptor = make_m8_layout("m8-http-errors-object", "m8-http-errors-chunk-e", "m8-http-errors-chunk-f");
    track_object(descriptor.object_id());

    const ArtifactVersion version =
        register_committed_version(*repository_, artifact_id, descriptor, artifact_id.str());

    const std::vector<std::string> placement{"m8-http-node-a", "m8-http-node-b", "m8-http-node-c"};
    for (const ChunkRef& chunk : descriptor.layout().chunks()) {
        const std::vector<std::string> desired = select_replica_nodes(chunk.chunk_id, placement, 2U);
        for (const std::string& node_id : desired) {
            register_chunk_on_node(*repository_, chunk, node_id, StorageLocationState::Available);
        }
    }

    const HttpResponse missing_node =
        http_exchange(server_->port(), beast_http::verb::get, "/v1/storage-nodes/m8-http-missing");
    EXPECT_EQ(missing_node.result(), beast_http::status::not_found);
    EXPECT_EQ(boost::json::parse(missing_node.body()).at("error").as_string(), "storage_node_not_found");

    const std::string start_payload = boost::json::serialize(boost::json::object{
        {"replication_run_id", run_id.str()},
        {"version_id", version.version_id()},
        {"replication_factor", 2},
    });
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/replication-runs", start_payload,
                            "application/json")
                  .result(),
              beast_http::status::ok);

    const std::string gc_payload = boost::json::serialize(boost::json::object{
        {"gc_run_id", gc_run_id.str()},
        {"target_node_id", "m8-http-node-a"},
        {"dry_run", false},
    });
    const HttpResponse gc_blocked =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/gc-runs", gc_payload, "application/json");
    EXPECT_EQ(gc_blocked.result(), beast_http::status::conflict);
    EXPECT_EQ(boost::json::parse(gc_blocked.body()).at("error").as_string(), "replication_in_progress");

    const HttpResponse missing_run =
        http_exchange(server_->port(), beast_http::verb::get, "/v1/replication-runs/" + UuidV7::generate().str());
    EXPECT_EQ(missing_run.result(), beast_http::status::not_found);
    EXPECT_EQ(boost::json::parse(missing_run.body()).at("error").as_string(), "replication_run_not_found");
}

}  // namespace
