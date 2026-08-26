#include <gtest/gtest.h>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <pqxx/pqxx>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "aistore/http/http_server.hpp"
#include "aistore/metadata/artifact_model.hpp"
#include "aistore/metadata/chunking.hpp"
#include "aistore/metadata/object.hpp"
#include "aistore/metadata/object_layout.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/postgres_metadata_repository.hpp"
#include "aistore/metadata/upload_session.hpp"
#include "aistore/metadata/uuid_v7.hpp"
#include "aistore/service/metadata_service.hpp"

namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace beast_http = beast::http;

using tcp = asio::ip::tcp;

using aistore::http::HttpRequest;
using aistore::http::HttpResponse;
using aistore::http::HttpServer;
using aistore::http::HttpServerConfig;
using aistore::metadata::Artifact;
using aistore::metadata::ArtifactVersion;
using aistore::metadata::ChunkingStrategy;
using aistore::metadata::ChunkRef;
using aistore::metadata::Object;
using aistore::metadata::ObjectLayout;
using aistore::metadata::ObjectLayoutDescriptor;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::UploadSession;
using aistore::metadata::UploadSessionState;
using aistore::metadata::UuidV7;
using aistore::metadata::VersionState;
using aistore::service::MetadataService;

constexpr std::uint64_t kMaxRequestBodyBytes = 8ULL * 1024ULL * 1024ULL;
constexpr std::uint64_t kChunkSizeBytes = 4194304ULL;

const std::string kSharedChunk(64, 'a');
const std::string kLiveOnlyChunk(64, 'b');
const std::string kUntrackedChunk(64, 'd');
const std::string kDeadOnlyChunk(64, 'c');

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
    request.set(beast_http::field::user_agent, "aistore-metadata-service-gc-api-test");

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

void register_live_gc_layout(PostgresMetadataRepository& repository, const UuidV7& artifact_id,
                             const ObjectLayoutDescriptor& descriptor, std::string marker) {
    repository.create_artifact(Artifact{artifact_id, "gc-http-" + marker, "m4gc-project"});
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

void expect_gc_run_shape(const boost::json::object& body) {
    EXPECT_EQ(body.size(), 14U);
    EXPECT_TRUE(body.at("gc_run_id").is_string());
    EXPECT_TRUE(body.at("target_node_id").is_string());
    EXPECT_TRUE(body.at("dry_run").is_bool());
    EXPECT_TRUE(body.at("state").is_string());
    EXPECT_TRUE(body.at("physical_chunks_scanned").is_int64() || body.at("physical_chunks_scanned").is_uint64());
    EXPECT_TRUE(body.at("physical_bytes_scanned").is_int64() || body.at("physical_bytes_scanned").is_uint64());
    EXPECT_TRUE(body.at("collectible_chunks").is_int64() || body.at("collectible_chunks").is_uint64());
    EXPECT_TRUE(body.at("collectible_bytes").is_int64() || body.at("collectible_bytes").is_uint64());
    EXPECT_TRUE(body.at("physically_deleted_chunks").is_int64() || body.at("physically_deleted_chunks").is_uint64());
    EXPECT_TRUE(body.at("physically_deleted_bytes").is_int64() || body.at("physically_deleted_bytes").is_uint64());
    EXPECT_TRUE(body.at("storage_locations_swept").is_int64() || body.at("storage_locations_swept").is_uint64());
    EXPECT_TRUE(body.at("chunk_rows_swept").is_int64() || body.at("chunk_rows_swept").is_uint64());
    EXPECT_TRUE(body.at("object_layouts_swept").is_int64() || body.at("object_layouts_swept").is_uint64());
    EXPECT_TRUE(body.at("objects_swept").is_int64() || body.at("objects_swept").is_uint64());
}

class MetadataServiceGcApiTest : public ::testing::Test {
   protected:
    void SetUp() override {
        repository_.emplace(test_database_connection_string());
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

        transaction.commit();
    }

    [[nodiscard]] std::string make_start_gc_json(const UuidV7& gc_run_id, std::string_view target_node_id,
                                                 bool dry_run) const {
        return boost::json::serialize(boost::json::object{
            {"gc_run_id", gc_run_id.str()},
            {"target_node_id", target_node_id},
            {"dry_run", dry_run},
        });
    }

    [[nodiscard]] std::string make_session_json(const UuidV7& session_id, const UuidV7& artifact_id,
                                                std::string_view target_node_id = "m4gc-target") const {
        return boost::json::serialize(boost::json::object{
            {"session_id", session_id.str()},
            {"artifact_id", artifact_id.str()},
            {"target_node_id", target_node_id},
            {"chunking_strategy", "fixed-size"},
            {"chunking_parameters",
             boost::json::object{
                 {"chunk_size_bytes", kChunkSizeBytes},
             }},
            {"parent_version_id", nullptr},
            {"immutable_metadata",
             boost::json::object{
                 {"source", "m4gc"},
             }},
        });
    }

    void track_gc_run(UuidV7 gc_run_id) { owned_gc_run_ids_.push_back(std::move(gc_run_id)); }

    void track_session(UuidV7 session_id) { owned_session_ids_.push_back(std::move(session_id)); }

    void track_artifact(UuidV7 artifact_id) { owned_artifact_ids_.push_back(std::move(artifact_id)); }

    void track_object(std::string object_id) { owned_object_ids_.push_back(std::move(object_id)); }

    std::vector<UuidV7> owned_gc_run_ids_;
    std::vector<UuidV7> owned_session_ids_;
    std::vector<UuidV7> owned_artifact_ids_;
    std::vector<std::string> owned_object_ids_;
    std::optional<PostgresMetadataRepository> repository_;
    std::optional<MetadataService> service_;
    std::optional<RunningHttpServer> server_;
};

TEST_F(MetadataServiceGcApiTest, GcRunLifecycleHttpContract) {
    const UuidV7 gc_run_id = UuidV7::generate();
    track_gc_run(gc_run_id);

    const std::string start_payload = make_start_gc_json(gc_run_id, "m4gc-node-a", false);
    const HttpResponse started =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/gc-runs", start_payload, "application/json");

    ASSERT_EQ(started.result(), beast_http::status::ok);
    const boost::json::object started_body = boost::json::parse(started.body()).as_object();
    expect_gc_run_shape(started_body);
    EXPECT_EQ(started_body.at("gc_run_id").as_string(), gc_run_id.str());
    EXPECT_EQ(started_body.at("target_node_id").as_string(), "m4gc-node-a");
    EXPECT_FALSE(started_body.at("dry_run").as_bool());
    EXPECT_EQ(started_body.at("state").as_string(), "open");

    const HttpResponse idempotent =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/gc-runs", start_payload, "application/json");
    EXPECT_EQ(idempotent.result(), beast_http::status::ok);
    EXPECT_EQ(boost::json::parse(idempotent.body()).at("gc_run_id").as_string(), gc_run_id.str());

    const HttpResponse loaded =
        http_exchange(server_->port(), beast_http::verb::get, std::string{"/v1/gc-runs/"} + gc_run_id.str());
    ASSERT_EQ(loaded.result(), beast_http::status::ok);
    const boost::json::object loaded_body = boost::json::parse(loaded.body()).as_object();
    expect_gc_run_shape(loaded_body);
    EXPECT_EQ(loaded_body.at("state").as_string(), "open");

    const std::string complete_payload = boost::json::serialize(boost::json::object{
        {"physical_chunks_scanned", 2},
        {"physical_bytes_scanned", 8},
        {"collectible_chunks", 1},
        {"collectible_bytes", 4},
        {"physically_deleted_chunks", 1},
        {"physically_deleted_bytes", 4},
    });
    const HttpResponse completed = http_exchange(server_->port(), beast_http::verb::post,
                                                 std::string{"/v1/gc-runs/"} + gc_run_id.str() + "/complete",
                                                 complete_payload, "application/json");

    ASSERT_EQ(completed.result(), beast_http::status::ok);
    const boost::json::object completed_body = boost::json::parse(completed.body()).as_object();
    expect_gc_run_shape(completed_body);
    EXPECT_EQ(completed_body.at("state").as_string(), "completed");
    EXPECT_EQ(json_uint64(completed_body.at("physical_chunks_scanned")), 2U);
    EXPECT_EQ(json_uint64(completed_body.at("physically_deleted_bytes")), 4U);
}

TEST_F(MetadataServiceGcApiTest, GcChunkClassificationHttpContract) {
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 gc_run_id = UuidV7::generate();
    track_artifact(artifact_id);
    track_gc_run(gc_run_id);

    const auto live_layout = make_gc_layout('1', 'a', 'b');
    const auto dead_layout = make_gc_layout('2', 'a', 'c');
    track_object(live_layout.object_id());
    track_object(dead_layout.object_id());
    register_live_gc_layout(*repository_, artifact_id, live_layout, UuidV7::generate().str());
    register_dead_gc_layout(*repository_, dead_layout);

    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/gc-runs",
                            make_start_gc_json(gc_run_id, "m4gc-node-a", false), "application/json")
                  .result(),
              beast_http::status::ok);

    const std::string classify_payload = boost::json::serialize(boost::json::object{
        {"chunk_ids", boost::json::array{kLiveOnlyChunk, kSharedChunk, kUntrackedChunk}},
    });
    const HttpResponse classified = http_exchange(server_->port(), beast_http::verb::post,
                                                  std::string{"/v1/gc-runs/"} + gc_run_id.str() + "/classify",
                                                  classify_payload, "application/json");

    ASSERT_EQ(classified.result(), beast_http::status::ok);
    const boost::json::object classified_body = boost::json::parse(classified.body()).as_object();
    EXPECT_EQ(classified_body.size(), 2U);
    EXPECT_EQ(classified_body.at("gc_run_id").as_string(), gc_run_id.str());

    const boost::json::array& chunks = classified_body.at("chunks").as_array();
    ASSERT_EQ(chunks.size(), 3U);
    EXPECT_EQ(chunks.at(0).at("chunk_id").as_string(), kLiveOnlyChunk);
    EXPECT_FALSE(chunks.at(0).at("collectible").as_bool());
    EXPECT_EQ(chunks.at(1).at("chunk_id").as_string(), kSharedChunk);
    EXPECT_FALSE(chunks.at(1).at("collectible").as_bool());
    EXPECT_EQ(chunks.at(2).at("chunk_id").as_string(), kUntrackedChunk);
    EXPECT_TRUE(chunks.at(2).at("collectible").as_bool());

    const std::string dead_only_payload = boost::json::serialize(boost::json::object{
        {"chunk_ids", boost::json::array{kDeadOnlyChunk}},
    });
    const HttpResponse dead_only = http_exchange(server_->port(), beast_http::verb::post,
                                                 std::string{"/v1/gc-runs/"} + gc_run_id.str() + "/classify",
                                                 dead_only_payload, "application/json");

    ASSERT_EQ(dead_only.result(), beast_http::status::ok);
    EXPECT_TRUE(boost::json::parse(dead_only.body()).at("chunks").as_array().at(0).at("collectible").as_bool());
}

TEST_F(MetadataServiceGcApiTest, GcErrorsAndMethodValidation) {
    const UuidV7 gc_run_id = UuidV7::generate();
    const UuidV7 other_gc_run_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    track_gc_run(gc_run_id);
    track_gc_run(other_gc_run_id);
    track_session(session_id);
    track_artifact(artifact_id);

    repository_->create_artifact(Artifact{artifact_id, "gc-http-errors-" + session_id.str(), "m4gc-project"});
    repository_->create_upload_session(UploadSession{
        session_id,
        artifact_id,
        "m4gc-target",
        ChunkingStrategy::FixedSize,
        4,
        std::nullopt,
        UploadSession::ImmutableMetadata{},
        UploadSessionState::Open,
        std::nullopt,
    });

    const HttpResponse blocked = http_exchange(server_->port(), beast_http::verb::post, "/v1/gc-runs",
                                               make_start_gc_json(gc_run_id, "m4gc-node-a", false), "application/json");
    EXPECT_EQ(blocked.result(), beast_http::status::conflict);
    EXPECT_EQ(boost::json::parse(blocked.body()).at("error").as_string(), "gc_blocked_by_open_upload_sessions");

    repository_->abort_upload_session(session_id);

    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/gc-runs",
                            make_start_gc_json(gc_run_id, "m4gc-node-a", false), "application/json")
                  .result(),
              beast_http::status::ok);

    const HttpResponse another_open =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/gc-runs",
                      make_start_gc_json(other_gc_run_id, "m4gc-node-b", false), "application/json");
    EXPECT_EQ(another_open.result(), beast_http::status::conflict);
    EXPECT_EQ(boost::json::parse(another_open.body()).at("error").as_string(), "gc_already_in_progress");

    const HttpResponse conflict =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/gc-runs",
                      make_start_gc_json(gc_run_id, "m4gc-node-b", false), "application/json");
    EXPECT_EQ(conflict.result(), beast_http::status::conflict);
    EXPECT_EQ(boost::json::parse(conflict.body()).at("error").as_string(), "gc_run_conflict");

    const UuidV7 missing = UuidV7::generate();
    const HttpResponse not_found =
        http_exchange(server_->port(), beast_http::verb::get, std::string{"/v1/gc-runs/"} + missing.str());
    EXPECT_EQ(not_found.result(), beast_http::status::not_found);
    EXPECT_EQ(boost::json::parse(not_found.body()).at("error").as_string(), "gc_run_not_found");

    const std::string complete_payload = boost::json::serialize(boost::json::object{
        {"physical_chunks_scanned", 0},
        {"physical_bytes_scanned", 0},
        {"collectible_chunks", 0},
        {"collectible_bytes", 0},
        {"physically_deleted_chunks", 0},
        {"physically_deleted_bytes", 0},
    });
    const HttpResponse completed = http_exchange(server_->port(), beast_http::verb::post,
                                                 std::string{"/v1/gc-runs/"} + gc_run_id.str() + "/complete",
                                                 complete_payload, "application/json");
    ASSERT_EQ(completed.result(), beast_http::status::ok);

    const HttpResponse classify_after_complete = http_exchange(
        server_->port(), beast_http::verb::post, std::string{"/v1/gc-runs/"} + gc_run_id.str() + "/classify",
        boost::json::serialize(boost::json::object{
            {"chunk_ids", boost::json::array{kUntrackedChunk}},
        }),
        "application/json");
    EXPECT_EQ(classify_after_complete.result(), beast_http::status::conflict);
    EXPECT_EQ(boost::json::parse(classify_after_complete.body()).at("error").as_string(), "gc_run_not_open");

    const HttpResponse invalid_gc_run = http_exchange(server_->port(), beast_http::verb::get, "/v1/gc-runs/not-a-uuid");
    EXPECT_EQ(invalid_gc_run.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(invalid_gc_run.body()).at("error").as_string(), "invalid_gc_run_id");

    const HttpResponse collection_get = http_exchange(server_->port(), beast_http::verb::get, "/v1/gc-runs");
    EXPECT_EQ(collection_get.result(), beast_http::status::method_not_allowed);
    EXPECT_EQ(collection_get[beast_http::field::allow], "POST");
    EXPECT_EQ(boost::json::parse(collection_get.body()).at("error").as_string(), "method_not_allowed");

    const HttpResponse item_post =
        http_exchange(server_->port(), beast_http::verb::post, std::string{"/v1/gc-runs/"} + gc_run_id.str());
    EXPECT_EQ(item_post.result(), beast_http::status::method_not_allowed);
    EXPECT_EQ(item_post[beast_http::field::allow], "GET");
}

TEST_F(MetadataServiceGcApiTest, CreateUploadSessionReturnsConflictWhileGcOpen) {
    const UuidV7 gc_run_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    track_gc_run(gc_run_id);
    track_session(session_id);
    track_artifact(artifact_id);

    repository_->create_artifact(Artifact{artifact_id, "gc-http-upload-" + session_id.str(), "m4gc-project"});

    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/gc-runs",
                            make_start_gc_json(gc_run_id, "m4gc-node-a", false), "application/json")
                  .result(),
              beast_http::status::ok);

    const HttpResponse response = http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                                                make_session_json(session_id, artifact_id), "application/json");

    EXPECT_EQ(response.result(), beast_http::status::conflict);
    EXPECT_EQ(boost::json::parse(response.body()).at("error").as_string(), "gc_in_progress");
}

}  // namespace
