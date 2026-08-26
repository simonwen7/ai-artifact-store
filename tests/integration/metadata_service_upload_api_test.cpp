#include <gtest/gtest.h>

#include <array>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <optional>
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
#include "aistore/metadata/chunk_metadata.hpp"
#include "aistore/metadata/postgres_metadata_repository.hpp"
#include "aistore/metadata/storage_location.hpp"
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
using aistore::metadata::ChunkMetadata;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::StorageLocation;
using aistore::metadata::StorageLocationState;
using aistore::metadata::UploadSessionState;
using aistore::metadata::UuidV7;
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
    request.set(beast_http::field::user_agent, "aistore-metadata-service-upload-api-test");

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

class MetadataServiceUploadApiTest : public ::testing::Test {
   protected:
    void SetUp() override {
        artifact_id_ = UuidV7::generate();
        session_id_ = UuidV7::generate();
        repository_.emplace(test_database_connection_string());
        repository_->create_artifact(
            Artifact{artifact_id_, std::string{"m4s2-artifact-"} + artifact_id_.str(), "m4s2-project"});
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

        for (const UuidV7& session_id : owned_session_ids_) {
            transaction
                .exec(
                    "DELETE FROM upload_sessions "
                    "WHERE session_id = $1::uuid",
                    pqxx::params{
                        session_id.str(),
                    })
                .no_rows();
        }

        transaction
            .exec(
                "DELETE FROM upload_sessions "
                "WHERE session_id = $1::uuid",
                pqxx::params{
                    session_id_.str(),
                })
            .no_rows();

        transaction
            .exec(
                "DELETE FROM storage_locations "
                "WHERE node_id LIKE 'm4s2-%'")
            .no_rows();

        for (const std::string& chunk_id : owned_chunk_ids_) {
            transaction
                .exec(
                    "DELETE FROM storage_locations "
                    "WHERE chunk_id = $1",
                    pqxx::params{
                        chunk_id,
                    })
                .no_rows();

            transaction
                .exec(
                    "DELETE FROM chunks "
                    "WHERE chunk_id = $1",
                    pqxx::params{
                        chunk_id,
                    })
                .no_rows();
        }

        transaction
            .exec(
                "DELETE FROM artifacts "
                "WHERE artifact_id = $1::uuid",
                pqxx::params{
                    artifact_id_.str(),
                })
            .no_rows();

        transaction.commit();
    }

    [[nodiscard]] std::string make_session_json(const UuidV7& session_id, const UuidV7& artifact_id,
                                                std::string_view target_node_id = "m4s2-target") const {
        boost::json::object body{
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
                 {"source", "m4s2"},
             }},
        };

        return boost::json::serialize(body);
    }

    [[nodiscard]] std::string make_negotiate_json(const UuidV7& session_id,
                                                  const std::vector<ChunkMetadata>& chunks) const {
        boost::json::array chunk_array;

        for (const ChunkMetadata& chunk : chunks) {
            chunk_array.push_back(boost::json::object{
                {"chunk_id", chunk.chunk_id},
                {"size_bytes", chunk.size_bytes},
            });
        }

        return boost::json::serialize(boost::json::object{
            {"session_id", session_id.str()},
            {"chunks", std::move(chunk_array)},
        });
    }

    void track_chunk(std::string chunk_id) { owned_chunk_ids_.push_back(std::move(chunk_id)); }

    void track_session(UuidV7 session_id) { owned_session_ids_.push_back(std::move(session_id)); }

    UuidV7 artifact_id_{UuidV7::generate()};
    UuidV7 session_id_{UuidV7::generate()};
    std::vector<std::string> owned_chunk_ids_;
    std::vector<UuidV7> owned_session_ids_;
    std::optional<PostgresMetadataRepository> repository_;
    std::optional<MetadataService> service_;
    std::optional<RunningHttpServer> server_;
};

TEST_F(MetadataServiceUploadApiTest, CreatesUploadSessionOverHttp) {
    const HttpResponse response = http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                                                make_session_json(session_id_, artifact_id_), "application/json");

    ASSERT_EQ(response.result(), beast_http::status::ok);

    const boost::json::value body = boost::json::parse(response.body());
    ASSERT_TRUE(body.is_object());
    EXPECT_EQ(body.at("session_id").as_string(), session_id_.str());
    EXPECT_EQ(body.at("artifact_id").as_string(), artifact_id_.str());
    EXPECT_EQ(body.at("target_node_id").as_string(), "m4s2-target");
    EXPECT_EQ(body.at("chunking_strategy").as_string(), "fixed-size");
    EXPECT_EQ(json_uint64(body.at("chunking_parameters").as_object().at("chunk_size_bytes")), kChunkSizeBytes);
    EXPECT_TRUE(body.at("parent_version_id").is_null());
    EXPECT_EQ(body.at("immutable_metadata").as_object().at("source").as_string(), "m4s2");
    EXPECT_EQ(body.at("state").as_string(), "open");
    EXPECT_TRUE(body.at("finalized_version_id").is_null());

    const auto stored = repository_->get_upload_session(session_id_);
    ASSERT_TRUE(stored.has_value());
    EXPECT_EQ(stored->state(), UploadSessionState::Open);
}

TEST_F(MetadataServiceUploadApiTest, CreateUploadSessionIsIdempotent) {
    const std::string payload = make_session_json(session_id_, artifact_id_);

    const HttpResponse first =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions", payload, "application/json");
    const HttpResponse second =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions", payload, "application/json");

    EXPECT_EQ(first.result(), beast_http::status::ok);
    EXPECT_EQ(second.result(), beast_http::status::ok);

    pqxx::connection connection{test_database_connection_string()};
    pqxx::work transaction{connection};
    const long long count = transaction.query_value<long long>(
        "SELECT COUNT(*) FROM upload_sessions WHERE session_id = $1::uuid", pqxx::params{session_id_.str()});
    transaction.commit();

    EXPECT_EQ(count, 1);
}

TEST_F(MetadataServiceUploadApiTest, ConflictingUploadSessionCreateReturns409) {
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                            make_session_json(session_id_, artifact_id_, "m4s2-target"), "application/json")
                  .result(),
              beast_http::status::ok);

    const HttpResponse conflict =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                      make_session_json(session_id_, artifact_id_, "m4s2-other"), "application/json");

    EXPECT_EQ(conflict.result(), beast_http::status::conflict);
    EXPECT_EQ(boost::json::parse(conflict.body()).at("error").as_string(), "upload_session_conflict");

    const auto stored = repository_->get_upload_session(session_id_);
    ASSERT_TRUE(stored.has_value());
    EXPECT_EQ(stored->target_node_id(), "m4s2-target");
}

TEST_F(MetadataServiceUploadApiTest, GetsUploadSessionOverHttp) {
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                            make_session_json(session_id_, artifact_id_), "application/json")
                  .result(),
              beast_http::status::ok);

    const HttpResponse response =
        http_exchange(server_->port(), beast_http::verb::get, std::string{"/v1/upload-sessions/"} + session_id_.str());

    ASSERT_EQ(response.result(), beast_http::status::ok);
    const boost::json::value body = boost::json::parse(response.body());
    EXPECT_EQ(body.at("session_id").as_string(), session_id_.str());
    EXPECT_EQ(body.at("artifact_id").as_string(), artifact_id_.str());
    EXPECT_EQ(body.at("target_node_id").as_string(), "m4s2-target");
    EXPECT_EQ(body.at("state").as_string(), "open");
    EXPECT_TRUE(body.at("finalized_version_id").is_null());
}

TEST_F(MetadataServiceUploadApiTest, GetMissingUploadSessionReturns404) {
    const UuidV7 missing = UuidV7::generate();
    const HttpResponse response =
        http_exchange(server_->port(), beast_http::verb::get, std::string{"/v1/upload-sessions/"} + missing.str());

    EXPECT_EQ(response.result(), beast_http::status::not_found);
    EXPECT_EQ(boost::json::parse(response.body()).at("error").as_string(), "upload_session_not_found");
}

TEST_F(MetadataServiceUploadApiTest, GetMalformedUploadSessionIdReturns400) {
    const HttpResponse response =
        http_exchange(server_->port(), beast_http::verb::get, "/v1/upload-sessions/not-a-uuid");

    EXPECT_EQ(response.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(response.body()).at("error").as_string(), "invalid_session_id");
}

TEST_F(MetadataServiceUploadApiTest, AbortsOpenUploadSessionOverHttp) {
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                            make_session_json(session_id_, artifact_id_), "application/json")
                  .result(),
              beast_http::status::ok);

    const HttpResponse response = http_exchange(server_->port(), beast_http::verb::post,
                                                std::string{"/v1/upload-sessions/"} + session_id_.str() + "/abort");

    ASSERT_EQ(response.result(), beast_http::status::ok);
    EXPECT_EQ(boost::json::parse(response.body()).at("state").as_string(), "aborted");

    const auto stored = repository_->get_upload_session(session_id_);
    ASSERT_TRUE(stored.has_value());
    EXPECT_EQ(stored->state(), UploadSessionState::Aborted);
}

TEST_F(MetadataServiceUploadApiTest, AbortUploadSessionIsIdempotent) {
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                            make_session_json(session_id_, artifact_id_), "application/json")
                  .result(),
              beast_http::status::ok);

    const std::string abort_target = std::string{"/v1/upload-sessions/"} + session_id_.str() + "/abort";
    const HttpResponse first = http_exchange(server_->port(), beast_http::verb::post, abort_target);
    const HttpResponse second = http_exchange(server_->port(), beast_http::verb::post, abort_target);

    EXPECT_EQ(first.result(), beast_http::status::ok);
    EXPECT_EQ(second.result(), beast_http::status::ok);
    EXPECT_EQ(boost::json::parse(first.body()).at("state").as_string(), "aborted");
    EXPECT_EQ(boost::json::parse(second.body()).at("state").as_string(), "aborted");
}

TEST_F(MetadataServiceUploadApiTest, AbortMissingUploadSessionReturns404) {
    const UuidV7 missing = UuidV7::generate();
    const HttpResponse response = http_exchange(server_->port(), beast_http::verb::post,
                                                std::string{"/v1/upload-sessions/"} + missing.str() + "/abort");

    EXPECT_EQ(response.result(), beast_http::status::not_found);
    EXPECT_EQ(boost::json::parse(response.body()).at("error").as_string(), "upload_session_not_found");
}

TEST_F(MetadataServiceUploadApiTest, CreateUploadSessionRejectsMalformedJson) {
    const HttpResponse response =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions", "{", "application/json");

    EXPECT_EQ(response.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(response.body()).at("error").as_string(), "invalid_json");
}

TEST_F(MetadataServiceUploadApiTest, CreateUploadSessionRejectsInvalidShape) {
    boost::json::object body = boost::json::parse(make_session_json(session_id_, artifact_id_)).as_object();
    body["state"] = "open";

    const HttpResponse response = http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                                                boost::json::serialize(body), "application/json");

    EXPECT_EQ(response.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(response.body()).at("error").as_string(), "invalid_request");
}

TEST_F(MetadataServiceUploadApiTest, CreateUploadSessionRejectsMissingArtifact) {
    const UuidV7 missing_artifact = UuidV7::generate();
    const HttpResponse response = http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                                                make_session_json(session_id_, missing_artifact), "application/json");

    EXPECT_EQ(response.result(), beast_http::status::not_found);
    EXPECT_EQ(boost::json::parse(response.body()).at("error").as_string(), "upload_session_prerequisite_not_found");
}

TEST_F(MetadataServiceUploadApiTest, UploadSessionCollectionRejectsUnsupportedMethod) {
    const HttpResponse response = http_exchange(server_->port(), beast_http::verb::get, "/v1/upload-sessions");

    EXPECT_EQ(response.result(), beast_http::status::method_not_allowed);
    EXPECT_EQ(response[beast_http::field::allow], "POST");
    EXPECT_EQ(boost::json::parse(response.body()).at("error").as_string(), "method_not_allowed");
}

TEST_F(MetadataServiceUploadApiTest, NegotiationRegistersPreviouslyUnknownChunks) {
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                            make_session_json(session_id_, artifact_id_), "application/json")
                  .result(),
              beast_http::status::ok);

    const std::string chunk_a = sha256_hex(std::string{"m4s2-unknown-a-"} + UuidV7::generate().str());
    const std::string chunk_b = sha256_hex(std::string{"m4s2-unknown-b-"} + UuidV7::generate().str());
    track_chunk(chunk_a);
    track_chunk(chunk_b);

    const std::vector<ChunkMetadata> chunks{
        {.chunk_id = chunk_a, .size_bytes = 11},
        {.chunk_id = chunk_b, .size_bytes = 22},
    };

    const HttpResponse response = http_exchange(server_->port(), beast_http::verb::post, "/v1/chunks/negotiate",
                                                make_negotiate_json(session_id_, chunks), "application/json");

    ASSERT_EQ(response.result(), beast_http::status::ok);
    const boost::json::value parsed = boost::json::parse(response.body());
    const boost::json::array& result_chunks = parsed.at("chunks").as_array();
    ASSERT_EQ(result_chunks.size(), 2U);

    for (const boost::json::value& entry : result_chunks) {
        EXPECT_FALSE(entry.at("metadata_was_known").as_bool());
        EXPECT_EQ(entry.at("availability").as_string(), "no_available_location");
        EXPECT_TRUE(entry.at("available_node_ids").as_array().empty());
    }

    EXPECT_EQ(repository_->get_chunk_size(chunk_a), 11U);
    EXPECT_EQ(repository_->get_chunk_size(chunk_b), 22U);
}

TEST_F(MetadataServiceUploadApiTest, NegotiationReportsAvailableOnTarget) {
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                            make_session_json(session_id_, artifact_id_, "m4s2-target"), "application/json")
                  .result(),
              beast_http::status::ok);

    const std::string chunk_id = sha256_hex(std::string{"m4s2-on-target-"} + UuidV7::generate().str());
    track_chunk(chunk_id);
    repository_->register_chunks({ChunkMetadata{.chunk_id = chunk_id, .size_bytes = 7}});
    repository_->register_storage_location(StorageLocation{
        .chunk_id = chunk_id,
        .node_id = "m4s2-target",
        .storage_path = "/m4s2/on-target",
        .state = StorageLocationState::Available,
    });

    const HttpResponse response =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/chunks/negotiate",
                      make_negotiate_json(session_id_, {{.chunk_id = chunk_id, .size_bytes = 7}}), "application/json");

    ASSERT_EQ(response.result(), beast_http::status::ok);
    const boost::json::value entry = boost::json::parse(response.body()).at("chunks").as_array().at(0);
    EXPECT_TRUE(entry.at("metadata_was_known").as_bool());
    EXPECT_EQ(entry.at("availability").as_string(), "available_on_target");
    EXPECT_EQ(entry.at("available_node_ids").as_array().at(0).as_string(), "m4s2-target");
}

TEST_F(MetadataServiceUploadApiTest, NegotiationReportsAvailableElsewhere) {
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                            make_session_json(session_id_, artifact_id_, "m4s2-target"), "application/json")
                  .result(),
              beast_http::status::ok);

    const std::string chunk_id = sha256_hex(std::string{"m4s2-elsewhere-"} + UuidV7::generate().str());
    track_chunk(chunk_id);
    repository_->register_chunks({ChunkMetadata{.chunk_id = chunk_id, .size_bytes = 9}});
    repository_->register_storage_location(StorageLocation{
        .chunk_id = chunk_id,
        .node_id = "m4s2-other",
        .storage_path = "/m4s2/elsewhere",
        .state = StorageLocationState::Available,
    });

    const HttpResponse response =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/chunks/negotiate",
                      make_negotiate_json(session_id_, {{.chunk_id = chunk_id, .size_bytes = 9}}), "application/json");

    ASSERT_EQ(response.result(), beast_http::status::ok);
    const boost::json::value entry = boost::json::parse(response.body()).at("chunks").as_array().at(0);
    EXPECT_EQ(entry.at("availability").as_string(), "available_elsewhere");
    ASSERT_EQ(entry.at("available_node_ids").as_array().size(), 1U);
    EXPECT_EQ(entry.at("available_node_ids").as_array().at(0).as_string(), "m4s2-other");
}

TEST_F(MetadataServiceUploadApiTest, NegotiationReportsKnownChunkWithNoAvailableLocation) {
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                            make_session_json(session_id_, artifact_id_), "application/json")
                  .result(),
              beast_http::status::ok);

    const std::string chunk_id = sha256_hex(std::string{"m4s2-unavailable-"} + UuidV7::generate().str());
    track_chunk(chunk_id);
    repository_->register_chunks({ChunkMetadata{.chunk_id = chunk_id, .size_bytes = 5}});
    repository_->register_storage_location(StorageLocation{
        .chunk_id = chunk_id,
        .node_id = "m4s2-missing",
        .storage_path = "/m4s2/missing",
        .state = StorageLocationState::Missing,
    });
    repository_->register_storage_location(StorageLocation{
        .chunk_id = chunk_id,
        .node_id = "m4s2-corrupt",
        .storage_path = "/m4s2/corrupt",
        .state = StorageLocationState::Corrupt,
    });

    const HttpResponse response =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/chunks/negotiate",
                      make_negotiate_json(session_id_, {{.chunk_id = chunk_id, .size_bytes = 5}}), "application/json");

    ASSERT_EQ(response.result(), beast_http::status::ok);
    const boost::json::value entry = boost::json::parse(response.body()).at("chunks").as_array().at(0);
    EXPECT_TRUE(entry.at("metadata_was_known").as_bool());
    EXPECT_EQ(entry.at("availability").as_string(), "no_available_location");
    EXPECT_TRUE(entry.at("available_node_ids").as_array().empty());
}

TEST_F(MetadataServiceUploadApiTest, NegotiationRejectsChunkSizeConflictWithoutPartialRegistration) {
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                            make_session_json(session_id_, artifact_id_), "application/json")
                  .result(),
              beast_http::status::ok);

    const std::string chunk_a = sha256_hex(std::string{"m4s2-conflict-a-"} + UuidV7::generate().str());
    const std::string chunk_b = sha256_hex(std::string{"m4s2-conflict-b-"} + UuidV7::generate().str());
    track_chunk(chunk_a);
    track_chunk(chunk_b);
    repository_->register_chunks({ChunkMetadata{.chunk_id = chunk_a, .size_bytes = 10}});

    const HttpResponse response = http_exchange(
        server_->port(), beast_http::verb::post, "/v1/chunks/negotiate",
        make_negotiate_json(session_id_,
                            {{.chunk_id = chunk_b, .size_bytes = 20}, {.chunk_id = chunk_a, .size_bytes = 11}}),
        "application/json");

    ASSERT_EQ(response.result(), beast_http::status::conflict);
    const boost::json::value body = boost::json::parse(response.body());
    EXPECT_EQ(body.at("error").as_string(), "chunk_size_conflict");
    EXPECT_EQ(body.at("chunk_id").as_string(), chunk_a);
    EXPECT_EQ(json_uint64(body.at("requested_size_bytes")), 11U);
    EXPECT_EQ(json_uint64(body.at("stored_size_bytes")), 10U);

    EXPECT_EQ(repository_->get_chunk_size(chunk_a), 10U);
    EXPECT_FALSE(repository_->get_chunk_size(chunk_b).has_value());
}

TEST_F(MetadataServiceUploadApiTest, NegotiationCollapsesDuplicateChunkIdsWithSameSize) {
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                            make_session_json(session_id_, artifact_id_), "application/json")
                  .result(),
              beast_http::status::ok);

    const std::string chunk_a = sha256_hex(std::string{"m4s2-dup-a-"} + UuidV7::generate().str());
    const std::string chunk_b = sha256_hex(std::string{"m4s2-dup-b-"} + UuidV7::generate().str());
    track_chunk(chunk_a);
    track_chunk(chunk_b);

    const HttpResponse response =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/chunks/negotiate",
                      make_negotiate_json(session_id_, {{.chunk_id = chunk_a, .size_bytes = 3},
                                                        {.chunk_id = chunk_b, .size_bytes = 4},
                                                        {.chunk_id = chunk_a, .size_bytes = 3}}),
                      "application/json");

    ASSERT_EQ(response.result(), beast_http::status::ok);
    const boost::json::value parsed = boost::json::parse(response.body());
    const boost::json::array& result_chunks = parsed.at("chunks").as_array();
    ASSERT_EQ(result_chunks.size(), 2U);
    EXPECT_EQ(result_chunks.at(0).at("chunk_id").as_string(), chunk_a);
    EXPECT_EQ(result_chunks.at(1).at("chunk_id").as_string(), chunk_b);
}

TEST_F(MetadataServiceUploadApiTest, NegotiationRejectsConflictingDuplicateSizes) {
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                            make_session_json(session_id_, artifact_id_), "application/json")
                  .result(),
              beast_http::status::ok);

    const std::string chunk_a = sha256_hex(std::string{"m4s2-dup-conflict-"} + UuidV7::generate().str());
    track_chunk(chunk_a);

    const HttpResponse response = http_exchange(
        server_->port(), beast_http::verb::post, "/v1/chunks/negotiate",
        make_negotiate_json(session_id_,
                            {{.chunk_id = chunk_a, .size_bytes = 3}, {.chunk_id = chunk_a, .size_bytes = 4}}),
        "application/json");

    EXPECT_EQ(response.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(response.body()).at("error").as_string(), "invalid_request");
    EXPECT_FALSE(repository_->get_chunk_size(chunk_a).has_value());
}

TEST_F(MetadataServiceUploadApiTest, NegotiationRequiresOpenExistingSession) {
    const UuidV7 missing = UuidV7::generate();
    const std::string chunk_id = sha256_hex(std::string{"m4s2-session-gate-"} + UuidV7::generate().str());
    track_chunk(chunk_id);

    const HttpResponse missing_response =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/chunks/negotiate",
                      make_negotiate_json(missing, {{.chunk_id = chunk_id, .size_bytes = 1}}), "application/json");

    EXPECT_EQ(missing_response.result(), beast_http::status::not_found);
    EXPECT_EQ(boost::json::parse(missing_response.body()).at("error").as_string(), "upload_session_not_found");

    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                            make_session_json(session_id_, artifact_id_), "application/json")
                  .result(),
              beast_http::status::ok);
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post,
                            std::string{"/v1/upload-sessions/"} + session_id_.str() + "/abort")
                  .result(),
              beast_http::status::ok);

    const HttpResponse aborted_response =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/chunks/negotiate",
                      make_negotiate_json(session_id_, {{.chunk_id = chunk_id, .size_bytes = 1}}), "application/json");

    EXPECT_EQ(aborted_response.result(), beast_http::status::conflict);
    EXPECT_EQ(boost::json::parse(aborted_response.body()).at("error").as_string(), "upload_session_not_open");
    EXPECT_FALSE(repository_->get_chunk_size(chunk_id).has_value());
}

TEST_F(MetadataServiceUploadApiTest, NegotiationRejectsMalformedInput) {
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                            make_session_json(session_id_, artifact_id_), "application/json")
                  .result(),
              beast_http::status::ok);

    const std::string body = boost::json::serialize(boost::json::object{
        {"session_id", session_id_.str()},
        {"chunks", boost::json::array{}},
    });

    const HttpResponse response =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/chunks/negotiate", body, "application/json");

    EXPECT_EQ(response.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(response.body()).at("error").as_string(), "invalid_request");
}

TEST_F(MetadataServiceUploadApiTest, NegotiationRejectsUnsupportedMediaTypeAndMethod) {
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions",
                            make_session_json(session_id_, artifact_id_), "application/json")
                  .result(),
              beast_http::status::ok);

    const std::string chunk_id = sha256_hex(std::string{"m4s2-media-"} + UuidV7::generate().str());
    track_chunk(chunk_id);
    const std::string body = make_negotiate_json(session_id_, {{.chunk_id = chunk_id, .size_bytes = 2}});

    const HttpResponse media =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/chunks/negotiate", body, "text/plain");
    EXPECT_EQ(media.result(), beast_http::status::unsupported_media_type);
    EXPECT_EQ(boost::json::parse(media.body()).at("error").as_string(), "unsupported_media_type");

    const HttpResponse method = http_exchange(server_->port(), beast_http::verb::get, "/v1/chunks/negotiate");
    EXPECT_EQ(method.result(), beast_http::status::method_not_allowed);
    EXPECT_EQ(method[beast_http::field::allow], "POST");
    EXPECT_EQ(boost::json::parse(method.body()).at("error").as_string(), "method_not_allowed");
}

TEST_F(MetadataServiceUploadApiTest, CreatesAndGetsFastCdcUploadSession) {
    const UuidV7 session_id = UuidV7::generate();
    track_session(session_id);

    const std::string payload = boost::json::serialize(boost::json::object{
        {"session_id", session_id.str()},
        {"artifact_id", artifact_id_.str()},
        {"target_node_id", "m6-fastcdc-target"},
        {"chunking_strategy", "fastcdc"},
        {"chunking_parameters",
         boost::json::object{
             {"min_chunk_size_bytes", 64},
             {"avg_chunk_size_bytes", 256},
             {"max_chunk_size_bytes", 1024},
         }},
        {"parent_version_id", nullptr},
        {"immutable_metadata",
         boost::json::object{
             {"source", "m6-fastcdc"},
         }},
    });

    const HttpResponse create =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions", payload, "application/json");

    ASSERT_EQ(create.result(), beast_http::status::ok);

    const auto create_body = boost::json::parse(create.body()).as_object();
    EXPECT_EQ(create_body.at("chunking_strategy").as_string(), "fastcdc");
    EXPECT_EQ(json_uint64(create_body.at("chunking_parameters").as_object().at("avg_chunk_size_bytes")), 256U);

    const HttpResponse get =
        http_exchange(server_->port(), beast_http::verb::get, std::string{"/v1/upload-sessions/"} + session_id.str());

    ASSERT_EQ(get.result(), beast_http::status::ok);
    const auto get_body = boost::json::parse(get.body()).as_object();
    EXPECT_EQ(get_body.at("chunking_strategy").as_string(), "fastcdc");
    EXPECT_EQ(json_uint64(get_body.at("chunking_parameters").as_object().at("max_chunk_size_bytes")), 1024U);
}

TEST_F(MetadataServiceUploadApiTest, RejectsMalformedFastCdcChunkingParameters) {
    const UuidV7 session_id = UuidV7::generate();

    const std::string payload = boost::json::serialize(boost::json::object{
        {"session_id", session_id.str()},
        {"artifact_id", artifact_id_.str()},
        {"target_node_id", "m6-fastcdc-target"},
        {"chunking_strategy", "fastcdc"},
        {"chunking_parameters",
         boost::json::object{
             {"min_chunk_size_bytes", 64},
             {"avg_chunk_size_bytes", 300},
             {"max_chunk_size_bytes", 1024},
         }},
        {"parent_version_id", nullptr},
        {"immutable_metadata", boost::json::object{}},
    });

    const HttpResponse response =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/upload-sessions", payload, "application/json");

    EXPECT_EQ(response.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(response.body()).at("error").as_string(), "invalid_request");
}

}  // namespace
