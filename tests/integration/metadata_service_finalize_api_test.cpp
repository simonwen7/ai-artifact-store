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
#include "aistore/metadata/finalize_upload.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/postgres_metadata_repository.hpp"
#include "aistore/metadata/storage_location.hpp"
#include "aistore/metadata/upload_session.hpp"
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
using aistore::metadata::ChunkingStrategy;
using aistore::metadata::ChunkMetadata;
using aistore::metadata::ChunkRef;
using aistore::metadata::Object;
using aistore::metadata::ObjectLayout;
using aistore::metadata::ObjectLayoutDescriptor;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::StorageLocation;
using aistore::metadata::StorageLocationState;
using aistore::metadata::UploadSession;
using aistore::metadata::UploadSessionState;
using aistore::metadata::UuidV7;
using aistore::service::MetadataService;

constexpr std::uint64_t kMaxBodyBytes = 8ULL * 1024ULL * 1024ULL;

std::string test_database_connection_string() {
    const char* configured = std::getenv("AISTORE_TEST_DB_URL");
    return configured != nullptr && configured[0] != '\0' ? configured : "dbname=ai_artifact_store_test";
}

std::string digest_to_hex(const aistore::hashing::Sha256::Digest& digest) {
    constexpr std::array<char, 16> digits{'0', '1', '2', '3', '4', '5', '6', '7',
                                          '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    std::string result;
    result.reserve(64);
    for (const std::byte byte : digest) {
        const auto value = std::to_integer<unsigned int>(byte);
        result.push_back(digits[(value >> 4U) & 0x0FU]);
        result.push_back(digits[value & 0x0FU]);
    }
    return result;
}

std::string sha256_hex(std::string_view text) {
    aistore::hashing::Sha256 hasher;
    hasher.update(std::as_bytes(std::span{text.data(), text.size()}));
    return digest_to_hex(hasher.finalize());
}

HttpResponse http_exchange(std::uint16_t port, beast_http::verb method, std::string_view target,
                           std::optional<std::string> body = std::nullopt,
                           std::optional<std::string_view> content_type = std::nullopt) {
    asio::io_context ioc;
    tcp::resolver resolver{ioc};
    beast::tcp_stream stream{ioc};
    stream.connect(resolver.resolve("127.0.0.1", std::to_string(port)));
    HttpRequest request{method, target, 11};
    request.set(beast_http::field::host, "127.0.0.1");
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
                  .bind_address = "127.0.0.1", .port = 0, .worker_threads = 2, .max_request_body_bytes = kMaxBodyBytes},
              [&service](const HttpRequest& request) { return service.handle_request(request); }),
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

class MetadataServiceFinalizeApiTest : public ::testing::Test {
   protected:
    void SetUp() override {
        marker_ = UuidV7::generate().str();
        artifact_id_ = UuidV7::generate();
        session_id_ = UuidV7::generate();
        repository_.emplace(test_database_connection_string());
        repository_->create_artifact(Artifact{artifact_id_, "m4s5-api-" + marker_, "m4s5"});
        service_.emplace(*repository_);
        server_.emplace(*service_);
    }

    void TearDown() override {
        server_.reset();
        service_.reset();
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
    }

    ObjectLayoutDescriptor descriptor(std::string_view suffix = "main") {
        const std::string object_id = sha256_hex(marker_ + "-object-" + std::string{suffix});
        const std::string chunk_id = sha256_hex(marker_ + "-chunk-" + std::string{suffix});
        object_ids_.push_back(object_id);
        chunk_ids_.push_back(chunk_id);
        return ObjectLayoutDescriptor{Object{object_id, 4}, ChunkingStrategy::FixedSize,
                                      ObjectLayout{{ChunkRef{.chunk_id = chunk_id, .offset = 0, .size = 4}}}};
    }

    void create_session(std::string_view target = "m4s5-api-target") {
        repository_->create_upload_session(UploadSession{session_id_,
                                                         artifact_id_,
                                                         std::string{target},
                                                         ChunkingStrategy::FixedSize,
                                                         4,
                                                         std::nullopt,
                                                         {{"source", marker_}},
                                                         UploadSessionState::Open,
                                                         std::nullopt});
    }

    void make_available(const ObjectLayoutDescriptor& value, std::string_view node = "m4s5-api-target") {
        for (const ChunkRef& chunk : value.layout().chunks()) {
            repository_->register_chunks({ChunkMetadata{.chunk_id = chunk.chunk_id, .size_bytes = chunk.size}});
            repository_->register_storage_location(StorageLocation{.chunk_id = chunk.chunk_id,
                                                                   .node_id = std::string{node},
                                                                   .storage_path = "/m4s5/" + chunk.chunk_id,
                                                                   .state = StorageLocationState::Available});
        }
    }

    std::string payload(const ObjectLayoutDescriptor& value) const {
        boost::json::array chunks;
        for (const ChunkRef& chunk : value.layout().chunks()) {
            chunks.push_back(boost::json::object{
                {"chunk_id", chunk.chunk_id}, {"offset", chunk.offset}, {"size_bytes", chunk.size}});
        }
        return boost::json::serialize(boost::json::object{{"object_id", value.object_id()},
                                                          {"total_size_bytes", value.object().total_size()},
                                                          {"chunking_strategy", "fixed-size"},
                                                          {"chunking_parameters", boost::json::object{}},
                                                          {"chunks", std::move(chunks)}});
    }

    std::string target(const UuidV7& id) const { return std::string{"/v1/upload-sessions/"} + id.str() + "/finalize"; }

    std::string marker_;
    UuidV7 artifact_id_{UuidV7::generate()};
    UuidV7 session_id_{UuidV7::generate()};
    std::vector<std::string> object_ids_;
    std::vector<std::string> chunk_ids_;
    std::optional<PostgresMetadataRepository> repository_;
    std::optional<MetadataService> service_;
    std::optional<RunningHttpServer> server_;
};

TEST_F(MetadataServiceFinalizeApiTest, FinalizesUploadOverHttp) {
    create_session();
    const auto value = descriptor();
    make_available(value);
    const HttpResponse response =
        http_exchange(server_->port(), beast_http::verb::post, target(session_id_), payload(value), "application/json");
    ASSERT_EQ(response.result(), beast_http::status::ok);
    const auto body = boost::json::parse(response.body());
    EXPECT_EQ(body.at("session_id").as_string(), session_id_.str());
    EXPECT_EQ(body.at("object_id").as_string(), value.object_id());
    EXPECT_EQ(body.at("layout_id").as_string(), value.layout_id());
    EXPECT_EQ(body.at("state").as_string(), "committed");
    ASSERT_TRUE(repository_->get_version(std::string{body.at("version_id").as_string()}).has_value());
}

TEST_F(MetadataServiceFinalizeApiTest, FinalizeRetryIsIdempotentAndDifferentPayloadConflicts) {
    create_session();
    const auto first = descriptor("first");
    const ObjectLayoutDescriptor different{
        Object{first.object_id(), 4}, ChunkingStrategy::FixedSize,
        ObjectLayout{{ChunkRef{.chunk_id = sha256_hex(marker_ + "-different"), .offset = 0, .size = 4}}}};
    chunk_ids_.push_back(different.layout().chunks().front().chunk_id);
    make_available(first);
    make_available(different);
    const auto first_response =
        http_exchange(server_->port(), beast_http::verb::post, target(session_id_), payload(first), "application/json");
    const auto retry =
        http_exchange(server_->port(), beast_http::verb::post, target(session_id_), payload(first), "application/json");
    const auto conflict = http_exchange(server_->port(), beast_http::verb::post, target(session_id_),
                                        payload(different), "application/json");
    EXPECT_EQ(first_response.result(), beast_http::status::ok);
    EXPECT_EQ(retry.result(), beast_http::status::ok);
    EXPECT_EQ(boost::json::parse(first_response.body()).at("version_id"),
              boost::json::parse(retry.body()).at("version_id"));
    EXPECT_EQ(conflict.result(), beast_http::status::conflict);
    EXPECT_EQ(boost::json::parse(conflict.body()).at("error").as_string(), "finalize_conflict");
}

TEST_F(MetadataServiceFinalizeApiTest, FinalizeMissingAndAbortedSessionErrors) {
    const auto value = descriptor();
    const HttpResponse missing = http_exchange(server_->port(), beast_http::verb::post, target(UuidV7::generate()),
                                               payload(value), "application/json");
    EXPECT_EQ(missing.result(), beast_http::status::not_found);
    EXPECT_EQ(boost::json::parse(missing.body()).at("error").as_string(), "upload_session_not_found");
    create_session();
    repository_->abort_upload_session(session_id_);
    const HttpResponse aborted =
        http_exchange(server_->port(), beast_http::verb::post, target(session_id_), payload(value), "application/json");
    EXPECT_EQ(aborted.result(), beast_http::status::conflict);
    EXPECT_EQ(boost::json::parse(aborted.body()).at("error").as_string(), "upload_session_not_open");
}

TEST_F(MetadataServiceFinalizeApiTest, FinalizeRequiresAvailableTargetChunk) {
    create_session();
    const auto value = descriptor();
    repository_->register_chunks(
        {ChunkMetadata{.chunk_id = value.layout().chunks().front().chunk_id, .size_bytes = 4}});
    const HttpResponse response =
        http_exchange(server_->port(), beast_http::verb::post, target(session_id_), payload(value), "application/json");
    EXPECT_EQ(response.result(), beast_http::status::conflict);
    const auto body = boost::json::parse(response.body());
    EXPECT_EQ(body.at("error").as_string(), "chunk_not_available_on_target");
    EXPECT_EQ(body.at("chunk_id").as_string(), value.layout().chunks().front().chunk_id);
}

TEST_F(MetadataServiceFinalizeApiTest, FinalizeRejectsMalformedOrInvalidDescriptor) {
    create_session();
    const HttpResponse malformed =
        http_exchange(server_->port(), beast_http::verb::post, target(session_id_), "{", "application/json");
    EXPECT_EQ(malformed.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(malformed.body()).at("error").as_string(), "invalid_json");
    const std::string invalid =
        boost::json::serialize(boost::json::object{{"object_id", "not-a-hash"},
                                                   {"total_size_bytes", 0},
                                                   {"chunking_strategy", "fixed-size"},
                                                   {"chunking_parameters", boost::json::object{}},
                                                   {"chunks", boost::json::array{}}});
    const HttpResponse invalid_response =
        http_exchange(server_->port(), beast_http::verb::post, target(session_id_), invalid, "application/json");
    EXPECT_EQ(invalid_response.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(invalid_response.body()).at("error").as_string(), "invalid_request");
}

TEST_F(MetadataServiceFinalizeApiTest, FinalizeRejectsUnsupportedMediaTypeAndMethod) {
    create_session();
    const auto value = descriptor();
    const HttpResponse media =
        http_exchange(server_->port(), beast_http::verb::post, target(session_id_), payload(value), "text/plain");
    EXPECT_EQ(media.result(), beast_http::status::unsupported_media_type);
    EXPECT_EQ(boost::json::parse(media.body()).at("error").as_string(), "unsupported_media_type");
    const HttpResponse method = http_exchange(server_->port(), beast_http::verb::get, target(session_id_));
    EXPECT_EQ(method.result(), beast_http::status::method_not_allowed);
    EXPECT_EQ(method[beast_http::field::allow], "POST");
    EXPECT_EQ(boost::json::parse(method.body()).at("error").as_string(), "method_not_allowed");
}

TEST_F(MetadataServiceFinalizeApiTest, FinalizesFastCdcUploadThroughHttp) {
    const aistore::metadata::FastCdcParameters parameters{
        .min_chunk_size_bytes = 64,
        .avg_chunk_size_bytes = 256,
        .max_chunk_size_bytes = 1024,
    };

    repository_->create_upload_session(UploadSession{session_id_,
                                                     artifact_id_,
                                                     "m6-fastcdc-target",
                                                     parameters,
                                                     std::nullopt,
                                                     {{"source", marker_}},
                                                     UploadSessionState::Open,
                                                     std::nullopt});

    const std::string object_id = sha256_hex(marker_ + "-fastcdc-object");
    const std::string chunk_id = sha256_hex(marker_ + "-fastcdc-chunk");
    object_ids_.push_back(object_id);
    chunk_ids_.push_back(chunk_id);

    const ObjectLayoutDescriptor value{
        Object{object_id, 128},
        parameters,
        ObjectLayout{{ChunkRef{.chunk_id = chunk_id, .offset = 0, .size = 128}}},
    };

    make_available(value, "m6-fastcdc-target");

    boost::json::array chunks;
    chunks.push_back(boost::json::object{
        {"chunk_id", chunk_id},
        {"offset", 0},
        {"size_bytes", 128},
    });

    const std::string body = boost::json::serialize(boost::json::object{
        {"object_id", object_id},
        {"total_size_bytes", 128},
        {"chunking_strategy", "fastcdc"},
        {"chunking_parameters",
         boost::json::object{
             {"min_chunk_size_bytes", 64},
             {"avg_chunk_size_bytes", 256},
             {"max_chunk_size_bytes", 1024},
         }},
        {"chunks", std::move(chunks)},
    });

    const HttpResponse response =
        http_exchange(server_->port(), beast_http::verb::post, target(session_id_), body, "application/json");

    ASSERT_EQ(response.result(), beast_http::status::ok);
    EXPECT_EQ(boost::json::parse(response.body()).at("layout_id").as_string(), value.layout_id());
    ASSERT_TRUE(repository_->get_object_layout(value.layout_id()).has_value());
}

}  // namespace
