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
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/postgres_metadata_repository.hpp"
#include "aistore/metadata/storage_location.hpp"
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
using aistore::metadata::ChunkMetadata;
using aistore::metadata::ChunkRef;
using aistore::metadata::Object;
using aistore::metadata::ObjectLayout;
using aistore::metadata::ObjectLayoutDescriptor;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::StorageLocation;
using aistore::metadata::StorageLocationState;
using aistore::metadata::UuidV7;
using aistore::metadata::VersionState;
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

HttpResponse http_exchange(std::uint16_t port, beast_http::verb method, std::string_view target) {
    asio::io_context ioc;
    tcp::resolver resolver{ioc};
    beast::tcp_stream stream{ioc};
    stream.connect(resolver.resolve("127.0.0.1", std::to_string(port)));
    HttpRequest request{method, target, 11};
    request.set(beast_http::field::host, "127.0.0.1");
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

class MetadataServiceRestoreApiTest : public ::testing::Test {
   protected:
    void SetUp() override {
        marker_ = UuidV7::generate().str();
        artifact_id_ = UuidV7::generate();
        repository_.emplace(test_database_connection_string());
        repository_->create_artifact(Artifact{artifact_id_, "m5s5-api-" + marker_, "m5s5"});
        service_.emplace(*repository_);
        server_.emplace(*service_);
    }

    void TearDown() override {
        server_.reset();
        service_.reset();
        pqxx::connection connection{test_database_connection_string()};
        pqxx::work transaction{connection};
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

    void make_available(const ObjectLayoutDescriptor& value, std::string_view node = "m5s5-api-source") {
        for (const ChunkRef& chunk : value.layout().chunks()) {
            repository_->register_chunks({ChunkMetadata{.chunk_id = chunk.chunk_id, .size_bytes = chunk.size}});
            repository_->register_storage_location(StorageLocation{.chunk_id = chunk.chunk_id,
                                                                   .node_id = std::string{node},
                                                                   .storage_path = "/m5s5/" + chunk.chunk_id,
                                                                   .state = StorageLocationState::Available});
        }
    }

    ArtifactVersion create_committed_version(const ObjectLayoutDescriptor& value) {
        repository_->register_object(value.object());
        repository_->register_object_layout(value);
        make_available(value);
        ArtifactVersion version{
            artifact_id_,
            value.object_id(),
            std::nullopt,
            ArtifactVersion::ImmutableMetadata{{"source", marker_}},
            VersionState::Committed,
        };
        repository_->create_version(version);
        return version;
    }

    std::string target(std::string_view version_id, std::string_view source_node = "m5s5-api-source") const {
        return std::string{"/v1/artifact-versions/"} + std::string{version_id} + "/restore-plan/" +
               std::string{source_node};
    }

    std::string marker_;
    UuidV7 artifact_id_{UuidV7::generate()};
    std::vector<std::string> object_ids_;
    std::vector<std::string> chunk_ids_;
    std::optional<PostgresMetadataRepository> repository_;
    std::optional<MetadataService> service_;
    std::optional<RunningHttpServer> server_;
};

TEST_F(MetadataServiceRestoreApiTest, GetRestorePlanReturnsCommittedVersionDescriptor) {
    const auto value = descriptor();
    const ArtifactVersion version = create_committed_version(value);
    const HttpResponse response = http_exchange(server_->port(), beast_http::verb::get, target(version.version_id()));
    ASSERT_EQ(response.result(), beast_http::status::ok);
    const auto body = boost::json::parse(response.body()).as_object();
    EXPECT_EQ(body.at("version_id").as_string(), version.version_id());
    EXPECT_EQ(body.at("artifact_id").as_string(), artifact_id_.str());
    EXPECT_EQ(body.at("source_node_id").as_string(), "m5s5-api-source");
    EXPECT_EQ(body.at("object_id").as_string(), value.object_id());
    EXPECT_EQ(body.at("layout_id").as_string(), value.layout_id());
    EXPECT_EQ(json_uint64(body.at("total_size_bytes")), 4U);
    EXPECT_EQ(body.at("chunking_strategy").as_string(), "fixed-size");
    EXPECT_EQ(json_uint64(body.at("chunk_count")), 1U);
    ASSERT_EQ(body.at("chunks").as_array().size(), 1U);
    EXPECT_EQ(body.at("chunks").as_array().at(0).at("chunk_id").as_string(), value.layout().chunks().front().chunk_id);
}

TEST_F(MetadataServiceRestoreApiTest, GetRestorePlanReturnsNotFoundForMissingVersion) {
    const HttpResponse response = http_exchange(server_->port(), beast_http::verb::get, target(std::string(64, 'f')));
    EXPECT_EQ(response.result(), beast_http::status::not_found);
    EXPECT_EQ(boost::json::parse(response.body()).at("error").as_string(), "artifact_version_not_found");
}

TEST_F(MetadataServiceRestoreApiTest, GetRestorePlanRejectsNonCommittedVersion) {
    const auto value = descriptor();
    repository_->register_object(value.object());
    repository_->register_object_layout(value);
    ArtifactVersion version{
        artifact_id_,          value.object_id(), std::nullopt, ArtifactVersion::ImmutableMetadata{{"source", marker_}},
        VersionState::Staging,
    };
    repository_->create_version(version);
    const HttpResponse response = http_exchange(server_->port(), beast_http::verb::get, target(version.version_id()));
    EXPECT_EQ(response.result(), beast_http::status::conflict);
    EXPECT_EQ(boost::json::parse(response.body()).at("error").as_string(), "artifact_version_not_committed");
}

TEST_F(MetadataServiceRestoreApiTest, GetRestorePlanRejectsUnavailableSource) {
    const auto value = descriptor();
    repository_->register_object(value.object());
    repository_->register_object_layout(value);
    for (const ChunkRef& chunk : value.layout().chunks()) {
        chunk_ids_.push_back(chunk.chunk_id);
        repository_->register_chunks({ChunkMetadata{.chunk_id = chunk.chunk_id, .size_bytes = chunk.size}});
        repository_->register_storage_location(StorageLocation{.chunk_id = chunk.chunk_id,
                                                               .node_id = "m5s5-other-node",
                                                               .storage_path = "/m5s5/" + chunk.chunk_id,
                                                               .state = StorageLocationState::Available});
    }
    ArtifactVersion version{
        artifact_id_,
        value.object_id(),
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{{"source", marker_}},
        VersionState::Committed,
    };
    repository_->create_version(version);
    const HttpResponse response =
        http_exchange(server_->port(), beast_http::verb::get, target(version.version_id(), "m5s5-api-source"));
    EXPECT_EQ(response.result(), beast_http::status::conflict);
    EXPECT_EQ(boost::json::parse(response.body()).at("error").as_string(), "restore_source_unavailable");
}

TEST_F(MetadataServiceRestoreApiTest, GetRestorePlanValidatesIdsAndMethodContract) {
    const auto value = descriptor();
    const ArtifactVersion version = create_committed_version(value);

    const HttpResponse invalid_version =
        http_exchange(server_->port(), beast_http::verb::get, target("not-a-version-id"));
    EXPECT_EQ(invalid_version.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(invalid_version.body()).at("error").as_string(), "invalid_version_id");

    const HttpResponse invalid_node =
        http_exchange(server_->port(), beast_http::verb::get,
                      std::string{"/v1/artifact-versions/"} + version.version_id() + "/restore-plan/bad!node");
    EXPECT_EQ(invalid_node.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(invalid_node.body()).at("error").as_string(), "invalid_node_id");

    const HttpResponse method = http_exchange(server_->port(), beast_http::verb::post, target(version.version_id()));
    EXPECT_EQ(method.result(), beast_http::status::method_not_allowed);
    EXPECT_EQ(method[beast_http::field::allow], "GET");
    EXPECT_EQ(boost::json::parse(method.body()).at("error").as_string(), "method_not_allowed");

    const HttpResponse query =
        http_exchange(server_->port(), beast_http::verb::get, target(version.version_id()) + "?debug=1");
    EXPECT_EQ(query.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(query.body()).at("error").as_string(), "invalid_request");

    HttpRequest fragment_request{beast_http::verb::get, target(version.version_id()) + "#section", 11};
    fragment_request.set(beast_http::field::host, "127.0.0.1");
    const HttpResponse fragment = service_->handle_request(fragment_request);
    EXPECT_EQ(fragment.result(), beast_http::status::bad_request);
    EXPECT_EQ(boost::json::parse(fragment.body()).at("error").as_string(), "invalid_request");
}

}  // namespace
