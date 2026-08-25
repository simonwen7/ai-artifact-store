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
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "aistore/hashing/sha256.hpp"
#include "aistore/http/http_server.hpp"
#include "aistore/metadata/postgres_metadata_repository.hpp"
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
using aistore::metadata::PostgresMetadataRepository;
using aistore::service::MetadataService;

constexpr std::uint64_t kMaxRequestBodyBytes = 8ULL * 1024ULL * 1024ULL;

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

const std::string kChunkA = sha256_hex("step3-chunk-a");
const std::string kChunkB = sha256_hex("step3-chunk-b");
constexpr std::int64_t kChunkASize = 13;
constexpr std::int64_t kChunkBSize = 13;

void cleanup_step3_fixtures() {
    pqxx::connection connection{test_database_connection_string()};
    pqxx::work transaction{connection};

    transaction
        .exec(
            "DELETE FROM storage_locations "
            "WHERE chunk_id = $1 "
            "   OR chunk_id = $2 "
            "   OR node_id LIKE 'step3-%'",
            pqxx::params{
                kChunkA,
                kChunkB,
            })
        .no_rows();

    transaction
        .exec(
            "DELETE FROM chunks "
            "WHERE chunk_id = $1 "
            "   OR chunk_id = $2",
            pqxx::params{
                kChunkA,
                kChunkB,
            })
        .no_rows();

    transaction.commit();
}

void ensure_test_chunk(std::string_view chunk_id, std::int64_t size_bytes) {
    pqxx::connection connection{test_database_connection_string()};
    pqxx::work transaction{connection};

    transaction
        .exec(
            "INSERT INTO chunks ("
            "    chunk_id, "
            "    size_bytes"
            ") "
            "VALUES ("
            "    $1, "
            "    $2"
            ") "
            "ON CONFLICT (chunk_id) "
            "DO NOTHING",
            pqxx::params{
                chunk_id,
                size_bytes,
            })
        .no_rows();

    transaction.commit();
}

long long count_locations_for_chunk_node(std::string_view chunk_id, std::string_view node_id) {
    pqxx::connection connection{test_database_connection_string()};
    pqxx::work transaction{connection};

    const long long count = transaction.query_value<long long>(
        "SELECT COUNT(*) "
        "FROM storage_locations "
        "WHERE chunk_id = $1 "
        "  AND node_id = $2",
        pqxx::params{
            chunk_id,
            node_id,
        });

    transaction.commit();

    return count;
}

long long count_locations_for_chunk(std::string_view chunk_id) {
    pqxx::connection connection{test_database_connection_string()};
    pqxx::work transaction{connection};

    const long long count = transaction.query_value<long long>(
        "SELECT COUNT(*) "
        "FROM storage_locations "
        "WHERE chunk_id = $1",
        pqxx::params{
            chunk_id,
        });

    transaction.commit();

    return count;
}

std::string locations_collection_target(std::string_view chunk_id) {
    return std::string{"/v1/chunks/"} + std::string{chunk_id} + "/locations";
}

std::string location_item_target(std::string_view chunk_id, std::string_view node_id) {
    return locations_collection_target(chunk_id) + "/" + std::string{node_id};
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
    request.set(beast_http::field::user_agent, "aistore-metadata-service-location-api-test");

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

class MetadataLocationApiFixture {
   public:
    MetadataLocationApiFixture()
        : repository_(test_database_connection_string()), service_(repository_), server_(service_) {
        cleanup_step3_fixtures();
    }

    ~MetadataLocationApiFixture() { cleanup_step3_fixtures(); }

    MetadataLocationApiFixture(const MetadataLocationApiFixture&) = delete;

    MetadataLocationApiFixture& operator=(const MetadataLocationApiFixture&) = delete;

    [[nodiscard]] std::uint16_t port() const noexcept { return server_.port(); }

   private:
    PostgresMetadataRepository repository_;
    MetadataService service_;
    RunningHttpServer server_;
};

boost::json::object parse_json_object(const HttpResponse& response) {
    const boost::json::value parsed = boost::json::parse(response.body());

    if (!parsed.is_object()) {
        throw std::runtime_error("expected JSON object response body");
    }

    return parsed.as_object();
}

TEST(MetadataServiceLocationApiTest, RegistersAndReadsLocationOverHttp) {
    MetadataLocationApiFixture fixture;
    ensure_test_chunk(kChunkA, kChunkASize);

    const HttpResponse put_response =
        http_exchange(fixture.port(), beast_http::verb::put, location_item_target(kChunkA, "step3-node-a"),
                      R"({"storage_path":"/step3/data/a","state":"available"})", "application/json");

    EXPECT_EQ(put_response.result(), beast_http::status::no_content);

    const HttpResponse get_response =
        http_exchange(fixture.port(), beast_http::verb::get, locations_collection_target(kChunkA));

    EXPECT_EQ(get_response.result(), beast_http::status::ok);

    const boost::json::object body = parse_json_object(get_response);

    ASSERT_TRUE(body.contains("chunk_id"));
    EXPECT_EQ(body.at("chunk_id").as_string(), kChunkA);

    ASSERT_TRUE(body.contains("locations"));
    ASSERT_TRUE(body.at("locations").is_array());

    const boost::json::array& locations = body.at("locations").as_array();

    ASSERT_EQ(locations.size(), 1U);
    ASSERT_TRUE(locations[0].is_object());

    const boost::json::object& location = locations[0].as_object();

    EXPECT_EQ(location.at("node_id").as_string(), "step3-node-a");
    EXPECT_EQ(location.at("storage_path").as_string(), "/step3/data/a");
    EXPECT_EQ(location.at("state").as_string(), "available");
}

TEST(MetadataServiceLocationApiTest, UpdatingLocationReusesChunkNodeIdentity) {
    MetadataLocationApiFixture fixture;
    ensure_test_chunk(kChunkA, kChunkASize);

    const HttpResponse first_put =
        http_exchange(fixture.port(), beast_http::verb::put, location_item_target(kChunkA, "step3-node-a"),
                      R"({"storage_path":"/step3/data/old","state":"available"})", "application/json");
    const HttpResponse second_put =
        http_exchange(fixture.port(), beast_http::verb::put, location_item_target(kChunkA, "step3-node-a"),
                      R"({"storage_path":"/step3/data/new","state":"corrupt"})", "application/json");

    EXPECT_EQ(first_put.result(), beast_http::status::no_content);
    EXPECT_EQ(second_put.result(), beast_http::status::no_content);

    EXPECT_EQ(count_locations_for_chunk_node(kChunkA, "step3-node-a"), 1);

    const HttpResponse get_response =
        http_exchange(fixture.port(), beast_http::verb::get, locations_collection_target(kChunkA));

    ASSERT_EQ(get_response.result(), beast_http::status::ok);

    const boost::json::object body = parse_json_object(get_response);
    const boost::json::array& locations = body.at("locations").as_array();

    ASSERT_EQ(locations.size(), 1U);

    const boost::json::object& location = locations[0].as_object();

    EXPECT_EQ(location.at("storage_path").as_string(), "/step3/data/new");
    EXPECT_EQ(location.at("state").as_string(), "corrupt");
}

TEST(MetadataServiceLocationApiTest, MultipleLocationsAreReturnedInNodeOrder) {
    MetadataLocationApiFixture fixture;
    ensure_test_chunk(kChunkA, kChunkASize);

    ASSERT_EQ(http_exchange(fixture.port(), beast_http::verb::put, location_item_target(kChunkA, "step3-node-c"),
                            R"({"storage_path":"/step3/data/c","state":"missing"})", "application/json")
                  .result(),
              beast_http::status::no_content);
    ASSERT_EQ(http_exchange(fixture.port(), beast_http::verb::put, location_item_target(kChunkA, "step3-node-a"),
                            R"({"storage_path":"/step3/data/a","state":"available"})", "application/json")
                  .result(),
              beast_http::status::no_content);
    ASSERT_EQ(http_exchange(fixture.port(), beast_http::verb::put, location_item_target(kChunkA, "step3-node-b"),
                            R"({"storage_path":"/step3/data/b","state":"corrupt"})", "application/json")
                  .result(),
              beast_http::status::no_content);

    const HttpResponse get_response =
        http_exchange(fixture.port(), beast_http::verb::get, locations_collection_target(kChunkA));

    ASSERT_EQ(get_response.result(), beast_http::status::ok);

    const boost::json::object body = parse_json_object(get_response);
    const boost::json::array& locations = body.at("locations").as_array();

    ASSERT_EQ(locations.size(), 3U);

    EXPECT_EQ(locations[0].as_object().at("node_id").as_string(), "step3-node-a");
    EXPECT_EQ(locations[0].as_object().at("state").as_string(), "available");

    EXPECT_EQ(locations[1].as_object().at("node_id").as_string(), "step3-node-b");
    EXPECT_EQ(locations[1].as_object().at("state").as_string(), "corrupt");

    EXPECT_EQ(locations[2].as_object().at("node_id").as_string(), "step3-node-c");
    EXPECT_EQ(locations[2].as_object().at("state").as_string(), "missing");
}

TEST(MetadataServiceLocationApiTest, UnknownChunkQueryReturnsEmptyLocations) {
    MetadataLocationApiFixture fixture;

    const std::string unknown_chunk = sha256_hex("step3-unknown-chunk-query");

    const HttpResponse get_response =
        http_exchange(fixture.port(), beast_http::verb::get, locations_collection_target(unknown_chunk));

    EXPECT_EQ(get_response.result(), beast_http::status::ok);

    const boost::json::object body = parse_json_object(get_response);

    EXPECT_EQ(body.at("chunk_id").as_string(), unknown_chunk);
    ASSERT_TRUE(body.at("locations").is_array());
    EXPECT_TRUE(body.at("locations").as_array().empty());
}

TEST(MetadataServiceLocationApiTest, PutForMissingChunkReturns404) {
    MetadataLocationApiFixture fixture;

    const HttpResponse put_response =
        http_exchange(fixture.port(), beast_http::verb::put, location_item_target(kChunkA, "step3-node-a"),
                      R"({"storage_path":"/step3/data/a","state":"available"})", "application/json");

    EXPECT_EQ(put_response.result(), beast_http::status::not_found);

    const boost::json::object body = parse_json_object(put_response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "chunk_not_found");
}

TEST(MetadataServiceLocationApiTest, MalformedChunkIdReturns400) {
    MetadataLocationApiFixture fixture;

    const HttpResponse get_response =
        http_exchange(fixture.port(), beast_http::verb::get, "/v1/chunks/not-a-valid-id/locations");

    EXPECT_EQ(get_response.result(), beast_http::status::bad_request);

    const boost::json::object body = parse_json_object(get_response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "invalid_chunk_id");
}

TEST(MetadataServiceLocationApiTest, MalformedNodeIdReturns400) {
    MetadataLocationApiFixture fixture;
    ensure_test_chunk(kChunkA, kChunkASize);

    const HttpResponse put_response =
        http_exchange(fixture.port(), beast_http::verb::put, location_item_target(kChunkA, "bad%node"),
                      R"({"storage_path":"/step3/data/a","state":"available"})", "application/json");

    EXPECT_EQ(put_response.result(), beast_http::status::bad_request);

    const boost::json::object body = parse_json_object(put_response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "invalid_node_id");
}

TEST(MetadataServiceLocationApiTest, MalformedJsonReturns400) {
    MetadataLocationApiFixture fixture;
    ensure_test_chunk(kChunkA, kChunkASize);

    const HttpResponse put_response = http_exchange(
        fixture.port(), beast_http::verb::put, location_item_target(kChunkA, "step3-node-a"), "{", "application/json");

    EXPECT_EQ(put_response.result(), beast_http::status::bad_request);

    const boost::json::object body = parse_json_object(put_response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "invalid_json");
}

TEST(MetadataServiceLocationApiTest, InvalidLocationShapeReturns400) {
    MetadataLocationApiFixture fixture;
    ensure_test_chunk(kChunkA, kChunkASize);

    const HttpResponse put_response =
        http_exchange(fixture.port(), beast_http::verb::put, location_item_target(kChunkA, "step3-node-a"),
                      R"({"state":"available"})", "application/json");

    EXPECT_EQ(put_response.result(), beast_http::status::bad_request);

    const boost::json::object body = parse_json_object(put_response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "invalid_request");

    EXPECT_EQ(count_locations_for_chunk(kChunkA), 0);
}

TEST(MetadataServiceLocationApiTest, InvalidStateReturns400) {
    MetadataLocationApiFixture fixture;
    ensure_test_chunk(kChunkA, kChunkASize);

    const HttpResponse put_response =
        http_exchange(fixture.port(), beast_http::verb::put, location_item_target(kChunkA, "step3-node-a"),
                      R"({"storage_path":"/step3/data/a","state":"unknown"})", "application/json");

    EXPECT_EQ(put_response.result(), beast_http::status::bad_request);

    const boost::json::object body = parse_json_object(put_response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "invalid_state");
}

TEST(MetadataServiceLocationApiTest, PutRejectsUnsupportedMediaType) {
    MetadataLocationApiFixture fixture;
    ensure_test_chunk(kChunkA, kChunkASize);

    const HttpResponse put_response =
        http_exchange(fixture.port(), beast_http::verb::put, location_item_target(kChunkA, "step3-node-a"),
                      R"({"storage_path":"/step3/data/a","state":"available"})", "text/plain");

    EXPECT_EQ(put_response.result(), beast_http::status::unsupported_media_type);

    const boost::json::object body = parse_json_object(put_response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "unsupported_media_type");

    EXPECT_EQ(count_locations_for_chunk(kChunkA), 0);
}

TEST(MetadataServiceLocationApiTest, LocationCollectionRejectsUnsupportedMethod) {
    MetadataLocationApiFixture fixture;
    ensure_test_chunk(kChunkA, kChunkASize);

    const HttpResponse post_response =
        http_exchange(fixture.port(), beast_http::verb::post, locations_collection_target(kChunkA));

    EXPECT_EQ(post_response.result(), beast_http::status::method_not_allowed);
    EXPECT_EQ(post_response[beast_http::field::allow], "GET");

    const boost::json::object body = parse_json_object(post_response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "method_not_allowed");
}

TEST(MetadataServiceLocationApiTest, ConflictingNodePathReturns409) {
    MetadataLocationApiFixture fixture;
    ensure_test_chunk(kChunkA, kChunkASize);
    ensure_test_chunk(kChunkB, kChunkBSize);

    const HttpResponse first_put =
        http_exchange(fixture.port(), beast_http::verb::put, location_item_target(kChunkA, "step3-conflict-node"),
                      R"({"storage_path":"/step3/shared/path","state":"available"})", "application/json");

    EXPECT_EQ(first_put.result(), beast_http::status::no_content);

    const HttpResponse second_put =
        http_exchange(fixture.port(), beast_http::verb::put, location_item_target(kChunkB, "step3-conflict-node"),
                      R"({"storage_path":"/step3/shared/path","state":"available"})", "application/json");

    EXPECT_EQ(second_put.result(), beast_http::status::conflict);

    const boost::json::object conflict_body = parse_json_object(second_put);

    ASSERT_TRUE(conflict_body.contains("error"));
    EXPECT_EQ(conflict_body.at("error").as_string(), "location_conflict");

    const HttpResponse get_response =
        http_exchange(fixture.port(), beast_http::verb::get, locations_collection_target(kChunkA));

    ASSERT_EQ(get_response.result(), beast_http::status::ok);

    const boost::json::object body = parse_json_object(get_response);
    const boost::json::array& locations = body.at("locations").as_array();

    ASSERT_EQ(locations.size(), 1U);

    const boost::json::object& location = locations[0].as_object();

    EXPECT_EQ(location.at("node_id").as_string(), "step3-conflict-node");
    EXPECT_EQ(location.at("storage_path").as_string(), "/step3/shared/path");
    EXPECT_EQ(location.at("state").as_string(), "available");
}

}  // namespace
