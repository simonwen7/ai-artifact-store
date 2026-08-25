#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "aistore/hashing/sha256.hpp"
#include "aistore/http/http_server.hpp"
#include "aistore/service/storage_node_service.hpp"
#include "aistore/storage/local_chunk_store.hpp"

namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace beast_http = beast::http;

using tcp = asio::ip::tcp;

using aistore::http::HttpRequest;
using aistore::http::HttpResponse;
using aistore::http::HttpServer;
using aistore::http::HttpServerConfig;
using aistore::service::StorageNodeService;
using aistore::storage::LocalChunkStore;

constexpr std::uint64_t kDefaultMaxRequestBodyBytes = 8ULL * 1024ULL * 1024ULL;

constexpr std::string_view kAbcChunkId =
    "ba7816bf8f01cfea414140de5dae2223"
    "b00361a396177a9cb410ff61f20015ad";

constexpr std::string_view kEmptyChunkId =
    "e3b0c44298fc1c149afbf4c8996fb924"
    "27ae41e4649b934ca495991b7852b855";

constexpr std::string_view kAbcBody = "abc";

class TemporaryDirectory {
   public:
    TemporaryDirectory() {
        static std::atomic<std::uint64_t> counter{0};

        const auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();

        path_ = std::filesystem::temp_directory_path() /
                ("aistore-chunk-api-" + std::to_string(timestamp) + "-" + std::to_string(counter.fetch_add(1)));

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

std::string sha256_hex(const std::string& body) {
    aistore::hashing::Sha256 hasher;
    hasher.update(std::as_bytes(std::span{body.data(), body.size()}));

    return digest_to_hex(hasher.finalize());
}

std::string chunk_target(std::string_view chunk_id) { return std::string{"/v1/chunks/"} + std::string{chunk_id}; }

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
    request.set(beast_http::field::user_agent, "aistore-storage-node-chunk-api-test");

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
    RunningHttpServer(StorageNodeService& service, std::uint64_t max_request_body_bytes = kDefaultMaxRequestBodyBytes)
        : server_(
              HttpServerConfig{
                  .bind_address = "127.0.0.1",
                  .port = 0,
                  .worker_threads = 2,
                  .max_request_body_bytes = max_request_body_bytes,
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

class StorageNodeChunkApiFixture {
   public:
    explicit StorageNodeChunkApiFixture(std::uint64_t max_request_body_bytes = kDefaultMaxRequestBodyBytes)
        : chunk_store_(temporary_directory_.path()),
          service_(chunk_store_),
          server_(service_, max_request_body_bytes) {}

    [[nodiscard]] std::uint16_t port() const noexcept { return server_.port(); }

    [[nodiscard]] LocalChunkStore& chunk_store() noexcept { return chunk_store_; }

    [[nodiscard]] const std::filesystem::path& storage_root() const noexcept { return temporary_directory_.path(); }

   private:
    TemporaryDirectory temporary_directory_;
    LocalChunkStore chunk_store_;
    StorageNodeService service_;
    RunningHttpServer server_;
};

boost::json::object parse_json_object(const HttpResponse& response) {
    const boost::json::value parsed = boost::json::parse(response.body());

    if (!parsed.is_object()) {
        throw std::runtime_error("expected JSON object response body");
    }

    return parsed.as_object();
}

TEST(StorageNodeChunkApiTest, PutAndGetVerifiedChunkOverHttp) {
    StorageNodeChunkApiFixture fixture;

    const HttpResponse put_response = http_exchange(fixture.port(), beast_http::verb::put, chunk_target(kAbcChunkId),
                                                    std::string{kAbcBody}, "application/octet-stream");

    EXPECT_EQ(put_response.result(), beast_http::status::no_content);

    const HttpResponse get_response = http_exchange(fixture.port(), beast_http::verb::get, chunk_target(kAbcChunkId));

    EXPECT_EQ(get_response.result(), beast_http::status::ok);
    EXPECT_NE(get_response[beast_http::field::content_type].find("application/octet-stream"), std::string_view::npos);
    EXPECT_EQ(get_response.body(), kAbcBody);
}

TEST(StorageNodeChunkApiTest, HeadReportsExistingChunk) {
    StorageNodeChunkApiFixture fixture;

    const HttpResponse put_response = http_exchange(fixture.port(), beast_http::verb::put, chunk_target(kAbcChunkId),
                                                    std::string{kAbcBody}, "application/octet-stream");

    ASSERT_EQ(put_response.result(), beast_http::status::no_content);

    const HttpResponse head_response = http_exchange(fixture.port(), beast_http::verb::head, chunk_target(kAbcChunkId));

    EXPECT_EQ(head_response.result(), beast_http::status::ok);
    EXPECT_TRUE(head_response.body().empty());
}

TEST(StorageNodeChunkApiTest, HeadReportsMissingChunk) {
    StorageNodeChunkApiFixture fixture;

    const HttpResponse head_response =
        http_exchange(fixture.port(), beast_http::verb::head, chunk_target(std::string(64, 'f')));

    EXPECT_EQ(head_response.result(), beast_http::status::not_found);
    EXPECT_TRUE(head_response.body().empty());
}

TEST(StorageNodeChunkApiTest, DuplicatePutIsIdempotentOverHttp) {
    StorageNodeChunkApiFixture fixture;

    const HttpResponse first_put = http_exchange(fixture.port(), beast_http::verb::put, chunk_target(kAbcChunkId),
                                                 std::string{kAbcBody}, "application/octet-stream");
    const HttpResponse second_put = http_exchange(fixture.port(), beast_http::verb::put, chunk_target(kAbcChunkId),
                                                  std::string{kAbcBody}, "application/octet-stream");

    EXPECT_EQ(first_put.result(), beast_http::status::no_content);
    EXPECT_EQ(second_put.result(), beast_http::status::no_content);

    const HttpResponse get_response = http_exchange(fixture.port(), beast_http::verb::get, chunk_target(kAbcChunkId));

    EXPECT_EQ(get_response.result(), beast_http::status::ok);
    EXPECT_EQ(get_response.body(), kAbcBody);
}

TEST(StorageNodeChunkApiTest, PutRejectsHashMismatch) {
    StorageNodeChunkApiFixture fixture;

    const HttpResponse put_response = http_exchange(fixture.port(), beast_http::verb::put, chunk_target(kEmptyChunkId),
                                                    std::string{kAbcBody}, "application/octet-stream");

    EXPECT_EQ(put_response.result(), beast_http::status::unprocessable_entity);

    const boost::json::object body = parse_json_object(put_response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "chunk_hash_mismatch");

    const HttpResponse head_response =
        http_exchange(fixture.port(), beast_http::verb::head, chunk_target(kEmptyChunkId));

    EXPECT_EQ(head_response.result(), beast_http::status::not_found);
}

TEST(StorageNodeChunkApiTest, PutRejectsUnsupportedMediaType) {
    StorageNodeChunkApiFixture fixture;

    const HttpResponse put_response = http_exchange(fixture.port(), beast_http::verb::put, chunk_target(kAbcChunkId),
                                                    std::string{kAbcBody}, "text/plain");

    EXPECT_EQ(put_response.result(), beast_http::status::unsupported_media_type);

    const boost::json::object body = parse_json_object(put_response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "unsupported_media_type");

    const HttpResponse head_response = http_exchange(fixture.port(), beast_http::verb::head, chunk_target(kAbcChunkId));

    EXPECT_EQ(head_response.result(), beast_http::status::not_found);
}

TEST(StorageNodeChunkApiTest, GetMissingChunkReturns404) {
    StorageNodeChunkApiFixture fixture;

    const HttpResponse get_response =
        http_exchange(fixture.port(), beast_http::verb::get, chunk_target(std::string(64, 'f')));

    EXPECT_EQ(get_response.result(), beast_http::status::not_found);

    const boost::json::object body = parse_json_object(get_response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "chunk_not_found");
}

TEST(StorageNodeChunkApiTest, MalformedChunkIdReturns400) {
    StorageNodeChunkApiFixture fixture;

    const HttpResponse get_response =
        http_exchange(fixture.port(), beast_http::verb::get, "/v1/chunks/not-a-valid-chunk-id");

    EXPECT_EQ(get_response.result(), beast_http::status::bad_request);

    const boost::json::object body = parse_json_object(get_response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "invalid_chunk_id");
}

TEST(StorageNodeChunkApiTest, ChunkRouteRejectsUnsupportedMethod) {
    StorageNodeChunkApiFixture fixture;

    const HttpResponse post_response = http_exchange(fixture.port(), beast_http::verb::post, chunk_target(kAbcChunkId));

    EXPECT_EQ(post_response.result(), beast_http::status::method_not_allowed);
    EXPECT_EQ(post_response[beast_http::field::allow], "GET, HEAD, PUT");

    const boost::json::object body = parse_json_object(post_response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "method_not_allowed");
}

TEST(StorageNodeChunkApiTest, BinaryChunkRoundTripsWithoutTextTruncation) {
    StorageNodeChunkApiFixture fixture;

    const std::string binary_body{
        {'a', '\0', 'b'},
    };
    const std::string chunk_id = sha256_hex(binary_body);

    const HttpResponse put_response = http_exchange(fixture.port(), beast_http::verb::put, chunk_target(chunk_id),
                                                    binary_body, "application/octet-stream");

    ASSERT_EQ(put_response.result(), beast_http::status::no_content);

    const HttpResponse get_response = http_exchange(fixture.port(), beast_http::verb::get, chunk_target(chunk_id));

    ASSERT_EQ(get_response.result(), beast_http::status::ok);
    ASSERT_EQ(get_response.body().size(), 3U);
    EXPECT_EQ(get_response.body()[0], 'a');
    EXPECT_EQ(get_response.body()[1], '\0');
    EXPECT_EQ(get_response.body()[2], 'b');
}

TEST(StorageNodeChunkApiTest, OversizedRequestReturns413) {
    StorageNodeChunkApiFixture fixture{4};

    const HttpResponse put_response = http_exchange(fixture.port(), beast_http::verb::put, chunk_target(kAbcChunkId),
                                                    std::string{"abcde"}, "application/octet-stream");

    EXPECT_EQ(put_response.result(), beast_http::status::payload_too_large);
    EXPECT_EQ(put_response.body(), "payload too large");
}

TEST(StorageNodeChunkApiTest, CorruptedStoredChunkIsNeverServed) {
    StorageNodeChunkApiFixture fixture;

    fixture.chunk_store().put(kAbcChunkId, std::as_bytes(std::span{kAbcBody.data(), kAbcBody.size()}));

    const std::filesystem::path cas_path =
        fixture.storage_root() / "chunks" / std::string{kAbcChunkId.substr(0, 2)} / std::string{kAbcChunkId};

    {
        std::ofstream output{cas_path, std::ios::binary | std::ios::trunc};
        ASSERT_TRUE(output);
        output << "corrupt";
        ASSERT_TRUE(output);
    }

    const HttpResponse get_response = http_exchange(fixture.port(), beast_http::verb::get, chunk_target(kAbcChunkId));

    EXPECT_EQ(get_response.result(), beast_http::status::internal_server_error);
    EXPECT_EQ(get_response.body(), "internal server error");
}

}  // namespace
