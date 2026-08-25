#include <gtest/gtest.h>

#include <atomic>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include "aistore/http/http_server.hpp"
#include "aistore/service/metadata_service.hpp"
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
using aistore::service::MetadataService;
using aistore::service::StorageNodeService;
using aistore::storage::LocalChunkStore;

constexpr std::uint64_t kMaxRequestBodyBytes = 8ULL * 1024ULL * 1024ULL;

class TemporaryDirectory {
   public:
    TemporaryDirectory() {
        static std::atomic<std::uint64_t> counter{0};

        const auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();

        path_ = std::filesystem::temp_directory_path() /
                ("aistore-http-health-" + std::to_string(timestamp) + "-" + std::to_string(counter.fetch_add(1)));

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

HttpResponse http_exchange(std::uint16_t port, beast_http::verb method, std::string_view target) {
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
    request.set(beast_http::field::user_agent, "aistore-http-service-health-test");
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
    template <typename Service>
    explicit RunningHttpServer(Service& service)
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

boost::json::object parse_json_object(const HttpResponse& response) {
    const boost::json::value parsed = boost::json::parse(response.body());

    if (!parsed.is_object()) {
        throw std::runtime_error("expected JSON object response body");
    }

    return parsed.as_object();
}

TEST(HttpServiceHealthTest, StorageNodeHealthOverHttp) {
    TemporaryDirectory temporary_directory;
    LocalChunkStore chunk_store{temporary_directory.path()};
    StorageNodeService service{chunk_store};
    RunningHttpServer server{service};

    const HttpResponse response = http_exchange(server.port(), beast_http::verb::get, "/health");

    EXPECT_EQ(response.result(), beast_http::status::ok);

    EXPECT_NE(response[beast_http::field::content_type].find("application/json"), std::string_view::npos);

    const boost::json::object body = parse_json_object(response);

    ASSERT_TRUE(body.contains("status"));
    EXPECT_EQ(body.at("status").as_string(), "ok");
}

TEST(HttpServiceHealthTest, MetadataServiceHealthOverHttp) {
    MetadataService service;
    RunningHttpServer server{service};

    const HttpResponse response = http_exchange(server.port(), beast_http::verb::get, "/health");

    EXPECT_EQ(response.result(), beast_http::status::ok);

    EXPECT_NE(response[beast_http::field::content_type].find("application/json"), std::string_view::npos);

    const boost::json::object body = parse_json_object(response);

    ASSERT_TRUE(body.contains("status"));
    EXPECT_EQ(body.at("status").as_string(), "ok");
}

TEST(HttpServiceHealthTest, StorageNodeUnknownRouteReturns404) {
    TemporaryDirectory temporary_directory;
    LocalChunkStore chunk_store{temporary_directory.path()};
    StorageNodeService service{chunk_store};
    RunningHttpServer server{service};

    const HttpResponse response = http_exchange(server.port(), beast_http::verb::get, "/does-not-exist");

    EXPECT_EQ(response.result(), beast_http::status::not_found);

    const boost::json::object body = parse_json_object(response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "not_found");
}

TEST(HttpServiceHealthTest, MetadataServiceUnknownRouteReturns404) {
    MetadataService service;
    RunningHttpServer server{service};

    const HttpResponse response = http_exchange(server.port(), beast_http::verb::get, "/does-not-exist");

    EXPECT_EQ(response.result(), beast_http::status::not_found);

    const boost::json::object body = parse_json_object(response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "not_found");
}

TEST(HttpServiceHealthTest, StorageNodeHealthRejectsNonGet) {
    TemporaryDirectory temporary_directory;
    LocalChunkStore chunk_store{temporary_directory.path()};
    StorageNodeService service{chunk_store};
    RunningHttpServer server{service};

    const HttpResponse response = http_exchange(server.port(), beast_http::verb::post, "/health");

    EXPECT_EQ(response.result(), beast_http::status::method_not_allowed);

    EXPECT_EQ(response[beast_http::field::allow], "GET");

    const boost::json::object body = parse_json_object(response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "method_not_allowed");
}

TEST(HttpServiceHealthTest, MetadataServiceHealthRejectsNonGet) {
    MetadataService service;
    RunningHttpServer server{service};

    const HttpResponse response = http_exchange(server.port(), beast_http::verb::post, "/health");

    EXPECT_EQ(response.result(), beast_http::status::method_not_allowed);

    EXPECT_EQ(response[beast_http::field::allow], "GET");

    const boost::json::object body = parse_json_object(response);

    ASSERT_TRUE(body.contains("error"));
    EXPECT_EQ(body.at("error").as_string(), "method_not_allowed");
}

}  // namespace
