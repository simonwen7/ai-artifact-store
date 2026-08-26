#include "aistore/http/http_client.hpp"

#include <gtest/gtest.h>

#include <boost/asio/ip/tcp.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "aistore/http/http_server.hpp"

namespace {

namespace asio = boost::asio;
namespace beast_http = aistore::http::beast_http;

using tcp = asio::ip::tcp;

using aistore::http::HttpClient;
using aistore::http::HttpClientConfig;
using aistore::http::HttpClientError;
using aistore::http::HttpClientErrorKind;
using aistore::http::HttpEndpoint;
using aistore::http::HttpRequest;
using aistore::http::HttpResponse;
using aistore::http::HttpServer;
using aistore::http::HttpServerConfig;

constexpr std::uint64_t kMaxBodyBytes = 8ULL * 1024ULL * 1024ULL;

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

HttpClientConfig make_client_config(std::uint16_t port) {
    return HttpClientConfig{
        .endpoint =
            HttpEndpoint{
                .address = "127.0.0.1",
                .port = port,
            },
    };
}

TEST(HttpClientTest, GetRoundTripOverRealTcp) {
    RunningHttpServer server{[](const HttpRequest& request) {
        HttpResponse response{beast_http::status::ok, request.version()};
        response.set(beast_http::field::content_type, "text/plain");
        response.body() = "ok";
        response.prepare_payload();
        return response;
    }};

    const HttpClient client{make_client_config(server.port())};
    const auto response = client.request(beast_http::verb::get, "/ping");

    EXPECT_EQ(response.result(), beast_http::status::ok);
    EXPECT_EQ(response.body(), "ok");
}

TEST(HttpClientTest, BinaryRequestAndResponseAreExact) {
    RunningHttpServer server{[](const HttpRequest& request) {
        HttpResponse response{beast_http::status::ok, request.version()};
        response.set(beast_http::field::content_type, "application/octet-stream");
        response.body() = request.body();
        response.prepare_payload();
        return response;
    }};

    std::string body;
    body.push_back('A');
    body.push_back('\0');
    body.push_back('B');
    body.push_back('\0');
    body.push_back('C');

    const HttpClient client{make_client_config(server.port())};
    const auto response = client.request(beast_http::verb::post, "/echo", body, "application/octet-stream");

    ASSERT_EQ(response.result(), beast_http::status::ok);
    ASSERT_EQ(response.body().size(), 5U);
    EXPECT_EQ(response.body()[0], 'A');
    EXPECT_EQ(response.body()[1], '\0');
    EXPECT_EQ(response.body()[2], 'B');
    EXPECT_EQ(response.body()[3], '\0');
    EXPECT_EQ(response.body()[4], 'C');
}

TEST(HttpClientTest, RejectsRequestBodyAboveConfiguredLimitBeforeNetwork) {
    HttpClientConfig config{
        .endpoint =
            HttpEndpoint{
                .address = "127.0.0.1",
                .port = 1,
            },
        .max_request_body_bytes = 4,
    };

    const HttpClient client{std::move(config)};

    EXPECT_THROW((void)client.request(beast_http::verb::post, "/x", std::string(5, 'a')), std::invalid_argument);
}

TEST(HttpClientTest, ResponseBodyLimitProducesTypedError) {
    RunningHttpServer server{[](const HttpRequest& request) {
        HttpResponse response{beast_http::status::ok, request.version()};
        response.set(beast_http::field::content_type, "text/plain");
        response.body() = std::string(64, 'x');
        response.prepare_payload();
        return response;
    }};

    HttpClientConfig config = make_client_config(server.port());
    config.max_response_body_bytes = 8;

    const HttpClient client{std::move(config)};

    try {
        (void)client.request(beast_http::verb::get, "/large");
        FAIL() << "expected HttpClientError";
    } catch (const HttpClientError& error) {
        EXPECT_EQ(error.kind(), HttpClientErrorKind::ResponseTooLarge);
    }
}

TEST(HttpClientTest, ConnectionFailureProducesTypedError) {
    std::uint16_t closed_port = 0;

    {
        asio::io_context ioc;
        tcp::acceptor acceptor{ioc, tcp::endpoint{asio::ip::make_address("127.0.0.1"), 0}};
        closed_port = acceptor.local_endpoint().port();
    }

    const HttpClient client{make_client_config(closed_port)};

    try {
        (void)client.request(beast_http::verb::get, "/missing");
        FAIL() << "expected HttpClientError";
    } catch (const HttpClientError& error) {
        EXPECT_EQ(error.kind(), HttpClientErrorKind::Connect);
    }
}

TEST(HttpClientTest, ReadTimeoutProducesTypedError) {
    asio::io_context accept_ioc;
    tcp::acceptor acceptor{accept_ioc, tcp::endpoint{asio::ip::make_address("127.0.0.1"), 0}};
    const std::uint16_t port = acceptor.local_endpoint().port();

    std::thread server_thread{[&] {
        tcp::socket socket{accept_ioc};
        acceptor.accept(socket);
        std::this_thread::sleep_for(std::chrono::milliseconds{500});
        boost::system::error_code ignored;
        socket.close(ignored);
    }};

    HttpClientConfig config = make_client_config(port);
    config.request_timeout = std::chrono::milliseconds{100};

    const HttpClient client{std::move(config)};

    try {
        (void)client.request(beast_http::verb::get, "/hang");
        FAIL() << "expected HttpClientError";
    } catch (const HttpClientError& error) {
        EXPECT_EQ(error.kind(), HttpClientErrorKind::Timeout);
    }

    if (server_thread.joinable()) {
        server_thread.join();
    }
}

}  // namespace
