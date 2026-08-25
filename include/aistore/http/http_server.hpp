#ifndef AISTORE_HTTP_HTTP_SERVER_HPP
#define AISTORE_HTTP_HTTP_SERVER_HPP

#include <boost/beast/http.hpp>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace aistore::http {

namespace beast = boost::beast;
namespace beast_http = beast::http;

using HttpRequest = beast_http::request<beast_http::string_body>;

using HttpResponse = beast_http::response<beast_http::string_body>;

using RequestHandler = std::function<HttpResponse(const HttpRequest&)>;

struct HttpServerConfig {
    std::string bind_address;
    std::uint16_t port;
    std::size_t worker_threads;
    std::uint64_t max_request_body_bytes;
};

class HttpServer {
   public:
    HttpServer(HttpServerConfig config, RequestHandler handler);

    ~HttpServer();

    HttpServer(const HttpServer&) = delete;

    HttpServer& operator=(const HttpServer&) = delete;

    HttpServer(HttpServer&&) noexcept;

    HttpServer& operator=(HttpServer&&) noexcept;

    void run();

    void stop();

    [[nodiscard]] std::uint16_t port() const noexcept;

   private:
    class Impl;

    std::unique_ptr<Impl> impl_;
};

}  // namespace aistore::http

#endif  // AISTORE_HTTP_HTTP_SERVER_HPP
