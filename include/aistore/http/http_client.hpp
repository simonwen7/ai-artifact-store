#ifndef AISTORE_HTTP_HTTP_CLIENT_HPP
#define AISTORE_HTTP_HTTP_CLIENT_HPP

#include <boost/beast/http.hpp>
#include <chrono>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace aistore::http {

namespace beast = boost::beast;
namespace beast_http = beast::http;

struct HttpEndpoint {
    std::string address;
    std::uint16_t port;
};

struct HttpClientConfig {
    HttpEndpoint endpoint;

    std::chrono::milliseconds connect_timeout{3000};

    std::chrono::milliseconds request_timeout{30000};

    std::uint64_t max_request_body_bytes{8ULL * 1024ULL * 1024ULL};

    std::uint64_t max_response_body_bytes{8ULL * 1024ULL * 1024ULL};
};

enum class HttpClientErrorKind : std::uint8_t {
    Connect,
    Write,
    Read,
    Timeout,
    ResponseTooLarge,
};

class HttpClientError : public std::runtime_error {
   public:
    HttpClientError(HttpClientErrorKind kind, std::string message);

    [[nodiscard]] HttpClientErrorKind kind() const noexcept;

   private:
    HttpClientErrorKind kind_;
};

using HttpClientResponse = beast_http::response<beast_http::string_body>;

class HttpClient {
   public:
    explicit HttpClient(HttpClientConfig config);

    [[nodiscard]] HttpClientResponse request(beast_http::verb method, std::string_view target, std::string body = {},
                                             std::optional<std::string_view> content_type = std::nullopt) const;

    [[nodiscard]] const HttpClientConfig& config() const noexcept;

   private:
    HttpClientConfig config_;
};

}  // namespace aistore::http

#endif  // AISTORE_HTTP_HTTP_CLIENT_HPP
