#include "aistore/http/http_client.hpp"

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <utility>

#include "aistore/http/http_server.hpp"

namespace aistore::http {

namespace {

namespace asio = boost::asio;
using tcp = asio::ip::tcp;

[[noreturn]] void throw_mapped(HttpClientErrorKind kind, std::string message) {
    throw HttpClientError{kind, std::move(message)};
}

void map_and_throw(beast::error_code error, HttpClientErrorKind operation_kind, std::string_view operation) {
    if (!error) {
        return;
    }

    if (error == beast::error::timeout) {
        throw_mapped(HttpClientErrorKind::Timeout, std::string{operation} + " timed out");
    }

    if (error == beast_http::error::body_limit) {
        throw_mapped(HttpClientErrorKind::ResponseTooLarge, std::string{operation} + " response too large");
    }

    throw_mapped(operation_kind, std::string{operation} + " failed");
}

}  // namespace

// NOLINTNEXTLINE(performance-unnecessary-value-param)
HttpClientError::HttpClientError(HttpClientErrorKind kind, std::string message)
    : std::runtime_error{message}, kind_{kind} {}

HttpClientErrorKind HttpClientError::kind() const noexcept { return kind_; }

HttpClient::HttpClient(HttpClientConfig config) : config_{std::move(config)} {
    if (config_.endpoint.address.empty()) {
        throw std::invalid_argument("HTTP client endpoint address must not be empty");
    }

    boost::system::error_code address_error;
    (void)asio::ip::make_address(config_.endpoint.address, address_error);

    if (address_error) {
        throw std::invalid_argument("HTTP client endpoint address must be a numeric IP address");
    }

    if (config_.endpoint.port == 0U) {
        throw std::invalid_argument("HTTP client endpoint port must be greater than zero");
    }

    if (config_.connect_timeout <= std::chrono::milliseconds::zero()) {
        throw std::invalid_argument("HTTP client connect timeout must be greater than zero");
    }

    if (config_.request_timeout <= std::chrono::milliseconds::zero()) {
        throw std::invalid_argument("HTTP client request timeout must be greater than zero");
    }

    if (config_.max_request_body_bytes == 0U) {
        throw std::invalid_argument("HTTP client max request body bytes must be greater than zero");
    }

    if (config_.max_response_body_bytes == 0U) {
        throw std::invalid_argument("HTTP client max response body bytes must be greater than zero");
    }
}

const HttpClientConfig& HttpClient::config() const noexcept { return config_; }

HttpClientResponse HttpClient::request(beast_http::verb method, std::string_view target, std::string body,
                                       std::optional<std::string_view> content_type) const {
    if (target.empty() || target.front() != '/') {
        throw std::invalid_argument("HTTP client target must be nonempty and begin with '/'");
    }

    if (static_cast<std::uint64_t>(body.size()) > config_.max_request_body_bytes) {
        throw std::invalid_argument("HTTP client request body exceeds configured maximum");
    }

    const asio::ip::address address = asio::ip::make_address(config_.endpoint.address);
    const tcp::endpoint endpoint{address, config_.endpoint.port};

    asio::io_context ioc;
    beast::tcp_stream stream{ioc};

    {
        stream.expires_after(config_.connect_timeout);

        beast::error_code connect_error;
        stream.async_connect(endpoint, [&](beast::error_code error) { connect_error = error; });
        ioc.run();
        map_and_throw(connect_error, HttpClientErrorKind::Connect, "connect");
    }

    HttpClientResponse response;

    try {
        HttpRequest request{
            method,
            target,
            11,
        };
        request.set(beast_http::field::host, config_.endpoint.address + ":" + std::to_string(config_.endpoint.port));
        request.set(beast_http::field::connection, "close");

        if (content_type.has_value()) {
            request.set(beast_http::field::content_type, *content_type);
        }

        request.body() = std::move(body);
        request.prepare_payload();

        {
            ioc.restart();
            stream.expires_after(config_.request_timeout);

            beast::error_code write_error;
            beast_http::async_write(stream, request,
                                    [&](beast::error_code error, std::size_t) { write_error = error; });
            ioc.run();
            map_and_throw(write_error, HttpClientErrorKind::Write, "write");
        }

        {
            ioc.restart();
            stream.expires_after(config_.request_timeout);

            beast::flat_buffer buffer;
            beast_http::response_parser<beast_http::string_body> parser;
            parser.body_limit(config_.max_response_body_bytes);

            beast::error_code read_error;
            beast_http::async_read(stream, buffer, parser,
                                   [&](beast::error_code error, std::size_t) { read_error = error; });
            ioc.run();
            map_and_throw(read_error, HttpClientErrorKind::Read, "read");

            response = parser.release();
        }
    } catch (...) {
        beast::error_code ignored;
        // NOLINTNEXTLINE(bugprone-unused-return-value)
        stream.socket().shutdown(tcp::socket::shutdown_both, ignored);
        throw;
    }

    beast::error_code shutdown_error;
    // NOLINTNEXTLINE(bugprone-unused-return-value)
    stream.socket().shutdown(tcp::socket::shutdown_both, shutdown_error);

    return response;
}

}  // namespace aistore::http
