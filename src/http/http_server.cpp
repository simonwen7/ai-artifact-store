#include "aistore/http/http_server.hpp"

#include <atomic>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace aistore::http {

namespace {

namespace asio = boost::asio;
using tcp = asio::ip::tcp;

}  // namespace

class HttpServer::Impl {
   public:
    Impl(HttpServerConfig config, RequestHandler handler)
        : config_(std::move(config)), handler_(std::move(handler)), acceptor_(asio::make_strand(ioc_)) {
        if (config_.bind_address.empty()) {
            throw std::invalid_argument("HTTP bind address must not be empty");
        }

        if (config_.worker_threads < 1U) {
            throw std::invalid_argument("HTTP worker thread count must be at least 1");
        }

        if (config_.max_request_body_bytes == 0U) {
            throw std::invalid_argument("HTTP max request body size must be greater than 0");
        }

        const tcp::endpoint endpoint{
            asio::ip::make_address(config_.bind_address),
            config_.port,
        };

        acceptor_.open(endpoint.protocol());
        acceptor_.set_option(asio::socket_base::reuse_address(true));
        acceptor_.bind(endpoint);
        acceptor_.listen(asio::socket_base::max_listen_connections);

        bound_port_ = acceptor_.local_endpoint().port();
    }

    void run() {
        bool expected = false;

        if (!accept_started_.compare_exchange_strong(expected, true)) {
            throw std::logic_error("HTTP server run() may only be called once");
        }

        do_accept();

        std::vector<std::thread> workers;
        workers.reserve(config_.worker_threads - 1U);

        for (std::size_t index = 1; index < config_.worker_threads; ++index) {
            workers.emplace_back([this] { ioc_.run(); });
        }

        ioc_.run();

        for (std::thread& worker : workers) {
            worker.join();
        }
    }

    void stop() {
        beast::error_code close_error;
        // NOLINTNEXTLINE(bugprone-unused-return-value)
        acceptor_.close(close_error);
        ioc_.stop();
    }

    [[nodiscard]] std::uint16_t port() const noexcept { return bound_port_; }

   private:
    class Session : public std::enable_shared_from_this<Session> {
       public:
        Session(tcp::socket socket, RequestHandler handler, std::uint64_t max_request_body_bytes)
            : stream_(std::move(socket)),
              handler_(std::move(handler)),
              max_request_body_bytes_(max_request_body_bytes) {}

        void start() { do_read(); }

       private:
        void do_read() {
            parser_ = std::make_unique<beast_http::request_parser<beast_http::string_body>>();
            parser_->body_limit(max_request_body_bytes_);

            auto self = shared_from_this();

            beast_http::async_read(stream_, buffer_, *parser_, [self](beast::error_code error, std::size_t) {
                if (error) {
                    return;
                }

                self->handle_request();
            });
        }

        void handle_request() {
            try {
                response_ = handler_(parser_->get());
            } catch (const std::exception&) {
                response_ = HttpResponse{
                    beast_http::status::internal_server_error,
                    parser_->get().version(),
                };
                response_.set(beast_http::field::content_type, "text/plain");
                response_.body() = "internal server error";
                response_.prepare_payload();
            }

            response_.keep_alive(false);
            response_.set(beast_http::field::connection, "close");

            do_write();
        }

        void do_write() {
            auto self = shared_from_this();

            beast_http::async_write(stream_, response_, [self](beast::error_code error, std::size_t) {
                if (!error) {
                    beast::error_code shutdown_error;
                    // NOLINTNEXTLINE(bugprone-unused-return-value)
                    self->stream_.socket().shutdown(tcp::socket::shutdown_send, shutdown_error);
                }
            });
        }

        beast::tcp_stream stream_;
        beast::flat_buffer buffer_;
        RequestHandler handler_;
        std::uint64_t max_request_body_bytes_;
        std::unique_ptr<beast_http::request_parser<beast_http::string_body>> parser_;
        HttpResponse response_;
    };

    void do_accept() {
        acceptor_.async_accept(asio::make_strand(ioc_), [this](beast::error_code error, tcp::socket socket) {
            if (!error) {
                std::make_shared<Session>(std::move(socket), handler_, config_.max_request_body_bytes)->start();

                do_accept();
                return;
            }

            if (error != asio::error::operation_aborted && acceptor_.is_open()) {
                do_accept();
            }
        });
    }

    HttpServerConfig config_;
    RequestHandler handler_;
    asio::io_context ioc_;
    tcp::acceptor acceptor_;
    std::uint16_t bound_port_{0};
    std::atomic<bool> accept_started_{false};
};

HttpServer::HttpServer(HttpServerConfig config, RequestHandler handler)
    : impl_(std::make_unique<Impl>(std::move(config), std::move(handler))) {}

HttpServer::~HttpServer() = default;

HttpServer::HttpServer(HttpServer&&) noexcept = default;

HttpServer& HttpServer::operator=(HttpServer&&) noexcept = default;

void HttpServer::run() { impl_->run(); }

void HttpServer::stop() { impl_->stop(); }

std::uint16_t HttpServer::port() const noexcept { return impl_->port(); }

}  // namespace aistore::http
