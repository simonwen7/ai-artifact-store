#include <cstdint>
#include <iostream>
#include <stdexcept>

#include "aistore/http/http_server.hpp"
#include "aistore/service/metadata_service.hpp"

int main() {
    try {
        constexpr std::uint64_t kMaxRequestBodyBytes = 8ULL * 1024ULL * 1024ULL;

        aistore::service::MetadataService service;

        aistore::http::HttpServer server{
            aistore::http::HttpServerConfig{
                .bind_address = "127.0.0.1",
                .port = 8080,
                .worker_threads = 2,
                .max_request_body_bytes = kMaxRequestBodyBytes,
            },
            [&service](const aistore::http::HttpRequest& request) { return service.handle_request(request); },
        };

        std::cout << "metadata-service listening on 127.0.0.1:" << server.port() << '\n';

        server.run();
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "metadata-service error: " << error.what() << '\n';
        return 1;
    }
}
