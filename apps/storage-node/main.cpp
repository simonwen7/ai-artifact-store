#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>

#include "aistore/http/http_server.hpp"
#include "aistore/service/storage_node_service.hpp"
#include "aistore/storage/local_chunk_store.hpp"

namespace {

[[nodiscard]] std::string read_required_env(const char* name) {
    const char* value = std::getenv(name);

    if (value == nullptr || value[0] == '\0') {
        throw std::runtime_error(std::string{name} + " must be set");
    }

    return value;
}

[[nodiscard]] std::string read_env_or_default(const char* name, std::string_view default_value) {
    const char* value = std::getenv(name);

    if (value == nullptr || value[0] == '\0') {
        return std::string{default_value};
    }

    return value;
}

[[nodiscard]] std::uint16_t parse_port(std::string_view text) {
    if (text.empty()) {
        throw std::runtime_error("AISTORE_STORAGE_PORT must be a positive decimal integer");
    }

    std::uint64_t value = 0;

    for (const char character : text) {
        if (character < '0' || character > '9') {
            throw std::runtime_error("AISTORE_STORAGE_PORT must be a positive decimal integer");
        }

        value = (value * 10ULL) + static_cast<std::uint64_t>(character - '0');

        if (value > 65535ULL) {
            throw std::runtime_error("AISTORE_STORAGE_PORT must be in range 1..65535");
        }
    }

    if (value < 1ULL) {
        throw std::runtime_error("AISTORE_STORAGE_PORT must be in range 1..65535");
    }

    return static_cast<std::uint16_t>(value);
}

}  // namespace

int main() {
    try {
        constexpr std::uint64_t kMaxRequestBodyBytes = 8ULL * 1024ULL * 1024ULL;

        const std::string storage_root = read_required_env("AISTORE_STORAGE_ROOT");
        const std::string node_id = read_env_or_default("AISTORE_STORAGE_NODE_ID", "node-1");
        const std::uint16_t port = parse_port(read_env_or_default("AISTORE_STORAGE_PORT", "8081"));

        aistore::storage::LocalChunkStore chunk_store{storage_root};
        aistore::service::StorageNodeService service{chunk_store, node_id};

        aistore::http::HttpServer server{
            aistore::http::HttpServerConfig{
                .bind_address = "127.0.0.1",
                .port = port,
                .worker_threads = 2,
                .max_request_body_bytes = kMaxRequestBodyBytes,
            },
            [&service](const aistore::http::HttpRequest& request) { return service.handle_request(request); },
        };

        std::cout << "storage-node listening on 127.0.0.1:" << server.port() << " node_id=" << node_id
                  << " storage_root=" << storage_root << '\n';

        server.run();
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "storage-node error: " << error.what() << '\n';
        return 1;
    }
}
