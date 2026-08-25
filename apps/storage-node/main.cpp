#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string>

#include "aistore/http/http_server.hpp"
#include "aistore/service/storage_node_service.hpp"
#include "aistore/storage/local_chunk_store.hpp"

int main() {
    try {
        constexpr std::uint64_t kMaxRequestBodyBytes = 8ULL * 1024ULL * 1024ULL;

        const char* storage_root = std::getenv("AISTORE_STORAGE_ROOT");

        if (storage_root == nullptr || storage_root[0] == '\0') {
            throw std::runtime_error("AISTORE_STORAGE_ROOT must be set");
        }

        aistore::storage::LocalChunkStore chunk_store{storage_root};
        aistore::service::StorageNodeService service{chunk_store};

        aistore::http::HttpServer server{
            aistore::http::HttpServerConfig{
                .bind_address = "127.0.0.1",
                .port = 8081,
                .worker_threads = 2,
                .max_request_body_bytes = kMaxRequestBodyBytes,
            },
            [&service](const aistore::http::HttpRequest& request) { return service.handle_request(request); },
        };

        std::cout << "storage-node listening on 127.0.0.1:" << server.port() << " storage_root=" << storage_root
                  << '\n';

        server.run();
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "storage-node error: " << error.what() << '\n';
        return 1;
    }
}
