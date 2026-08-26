#ifndef AISTORE_CLIENT_STORAGE_NODE_CLIENT_HPP
#define AISTORE_CLIENT_STORAGE_NODE_CLIENT_HPP

#include <cstddef>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

#include "aistore/http/http_client.hpp"

namespace aistore::client {

class StorageNodeClient {
   public:
    explicit StorageNodeClient(http::HttpClientConfig config);

    [[nodiscard]] bool has_chunk(std::string_view chunk_id) const;

    void put_chunk(std::string_view chunk_id, std::span<const std::byte> bytes) const;

    [[nodiscard]] std::optional<std::vector<std::byte>> get_chunk(std::string_view chunk_id) const;

   private:
    http::HttpClient http_client_;
};

}  // namespace aistore::client

#endif  // AISTORE_CLIENT_STORAGE_NODE_CLIENT_HPP
