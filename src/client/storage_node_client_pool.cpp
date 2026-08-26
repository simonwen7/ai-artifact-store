#include "aistore/client/storage_node_client_pool.hpp"

#include <stdexcept>
#include <utility>

namespace aistore::client {

StorageNodeClientPool::StorageNodeClientPool(std::vector<std::pair<std::string, StorageNodeClient>> clients) {
    for (auto& [node_id, client] : clients) {
        if (node_id.empty()) {
            throw std::invalid_argument("storage node client pool node_id must be nonempty");
        }

        if (!clients_.emplace(node_id, std::move(client)).second) {
            throw std::invalid_argument("duplicate storage node ID in client pool");
        }
    }
}

StorageNodeClient& StorageNodeClientPool::client_for(std::string_view node_id) {
    const auto iterator = clients_.find(node_id);

    if (iterator == clients_.end()) {
        throw std::invalid_argument("storage node client not found in pool");
    }

    return iterator->second;
}

const StorageNodeClient& StorageNodeClientPool::client_for(std::string_view node_id) const {
    const auto iterator = clients_.find(node_id);

    if (iterator == clients_.end()) {
        throw std::invalid_argument("storage node client not found in pool");
    }

    return iterator->second;
}

bool StorageNodeClientPool::contains(std::string_view node_id) const { return clients_.contains(node_id); }

StorageNodeClientPool StorageNodeClientPool::from_registry_nodes(const std::vector<metadata::StorageNode>& nodes,
                                                                 const http::HttpClientConfig& base_config) {
    std::vector<std::pair<std::string, StorageNodeClient>> clients;
    clients.reserve(nodes.size());

    for (const metadata::StorageNode& node : nodes) {
        http::HttpClientConfig config = base_config;
        config.endpoint.address = node.address;
        config.endpoint.port = node.port;
        clients.emplace_back(node.node_id, StorageNodeClient{std::move(config)});
    }

    return StorageNodeClientPool{std::move(clients)};
}

}  // namespace aistore::client
