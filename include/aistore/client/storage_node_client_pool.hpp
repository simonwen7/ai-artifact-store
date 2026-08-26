#ifndef AISTORE_CLIENT_STORAGE_NODE_CLIENT_POOL_HPP
#define AISTORE_CLIENT_STORAGE_NODE_CLIENT_POOL_HPP

#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "aistore/client/storage_node_client.hpp"
#include "aistore/metadata/storage_node.hpp"

namespace aistore::client {

class StorageNodeClientPool {
   public:
    StorageNodeClientPool(std::vector<std::pair<std::string, StorageNodeClient>> clients);

    [[nodiscard]] StorageNodeClient& client_for(std::string_view node_id);

    [[nodiscard]] const StorageNodeClient& client_for(std::string_view node_id) const;

    [[nodiscard]] bool contains(std::string_view node_id) const;

    static StorageNodeClientPool from_registry_nodes(const std::vector<metadata::StorageNode>& nodes,
                                                     const http::HttpClientConfig& base_config);

   private:
    std::map<std::string, StorageNodeClient, std::less<>> clients_;
};

}  // namespace aistore::client

#endif  // AISTORE_CLIENT_STORAGE_NODE_CLIENT_POOL_HPP
