#ifndef AISTORE_REPLICATION_REPAIR_ENGINE_HPP
#define AISTORE_REPLICATION_REPAIR_ENGINE_HPP

#include <cstdint>
#include <string>

#include "aistore/client/metadata_client.hpp"
#include "aistore/client/storage_node_client_pool.hpp"
#include "aistore/metadata/replication.hpp"
#include "aistore/metadata/uuid_v7.hpp"

namespace aistore::replication {

struct RepairRequest {
    metadata::UuidV7 run_id;
    std::string version_id;
    std::uint8_t replication_factor;
};

struct RepairResult {
    metadata::UuidV7 run_id;
    std::string version_id;
    std::string layout_id;
    std::uint8_t replication_factor;
    metadata::ReplicationStats stats;
};

class RepairEngine {
   public:
    RepairEngine(client::MetadataClient& metadata_client, client::StorageNodeClientPool& storage_pool);

    [[nodiscard]] RepairResult repair(const RepairRequest& request) const;

   private:
    client::MetadataClient& metadata_client_;
    client::StorageNodeClientPool& storage_pool_;
};

}  // namespace aistore::replication

#endif  // AISTORE_REPLICATION_REPAIR_ENGINE_HPP
