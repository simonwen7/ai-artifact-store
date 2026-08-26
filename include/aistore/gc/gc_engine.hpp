#ifndef AISTORE_GC_GC_ENGINE_HPP
#define AISTORE_GC_GC_ENGINE_HPP

#include <cstddef>
#include <string>

#include "aistore/client/metadata_client.hpp"
#include "aistore/client/storage_node_client.hpp"
#include "aistore/metadata/gc.hpp"
#include "aistore/metadata/uuid_v7.hpp"

namespace aistore::gc {

struct GcRequest {
    metadata::UuidV7 run_id;
    bool dry_run = false;
};

class GcEngine {
   public:
    static constexpr std::size_t kInventoryPageSize = 128U;
    static constexpr std::size_t kClassificationBatchSize = 128U;

    GcEngine(client::MetadataClient& metadata_client, client::StorageNodeClient& storage_client,
             std::string storage_node_id);

    [[nodiscard]] metadata::GcRun collect(const GcRequest& request) const;

   private:
    client::MetadataClient& metadata_client_;
    client::StorageNodeClient& storage_client_;
    std::string storage_node_id_;
};

}  // namespace aistore::gc

#endif  // AISTORE_GC_GC_ENGINE_HPP
