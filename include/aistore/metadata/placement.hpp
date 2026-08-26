#ifndef AISTORE_METADATA_PLACEMENT_HPP
#define AISTORE_METADATA_PLACEMENT_HPP

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace aistore::metadata {

[[nodiscard]] std::vector<std::string> select_replica_nodes(std::string_view chunk_id,
                                                            const std::vector<std::string>& placement_node_ids,
                                                            std::size_t replication_factor);

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_PLACEMENT_HPP
