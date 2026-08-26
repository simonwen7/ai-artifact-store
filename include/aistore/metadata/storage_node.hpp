#ifndef AISTORE_METADATA_STORAGE_NODE_HPP
#define AISTORE_METADATA_STORAGE_NODE_HPP

#include <cstdint>
#include <string>
#include <string_view>

namespace aistore::metadata {

enum class StorageNodeState : std::uint8_t {
    Active,
    Draining,
    Disabled,
};

struct StorageNode {
    std::string node_id;
    std::string address;
    std::uint16_t port = 0;
    StorageNodeState state = StorageNodeState::Active;

    bool operator==(const StorageNode&) const = default;
};

void validate_storage_node_id(std::string_view node_id);

void validate_storage_node_address(std::string_view address);

void validate_storage_node_port(std::uint16_t port);

void validate_storage_node(const StorageNode& node);

[[nodiscard]] std::string_view storage_node_state_to_string(StorageNodeState state);

[[nodiscard]] StorageNodeState storage_node_state_from_string(std::string_view state);

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_STORAGE_NODE_HPP
