#include "aistore/metadata/storage_node.hpp"

#include <boost/asio/ip/address.hpp>
#include <stdexcept>

namespace aistore::metadata {

void validate_storage_node_id(std::string_view node_id) {
    if (node_id.empty() || node_id.size() > 128U) {
        throw std::invalid_argument("storage node ID is invalid");
    }

    for (const char character : node_id) {
        const bool is_upper = character >= 'A' && character <= 'Z';
        const bool is_lower = character >= 'a' && character <= 'z';
        const bool is_digit = character >= '0' && character <= '9';
        const bool is_allowed_punct = character == '-' || character == '_' || character == '.';

        if (!is_upper && !is_lower && !is_digit && !is_allowed_punct) {
            throw std::invalid_argument("storage node ID is invalid");
        }
    }
}

void validate_storage_node_address(std::string_view address) {
    if (address.empty()) {
        throw std::invalid_argument("storage node address is invalid");
    }

    boost::system::error_code error;
    (void)boost::asio::ip::make_address(std::string{address}, error);

    if (error) {
        throw std::invalid_argument("storage node address must be a numeric IP address");
    }
}

void validate_storage_node_port(std::uint16_t port) {
    if (port == 0U) {
        throw std::invalid_argument("storage node port must be between 1 and 65535");
    }
}

void validate_storage_node(const StorageNode& node) {
    validate_storage_node_id(node.node_id);
    validate_storage_node_address(node.address);
    validate_storage_node_port(node.port);
}

std::string_view storage_node_state_to_string(StorageNodeState state) {
    switch (state) {
        case StorageNodeState::Active:
            return "active";

        case StorageNodeState::Draining:
            return "draining";

        case StorageNodeState::Disabled:
            return "disabled";
    }

    throw std::logic_error("unsupported storage node state");
}

StorageNodeState storage_node_state_from_string(std::string_view state) {
    if (state == "active") {
        return StorageNodeState::Active;
    }

    if (state == "draining") {
        return StorageNodeState::Draining;
    }

    if (state == "disabled") {
        return StorageNodeState::Disabled;
    }

    throw std::runtime_error("unsupported storage node state string");
}

}  // namespace aistore::metadata
