#include "aistore/metadata/placement.hpp"

#include <algorithm>
#include <cstdint>
#include <ranges>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "aistore/hashing/sha256.hpp"
#include "aistore/metadata/storage_node.hpp"

namespace aistore::metadata {
namespace {

constexpr std::string_view kPlacementPrefix = "AISTORE_PLACEMENT_V1";

void validate_chunk_id_for_placement(std::string_view chunk_id) {
    if (chunk_id.size() != 64U) {
        throw std::invalid_argument("placement chunk_id must be 64 lowercase hex characters");
    }

    for (const char character : chunk_id) {
        const bool is_digit = character >= '0' && character <= '9';
        const bool is_lower_hex = character >= 'a' && character <= 'f';

        if (!is_digit && !is_lower_hex) {
            throw std::invalid_argument("placement chunk_id must be 64 lowercase hex characters");
        }
    }
}

[[nodiscard]] hashing::Sha256::Digest placement_digest(std::string_view chunk_id, std::string_view node_id) {
    hashing::Sha256 hasher;
    hasher.update(std::span<const std::byte>{reinterpret_cast<const std::byte*>(kPlacementPrefix.data()),
                                             kPlacementPrefix.size()});
    const std::byte separator{0};
    hasher.update(std::span<const std::byte>{&separator, 1U});
    hasher.update(std::span<const std::byte>{reinterpret_cast<const std::byte*>(chunk_id.data()), chunk_id.size()});
    hasher.update(std::span<const std::byte>{&separator, 1U});
    hasher.update(std::span<const std::byte>{reinterpret_cast<const std::byte*>(node_id.data()), node_id.size()});
    return hasher.finalize();
}

}  // namespace

std::vector<std::string> select_replica_nodes(std::string_view chunk_id,
                                              const std::vector<std::string>& placement_node_ids,
                                              std::size_t replication_factor) {
    validate_chunk_id_for_placement(chunk_id);

    if (placement_node_ids.empty() || placement_node_ids.size() > 64U) {
        throw std::invalid_argument("placement node set size must be between 1 and 64");
    }

    if (replication_factor < 1U || replication_factor > 8U || replication_factor > placement_node_ids.size()) {
        throw std::invalid_argument("replication_factor must be between 1 and 8 and <= placement node count");
    }

    std::set<std::string_view> unique_ids;

    for (std::size_t index = 0; index < placement_node_ids.size(); ++index) {
        const std::string& node_id = placement_node_ids[index];
        validate_storage_node_id(node_id);

        if (index > 0U && placement_node_ids[index - 1U] >= node_id) {
            throw std::invalid_argument("placement node IDs must be sorted and unique");
        }

        if (!unique_ids.insert(node_id).second) {
            throw std::invalid_argument("placement node IDs must be unique");
        }
    }

    struct RankedNode {
        hashing::Sha256::Digest digest{};
        std::string node_id;
    };

    std::vector<RankedNode> ranked;
    ranked.reserve(placement_node_ids.size());

    for (const std::string& node_id : placement_node_ids) {
        ranked.push_back(RankedNode{
            .digest = placement_digest(chunk_id, node_id),
            .node_id = node_id,
        });
    }

    std::ranges::sort(ranked, [](const RankedNode& left, const RankedNode& right) {
        for (std::size_t index = 0; index < left.digest.size(); ++index) {
            const auto left_value = std::to_integer<unsigned int>(left.digest[index]);
            const auto right_value = std::to_integer<unsigned int>(right.digest[index]);

            if (left_value != right_value) {
                return left_value > right_value;
            }
        }

        return left.node_id < right.node_id;
    });

    std::vector<std::string> selected;
    selected.reserve(replication_factor);

    for (std::size_t index = 0; index < replication_factor; ++index) {
        selected.push_back(ranked[index].node_id);
    }

    return selected;
}

}  // namespace aistore::metadata
