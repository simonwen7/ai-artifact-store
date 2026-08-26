#include "aistore/metadata/placement.hpp"

#include <gtest/gtest.h>

#include <stdexcept>
#include <string>
#include <vector>

namespace {

using aistore::metadata::select_replica_nodes;

constexpr const char* kChunkA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
constexpr const char* kChunkB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
constexpr const char* kChunkC = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
constexpr const char* kChunkD = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

const std::vector<std::string> kNodes{"node-A", "node-B", "node-C"};

TEST(PlacementTest, ProducesDeterministicGoldenReplicaOrder) {
    EXPECT_EQ((std::vector<std::string>{"node-C", "node-A"}), select_replica_nodes(kChunkA, kNodes, 2U));
    EXPECT_EQ((std::vector<std::string>{"node-B", "node-A"}), select_replica_nodes(kChunkB, kNodes, 2U));
    EXPECT_EQ((std::vector<std::string>{"node-C", "node-A"}), select_replica_nodes(kChunkC, kNodes, 2U));
    EXPECT_EQ((std::vector<std::string>{"node-A", "node-B"}), select_replica_nodes(kChunkD, kNodes, 2U));
    EXPECT_EQ((std::vector<std::string>{"node-C", "node-A", "node-B"}), select_replica_nodes(kChunkA, kNodes, 3U));
}

TEST(PlacementTest, InputNodeOrderDoesNotChangePlacement) {
    const std::vector<std::string> unsorted{"node-C", "node-A", "node-B"};
    EXPECT_THROW((void)select_replica_nodes(kChunkA, unsorted, 2U), std::invalid_argument);

    const std::vector<std::string> sorted_copy = kNodes;
    EXPECT_EQ(select_replica_nodes(kChunkA, kNodes, 2U), select_replica_nodes(kChunkA, sorted_copy, 2U));
}

TEST(PlacementTest, SelectsExactUniqueReplicationFactor) {
    const auto selected = select_replica_nodes(kChunkA, kNodes, 2U);
    ASSERT_EQ(selected.size(), 2U);
    EXPECT_NE(selected[0], selected[1]);
}

TEST(PlacementTest, DifferentChunksDistributePrimaryPlacement) {
    EXPECT_NE(select_replica_nodes(kChunkA, kNodes, 2U).front(), select_replica_nodes(kChunkB, kNodes, 2U).front());
}

TEST(PlacementTest, RejectsInvalidNodeSetsAndReplicationFactor) {
    EXPECT_THROW((void)select_replica_nodes("bad", kNodes, 1U), std::invalid_argument);
    EXPECT_THROW((void)select_replica_nodes(kChunkA, {}, 1U), std::invalid_argument);
    EXPECT_THROW((void)select_replica_nodes(kChunkA, kNodes, 0U), std::invalid_argument);
    EXPECT_THROW((void)select_replica_nodes(kChunkA, kNodes, 9U), std::invalid_argument);
    EXPECT_THROW((void)select_replica_nodes(kChunkA, kNodes, 4U), std::invalid_argument);

    const std::vector<std::string> duplicate{"node-A", "node-A"};
    EXPECT_THROW((void)select_replica_nodes(kChunkA, duplicate, 1U), std::invalid_argument);
}

TEST(PlacementTest, NodeSetChangeIsDeterministic) {
    const std::vector<std::string> two{"node-A", "node-B"};
    const auto with_two = select_replica_nodes(kChunkD, two, 2U);
    const auto with_three = select_replica_nodes(kChunkD, kNodes, 2U);
    EXPECT_EQ(with_two, (std::vector<std::string>{"node-A", "node-B"}));
    EXPECT_EQ(with_three, (std::vector<std::string>{"node-A", "node-B"}));
    EXPECT_EQ(with_two, with_three);
}

}  // namespace
