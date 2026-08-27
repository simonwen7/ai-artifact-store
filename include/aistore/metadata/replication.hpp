#ifndef AISTORE_METADATA_REPLICATION_HPP
#define AISTORE_METADATA_REPLICATION_HPP

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "aistore/metadata/uuid_v7.hpp"

namespace aistore::metadata {

enum class ReplicationRunState : std::uint8_t {
    Open,
    Completed,
};

enum class ReplicationErrorKind : std::uint8_t {
    RunNotFound,
    RunConflict,
    AnotherRunOpen,
    GcInProgress,
    RunNotOpen,
    VersionNotFound,
    VersionNotCommitted,
    SourceUnavailable,
    UnderReplicated,
    TargetDisabled,
    VersionRetired,
};

class ReplicationError : public std::runtime_error {
   public:
    ReplicationError(ReplicationErrorKind kind, const std::string& message);

    [[nodiscard]] ReplicationErrorKind kind() const noexcept;

   private:
    ReplicationErrorKind kind_;
};

struct ReplicationStats {
    std::uint64_t chunks_scanned = 0;
    std::uint64_t chunks_under_replicated = 0;
    std::uint64_t replicas_verified = 0;
    std::uint64_t replicas_written = 0;
    std::uint64_t bytes_copied = 0;
    std::uint64_t source_failovers = 0;

    bool operator==(const ReplicationStats&) const = default;
};

struct ReplicationRun {
    UuidV7 run_id;
    std::string version_id;
    std::string layout_id;
    std::uint8_t replication_factor = 1;
    std::vector<std::string> placement_node_ids;
    ReplicationRunState state = ReplicationRunState::Open;
    ReplicationStats stats{};
};

struct ReplicationNodeEndpoint {
    std::string node_id;
    std::string address;
    std::uint16_t port = 0;
};

struct ReplicationChunkPlan {
    std::string chunk_id;
    std::uint64_t offset = 0;
    std::uint64_t size_bytes = 0;
    std::vector<std::string> desired_node_ids;
    std::vector<ReplicationNodeEndpoint> source_nodes;
    std::vector<ReplicationNodeEndpoint> target_nodes;
};

struct ReplicationPlan {
    UuidV7 run_id;
    std::string version_id;
    std::string layout_id;
    std::uint8_t replication_factor = 1;
    std::vector<std::string> placement_node_ids;
    std::vector<ReplicationChunkPlan> chunks;
};

[[nodiscard]] std::string_view replication_run_state_to_string(ReplicationRunState state);

[[nodiscard]] ReplicationRunState replication_run_state_from_string(std::string_view state);

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_REPLICATION_HPP
