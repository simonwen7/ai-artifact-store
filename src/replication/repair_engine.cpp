#include "aistore/replication/repair_engine.hpp"

#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

#include "aistore/client/client_error.hpp"
#include "aistore/metadata/storage_location.hpp"

namespace aistore::replication {

namespace {

[[nodiscard]] std::string logical_storage_path(std::string_view chunk_id) {
    return std::string{"/v1/chunks/"} + std::string{chunk_id};
}

[[nodiscard]] bool metadata_says_available(const metadata::ReplicationChunkPlan& chunk_plan, std::string_view node_id) {
    for (const metadata::ReplicationNodeEndpoint& source : chunk_plan.source_nodes) {
        if (source.node_id == node_id) {
            return true;
        }
    }

    return false;
}

}  // namespace

RepairEngine::RepairEngine(client::MetadataClient& metadata_client, client::StorageNodeClientPool& storage_pool)
    : metadata_client_{metadata_client}, storage_pool_{storage_pool} {}

RepairResult RepairEngine::repair(const RepairRequest& request) const {
    metadata::ReplicationRun run =
        metadata_client_.start_replication_run(request.run_id, request.version_id, request.replication_factor);

    if (run.state == metadata::ReplicationRunState::Completed) {
        return RepairResult{
            .run_id = run.run_id,
            .version_id = run.version_id,
            .layout_id = run.layout_id,
            .replication_factor = run.replication_factor,
            .stats = run.stats,
        };
    }

    const metadata::ReplicationPlan plan = metadata_client_.get_replication_plan(request.run_id);
    metadata::ReplicationStats stats = run.stats;

    for (const metadata::ReplicationChunkPlan& chunk_plan : plan.chunks) {
        stats.chunks_scanned += 1U;

        std::vector<std::string> targets_needing_repair;

        for (const std::string& desired_node_id : chunk_plan.desired_node_ids) {
            if (!storage_pool_.contains(desired_node_id)) {
                throw std::runtime_error("replication target node is not available in storage client pool");
            }

            client::StorageNodeClient& target_client = storage_pool_.client_for(desired_node_id);
            const bool metadata_available = metadata_says_available(chunk_plan, desired_node_id);

            if (metadata_available && target_client.has_chunk(chunk_plan.chunk_id)) {
                stats.replicas_verified += 1U;
                continue;
            }

            targets_needing_repair.push_back(desired_node_id);
        }

        if (targets_needing_repair.empty()) {
            continue;
        }

        stats.chunks_under_replicated += 1U;

        std::optional<std::vector<std::byte>> chunk_bytes;

        for (const metadata::ReplicationNodeEndpoint& source : chunk_plan.source_nodes) {
            if (!storage_pool_.contains(source.node_id)) {
                continue;
            }

            try {
                chunk_bytes = storage_pool_.client_for(source.node_id).get_chunk(chunk_plan.chunk_id);

                if (chunk_bytes.has_value()) {
                    break;
                }
            } catch (const client::RemoteApiError&) {
                stats.source_failovers += 1U;
            } catch (const client::RemoteProtocolError&) {
                stats.source_failovers += 1U;
            }
        }

        if (!chunk_bytes.has_value()) {
            throw std::runtime_error("no healthy replication source could supply chunk bytes");
        }

        if (chunk_bytes->size() != chunk_plan.size_bytes) {
            throw std::runtime_error("replication source chunk size mismatch");
        }

        const std::span<const std::byte> bytes_span{*chunk_bytes};

        for (const std::string& target_node_id : targets_needing_repair) {
            client::StorageNodeClient& target_client = storage_pool_.client_for(target_node_id);
            target_client.put_chunk(chunk_plan.chunk_id, bytes_span);
            metadata_client_.register_storage_location(metadata::StorageLocation{
                .chunk_id = chunk_plan.chunk_id,
                .node_id = target_node_id,
                .storage_path = logical_storage_path(chunk_plan.chunk_id),
                .state = metadata::StorageLocationState::Available,
            });
            stats.replicas_written += 1U;
            stats.bytes_copied += chunk_plan.size_bytes;
        }
    }

    run = metadata_client_.complete_replication_run(request.run_id, stats);

    return RepairResult{
        .run_id = run.run_id,
        .version_id = run.version_id,
        .layout_id = run.layout_id,
        .replication_factor = run.replication_factor,
        .stats = run.stats,
    };
}

}  // namespace aistore::replication
