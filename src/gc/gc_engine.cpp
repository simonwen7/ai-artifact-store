#include "aistore/gc/gc_engine.hpp"

#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "aistore/client/client_error.hpp"
#include "aistore/storage/local_chunk_store.hpp"

namespace aistore::gc {
namespace {

[[nodiscard]] bool identities_match(const metadata::GcRun& run, const metadata::UuidV7& run_id,
                                    std::string_view target_node_id, bool dry_run) {
    const metadata::GcRunMode expected_mode = dry_run ? metadata::GcRunMode::DryRun : metadata::GcRunMode::Apply;
    return run.run_id == run_id && run.target_node_id == target_node_id && run.mode == expected_mode;
}

}  // namespace

GcEngine::GcEngine(client::MetadataClient& metadata_client, client::StorageNodeClient& storage_client,
                   std::string storage_node_id)
    : metadata_client_(metadata_client),
      storage_client_(storage_client),
      storage_node_id_(std::move(storage_node_id)) {}

metadata::GcRun GcEngine::collect(const GcRequest& request) const {
    metadata::GcRun run = metadata_client_.start_gc_run(request.run_id, storage_node_id_, request.dry_run);

    if (!identities_match(run, request.run_id, storage_node_id_, request.dry_run)) {
        throw std::runtime_error("GC run identity mismatch from metadata service");
    }

    if (run.state == metadata::GcRunState::Completed) {
        return run;
    }

    if (run.state != metadata::GcRunState::Open) {
        throw std::runtime_error("GC run is not open after start");
    }

    metadata::GcPhysicalStats physical_stats{};
    std::optional<std::string> after;

    while (true) {
        const storage::StoredChunkPage page = storage_client_.list_chunks(
            after.has_value() ? std::optional<std::string_view>{*after} : std::nullopt, kInventoryPageSize);

        if (page.chunks.empty()) {
            if (page.next_after.has_value()) {
                throw client::RemoteProtocolError("chunk inventory returned next_after with an empty page");
            }
            break;
        }

        std::string previous_id;
        for (std::size_t index = 0; index < page.chunks.size(); ++index) {
            const storage::StoredChunkInfo& info = page.chunks[index];
            if (index > 0U && info.chunk_id <= previous_id) {
                throw client::RemoteProtocolError("chunk inventory page is not lexicographically increasing");
            }
            previous_id = info.chunk_id;

            physical_stats.physical_chunks_scanned += 1U;
            physical_stats.physical_bytes_scanned += info.size_bytes;
        }

        if (page.next_after.has_value()) {
            if (*page.next_after != page.chunks.back().chunk_id) {
                throw client::RemoteProtocolError(
                    "chunk inventory next_after must equal the last chunk_id on the page");
            }
        }

        std::vector<std::string> chunk_ids;
        chunk_ids.reserve(page.chunks.size());
        for (const storage::StoredChunkInfo& info : page.chunks) {
            chunk_ids.push_back(info.chunk_id);
        }

        const std::vector<metadata::GcChunkDecision> decisions =
            metadata_client_.classify_gc_chunks(request.run_id, chunk_ids);

        if (decisions.size() != chunk_ids.size()) {
            throw client::RemoteProtocolError("GC classification response size mismatch");
        }

        for (std::size_t index = 0; index < decisions.size(); ++index) {
            const metadata::GcChunkDecision& decision = decisions[index];
            if (decision.chunk_id != chunk_ids[index]) {
                throw client::RemoteProtocolError("GC classification response order or chunk_id mismatch");
            }

            if (!decision.collectible) {
                continue;
            }

            const std::uint64_t size_bytes = page.chunks[index].size_bytes;
            physical_stats.collectible_chunks += 1U;
            physical_stats.collectible_bytes += size_bytes;

            if (request.dry_run) {
                continue;
            }

            const bool deleted = storage_client_.delete_chunk(decision.chunk_id);
            if (deleted) {
                physical_stats.physically_deleted_chunks += 1U;
                physical_stats.physically_deleted_bytes += size_bytes;
            }
        }

        if (!page.next_after.has_value()) {
            break;
        }

        if (after.has_value() && *page.next_after <= *after) {
            throw client::RemoteProtocolError("chunk inventory pagination cursor did not advance");
        }

        after = *page.next_after;
    }

    return metadata_client_.complete_gc_run(request.run_id, physical_stats);
}

}  // namespace aistore::gc
