#ifndef AISTORE_METADATA_CHUNK_METADATA_HPP
#define AISTORE_METADATA_CHUNK_METADATA_HPP

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace aistore::metadata {

struct ChunkMetadata {
    std::string chunk_id;
    std::uint64_t size_bytes;
};

struct ChunkNegotiationEntry {
    ChunkMetadata chunk;
    bool metadata_was_known;
    std::vector<std::string> available_node_ids;
};

struct ChunkSizeConflict {
    std::string chunk_id;
    std::uint64_t requested_size_bytes;
    std::uint64_t stored_size_bytes;
};

struct ChunkNegotiationBatch {
    std::vector<ChunkNegotiationEntry> chunks;
    std::optional<ChunkSizeConflict> conflict;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_CHUNK_METADATA_HPP
