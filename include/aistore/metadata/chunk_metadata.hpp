#ifndef AISTORE_METADATA_CHUNK_METADATA_HPP
#define AISTORE_METADATA_CHUNK_METADATA_HPP

#include <cstdint>
#include <string>

namespace aistore::metadata {

struct ChunkMetadata {
    std::string chunk_id;
    std::uint64_t size_bytes;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_CHUNK_METADATA_HPP
