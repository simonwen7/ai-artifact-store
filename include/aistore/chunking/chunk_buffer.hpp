#ifndef AISTORE_CHUNKING_CHUNK_BUFFER_HPP
#define AISTORE_CHUNKING_CHUNK_BUFFER_HPP

#include <cstdint>
#include <vector>

namespace aistore::chunking {

struct ChunkBuffer {
    std::uint64_t offset;
    std::vector<std::byte> bytes;
};

}  // namespace aistore::chunking

#endif  // AISTORE_CHUNKING_CHUNK_BUFFER_HPP
