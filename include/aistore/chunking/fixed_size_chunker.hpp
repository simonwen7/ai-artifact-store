#ifndef AISTORE_CHUNKING_FIXED_SIZE_CHUNKER_HPP
#define AISTORE_CHUNKING_FIXED_SIZE_CHUNKER_HPP

#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <vector>

#include "aistore/chunking/chunk_buffer.hpp"

namespace aistore::chunking {

class FixedSizeChunker {
   public:
    static constexpr std::size_t kDefaultChunkSize = std::size_t{4} * 1024U * 1024U;

    using ChunkConsumer = std::function<void(ChunkBuffer)>;

    explicit FixedSizeChunker(std::size_t chunk_size = kDefaultChunkSize);

    FixedSizeChunker(const FixedSizeChunker&) = delete;
    FixedSizeChunker& operator=(const FixedSizeChunker&) = delete;

    FixedSizeChunker(FixedSizeChunker&&) noexcept = default;
    FixedSizeChunker& operator=(FixedSizeChunker&&) noexcept = default;

    void update(std::span<const std::byte> data, const ChunkConsumer& consumer);

    void finalize(const ChunkConsumer& consumer);

    [[nodiscard]] std::size_t chunk_size() const noexcept;

    [[nodiscard]] std::uint64_t bytes_received() const noexcept;

    [[nodiscard]] bool finalized() const noexcept;

   private:
    void emit_current_chunk(const ChunkConsumer& consumer);

    std::size_t chunk_size_;
    std::vector<std::byte> buffer_;
    std::uint64_t next_chunk_offset_ = 0;
    bool finalized_ = false;
};

}  // namespace aistore::chunking

#endif  // AISTORE_CHUNKING_FIXED_SIZE_CHUNKER_HPP
