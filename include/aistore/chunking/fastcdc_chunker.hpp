#ifndef AISTORE_CHUNKING_FASTCDC_CHUNKER_HPP
#define AISTORE_CHUNKING_FASTCDC_CHUNKER_HPP

#include <cstddef>
#include <cstdint>
#include <functional>
#include <span>
#include <vector>

#include "aistore/chunking/chunk_buffer.hpp"

namespace aistore::chunking {

class FastCdcChunker {
   public:
    static constexpr std::size_t kDefaultMinChunkSize = std::size_t{2} * 1024U * 1024U;
    static constexpr std::size_t kDefaultAvgChunkSize = std::size_t{4} * 1024U * 1024U;
    static constexpr std::size_t kDefaultMaxChunkSize = std::size_t{8} * 1024U * 1024U;

    using ChunkConsumer = std::function<void(ChunkBuffer)>;

    FastCdcChunker(std::size_t min_chunk_size = kDefaultMinChunkSize, std::size_t avg_chunk_size = kDefaultAvgChunkSize,
                   std::size_t max_chunk_size = kDefaultMaxChunkSize);

    FastCdcChunker(const FastCdcChunker&) = delete;
    FastCdcChunker& operator=(const FastCdcChunker&) = delete;
    FastCdcChunker(FastCdcChunker&&) noexcept = default;
    FastCdcChunker& operator=(FastCdcChunker&&) noexcept = default;

    void update(std::span<const std::byte> data, const ChunkConsumer& consumer);
    void finalize(const ChunkConsumer& consumer);

    [[nodiscard]] std::size_t min_chunk_size() const noexcept;
    [[nodiscard]] std::size_t avg_chunk_size() const noexcept;
    [[nodiscard]] std::size_t max_chunk_size() const noexcept;
    [[nodiscard]] std::uint64_t bytes_received() const noexcept;
    [[nodiscard]] bool finalized() const noexcept;

   private:
    void emit_current_chunk(const ChunkConsumer& consumer);
    void process_buffered_bytes(const ChunkConsumer& consumer);

    std::size_t min_chunk_size_;
    std::size_t avg_chunk_size_;
    std::size_t max_chunk_size_;
    std::uint64_t mask_s_;
    std::uint64_t mask_l_;
    std::vector<std::byte> buffer_;
    std::uint64_t gear_hash_{0};
    std::uint64_t next_chunk_offset_{0};
    bool finalized_{false};
};

}  // namespace aistore::chunking
#endif
