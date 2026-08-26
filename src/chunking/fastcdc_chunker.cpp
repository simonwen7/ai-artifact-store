#include "aistore/chunking/fastcdc_chunker.hpp"

#include <array>
#include <bit>
#include <cstdint>
#include <stdexcept>
#include <utility>

namespace aistore::chunking {

namespace {

constexpr std::uint64_t kGearSeed = 0xA15C0DE5A15C0DE5ULL;

[[nodiscard]] constexpr std::uint64_t splitmix64(std::uint64_t value) noexcept {
    value += 0x9E3779B97F4A7C15ULL;
    value = (value ^ (value >> 30U)) * 0xBF58476D1CE4E5B9ULL;
    value = (value ^ (value >> 27U)) * 0x94D049BB133111EBULL;
    return value ^ (value >> 31U);
}

[[nodiscard]] constexpr std::array<std::uint64_t, 256> make_gear_table() noexcept {
    std::array<std::uint64_t, 256> table{};
    std::uint64_t state = kGearSeed;

    for (std::uint64_t& entry : table) {
        state = splitmix64(state);
        entry = state;
    }

    return table;
}

constexpr std::array<std::uint64_t, 256> kGearTable = make_gear_table();

[[nodiscard]] bool is_power_of_two(std::uint64_t value) noexcept { return value != 0U && (value & (value - 1U)) == 0U; }

void validate_fastcdc_chunker_parameters(std::size_t min_chunk_size, std::size_t avg_chunk_size,
                                         std::size_t max_chunk_size) {
    constexpr std::uint64_t kMinimumAllowed = 64ULL;
    constexpr std::uint64_t kMaximumAllowed = 8ULL * 1024ULL * 1024ULL;

    if (min_chunk_size < kMinimumAllowed) {
        throw std::invalid_argument("FastCDC min chunk size must be at least 64 bytes");
    }

    if (avg_chunk_size < kMinimumAllowed) {
        throw std::invalid_argument("FastCDC avg chunk size must be at least 64 bytes");
    }

    if (!is_power_of_two(static_cast<std::uint64_t>(avg_chunk_size))) {
        throw std::invalid_argument("FastCDC avg chunk size must be a power of two");
    }

    if (min_chunk_size > avg_chunk_size) {
        throw std::invalid_argument("FastCDC min chunk size must be <= avg chunk size");
    }

    if (avg_chunk_size > max_chunk_size) {
        throw std::invalid_argument("FastCDC avg chunk size must be <= max chunk size");
    }

    if (max_chunk_size > kMaximumAllowed) {
        throw std::invalid_argument("FastCDC max chunk size must be <= 8 MiB");
    }
}

[[nodiscard]] std::uint64_t compute_mask_s(std::size_t avg_chunk_size) {
    const auto bits = static_cast<unsigned>(std::countr_zero(avg_chunk_size));
    return (1ULL << (bits + 1U)) - 1ULL;
}

[[nodiscard]] std::uint64_t compute_mask_l(std::size_t avg_chunk_size) {
    const auto bits = static_cast<unsigned>(std::countr_zero(avg_chunk_size));
    return (1ULL << (bits - 1U)) - 1ULL;
}

}  // namespace

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
FastCdcChunker::FastCdcChunker(std::size_t min_chunk_size, std::size_t avg_chunk_size, std::size_t max_chunk_size)
    : min_chunk_size_(min_chunk_size),
      avg_chunk_size_(avg_chunk_size),
      max_chunk_size_(max_chunk_size),
      mask_s_(compute_mask_s(avg_chunk_size)),
      mask_l_(compute_mask_l(avg_chunk_size)) {
    validate_fastcdc_chunker_parameters(min_chunk_size_, avg_chunk_size_, max_chunk_size_);
    buffer_.reserve(max_chunk_size_);
}

void FastCdcChunker::update(std::span<const std::byte> data, const ChunkConsumer& consumer) {
    if (finalized_) {
        throw std::logic_error("cannot update a finalized FastCDC chunker");
    }

    if (!consumer) {
        throw std::invalid_argument("chunk consumer must not be empty");
    }

    for (const std::byte byte : data) {
        buffer_.push_back(byte);
        gear_hash_ = (gear_hash_ << 1U) + kGearTable[static_cast<unsigned char>(byte)];
        process_buffered_bytes(consumer);
    }
}

void FastCdcChunker::finalize(const ChunkConsumer& consumer) {
    if (finalized_) {
        throw std::logic_error("FastCDC chunker has already been finalized");
    }

    if (!consumer) {
        throw std::invalid_argument("chunk consumer must not be empty");
    }

    if (!buffer_.empty()) {
        emit_current_chunk(consumer);
    }

    finalized_ = true;
}

std::size_t FastCdcChunker::min_chunk_size() const noexcept { return min_chunk_size_; }

std::size_t FastCdcChunker::avg_chunk_size() const noexcept { return avg_chunk_size_; }

std::size_t FastCdcChunker::max_chunk_size() const noexcept { return max_chunk_size_; }

std::uint64_t FastCdcChunker::bytes_received() const noexcept {
    return next_chunk_offset_ + static_cast<std::uint64_t>(buffer_.size());
}

bool FastCdcChunker::finalized() const noexcept { return finalized_; }

void FastCdcChunker::emit_current_chunk(const ChunkConsumer& consumer) {
    ChunkBuffer chunk{
        .offset = next_chunk_offset_,
        .bytes = std::move(buffer_),
    };

    buffer_.clear();
    buffer_.reserve(max_chunk_size_);
    gear_hash_ = 0;

    next_chunk_offset_ += static_cast<std::uint64_t>(chunk.bytes.size());

    consumer(std::move(chunk));
}

void FastCdcChunker::process_buffered_bytes(const ChunkConsumer& consumer) {
    const std::size_t buffered_size = buffer_.size();

    if (buffered_size < min_chunk_size_) {
        return;
    }

    if (buffered_size >= max_chunk_size_) {
        emit_current_chunk(consumer);
        return;
    }

    if (buffered_size < avg_chunk_size_) {
        if ((gear_hash_ & mask_s_) == 0U) {
            emit_current_chunk(consumer);
        }
        return;
    }

    if ((gear_hash_ & mask_l_) == 0U) {
        emit_current_chunk(consumer);
    }
}

}  // namespace aistore::chunking
