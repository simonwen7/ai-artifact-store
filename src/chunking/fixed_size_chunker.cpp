#include "aistore/chunking/fixed_size_chunker.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace aistore::chunking {

FixedSizeChunker::FixedSizeChunker(std::size_t chunk_size) : chunk_size_(chunk_size) {
    if (chunk_size_ == 0) {
        throw std::invalid_argument("chunk size must be greater than zero");
    }

    buffer_.reserve(chunk_size_);
}

void FixedSizeChunker::update(std::span<const std::byte> data, const ChunkConsumer& consumer) {
    if (finalized_) {
        throw std::logic_error("cannot update a finalized fixed-size chunker");
    }

    if (!consumer) {
        throw std::invalid_argument("chunk consumer must not be empty");
    }

    std::size_t position = 0;

    while (position < data.size()) {
        const std::size_t remaining_capacity = chunk_size_ - buffer_.size();
        const std::size_t bytes_to_copy = std::min(remaining_capacity, data.size() - position);

        const auto piece = data.subspan(position, bytes_to_copy);

        buffer_.insert(buffer_.end(), piece.begin(), piece.end());

        position += bytes_to_copy;

        if (buffer_.size() == chunk_size_) {
            emit_current_chunk(consumer);
        }
    }
}

void FixedSizeChunker::finalize(const ChunkConsumer& consumer) {
    if (finalized_) {
        throw std::logic_error("fixed-size chunker has already been finalized");
    }

    if (!consumer) {
        throw std::invalid_argument("chunk consumer must not be empty");
    }

    if (!buffer_.empty()) {
        emit_current_chunk(consumer);
    }

    finalized_ = true;
}

std::size_t FixedSizeChunker::chunk_size() const noexcept { return chunk_size_; }

std::uint64_t FixedSizeChunker::bytes_received() const noexcept {
    return next_chunk_offset_ + static_cast<std::uint64_t>(buffer_.size());
}

bool FixedSizeChunker::finalized() const noexcept { return finalized_; }

void FixedSizeChunker::emit_current_chunk(const ChunkConsumer& consumer) {
    std::vector<std::byte> next_buffer;
    next_buffer.reserve(chunk_size_);

    ChunkBuffer chunk{
        .offset = next_chunk_offset_,
        .bytes = std::move(buffer_),
    };

    buffer_ = std::move(next_buffer);

    next_chunk_offset_ += static_cast<std::uint64_t>(chunk.bytes.size());

    consumer(std::move(chunk));
}

}  // namespace aistore::chunking
