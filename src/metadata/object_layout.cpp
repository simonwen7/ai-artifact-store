#include "aistore/metadata/object_layout.hpp"

#include <limits>
#include <stdexcept>
#include <string_view>
#include <utility>

namespace aistore::metadata {

namespace {

bool is_valid_chunk_id(std::string_view chunk_id) {
    if (chunk_id.size() != 64) {
        return false;
    }

    for (const char character : chunk_id) {
        const bool is_digit = character >= '0' && character <= '9';

        const bool is_lower_hex = character >= 'a' && character <= 'f';

        if (!is_digit && !is_lower_hex) {
            return false;
        }
    }

    return true;
}

}  // namespace

ObjectLayout::ObjectLayout(std::vector<ChunkRef> chunks) : chunks_(std::move(chunks)) {
    validate(chunks_);

    if (!chunks_.empty()) {
        const ChunkRef& last = chunks_.back();
        total_size_ = last.offset + last.size;
    }
}

const std::vector<ChunkRef>& ObjectLayout::chunks() const noexcept { return chunks_; }

std::uint64_t ObjectLayout::total_size() const noexcept { return total_size_; }

bool ObjectLayout::empty() const noexcept { return chunks_.empty(); }

void ObjectLayout::validate(const std::vector<ChunkRef>& chunks) {
    std::uint64_t expected_offset = 0;

    for (const ChunkRef& chunk : chunks) {
        if (!is_valid_chunk_id(chunk.chunk_id)) {
            throw std::invalid_argument("object layout contains an invalid chunk ID");
        }

        if (chunk.size == 0) {
            throw std::invalid_argument("object layout contains a zero-sized chunk");
        }

        if (chunk.offset != expected_offset) {
            throw std::invalid_argument("object layout chunks must be contiguous and ordered");
        }

        if (chunk.size > std::numeric_limits<std::uint64_t>::max() - expected_offset) {
            throw std::overflow_error("object layout size exceeds uint64_t range");
        }

        expected_offset += chunk.size;
    }
}

}  // namespace aistore::metadata
