#include "aistore/metadata/chunking.hpp"

#include <stdexcept>

namespace aistore::metadata {

namespace {

[[nodiscard]] bool is_power_of_two(std::uint64_t value) noexcept { return value != 0U && (value & (value - 1U)) == 0U; }

}  // namespace

void validate_fastcdc_parameters(const FastCdcParameters& parameters) {
    constexpr std::uint64_t kMinimumAllowed = 64ULL;
    constexpr std::uint64_t kMaximumAllowed = 8ULL * 1024ULL * 1024ULL;

    if (parameters.min_chunk_size_bytes < kMinimumAllowed) {
        throw std::invalid_argument("FastCDC min chunk size must be at least 64 bytes");
    }

    if (parameters.avg_chunk_size_bytes < kMinimumAllowed) {
        throw std::invalid_argument("FastCDC avg chunk size must be at least 64 bytes");
    }

    if (!is_power_of_two(parameters.avg_chunk_size_bytes)) {
        throw std::invalid_argument("FastCDC avg chunk size must be a power of two");
    }

    if (parameters.min_chunk_size_bytes > parameters.avg_chunk_size_bytes) {
        throw std::invalid_argument("FastCDC min chunk size must be <= avg chunk size");
    }

    if (parameters.avg_chunk_size_bytes > parameters.max_chunk_size_bytes) {
        throw std::invalid_argument("FastCDC avg chunk size must be <= max chunk size");
    }

    if (parameters.max_chunk_size_bytes > kMaximumAllowed) {
        throw std::invalid_argument("FastCDC max chunk size must be <= 8 MiB");
    }
}

std::string_view chunking_strategy_to_string(ChunkingStrategy strategy) {
    switch (strategy) {
        case ChunkingStrategy::FixedSize:
            return "fixed-size";

        case ChunkingStrategy::FastCdc:
            return "fastcdc";
    }

    throw std::logic_error("unsupported chunking strategy");
}

ChunkingStrategy chunking_strategy_from_string(std::string_view strategy) {
    if (strategy == "fixed-size") {
        return ChunkingStrategy::FixedSize;
    }

    if (strategy == "fastcdc") {
        return ChunkingStrategy::FastCdc;
    }

    throw std::runtime_error("unsupported chunking strategy string");
}

std::uint8_t chunking_strategy_code(ChunkingStrategy strategy) {
    switch (strategy) {
        case ChunkingStrategy::FixedSize:
            return 1;

        case ChunkingStrategy::FastCdc:
            return 2;
    }

    throw std::logic_error("unsupported chunking strategy");
}

}  // namespace aistore::metadata
