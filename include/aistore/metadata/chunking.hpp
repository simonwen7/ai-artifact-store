#ifndef AISTORE_METADATA_CHUNKING_HPP
#define AISTORE_METADATA_CHUNKING_HPP

#include <cstdint>
#include <string_view>

namespace aistore::metadata {

enum class ChunkingStrategy : std::uint8_t {
    FixedSize = 1,
    FastCdc = 2,
};

struct FastCdcParameters {
    std::uint64_t min_chunk_size_bytes;
    std::uint64_t avg_chunk_size_bytes;
    std::uint64_t max_chunk_size_bytes;

    bool operator==(const FastCdcParameters&) const = default;
};

void validate_fastcdc_parameters(const FastCdcParameters& parameters);

[[nodiscard]] std::string_view chunking_strategy_to_string(ChunkingStrategy strategy);

[[nodiscard]] ChunkingStrategy chunking_strategy_from_string(std::string_view strategy);

[[nodiscard]] std::uint8_t chunking_strategy_code(ChunkingStrategy strategy);

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_CHUNKING_HPP
