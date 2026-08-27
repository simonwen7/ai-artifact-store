#include <algorithm>
#include <array>
#include <boost/json.hpp>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "aistore/chunking/fastcdc_chunker.hpp"
#include "aistore/chunking/fixed_size_chunker.hpp"
#include "aistore/hashing/sha256.hpp"

namespace {

using aistore::chunking::ChunkBuffer;
using aistore::chunking::FastCdcChunker;
using aistore::chunking::FixedSizeChunker;
using aistore::hashing::Sha256;

constexpr std::uint64_t kDefaultBytes = 134217728ULL;
constexpr std::uint32_t kDefaultIterations = 5U;
constexpr std::uint64_t kDefaultShiftBytes = 65536ULL;
constexpr std::uint64_t kFixedChunkSize = 4ULL * 1024ULL * 1024ULL;
constexpr std::uint64_t kFastCdcMin = 2ULL * 1024ULL * 1024ULL;
constexpr std::uint64_t kFastCdcAvg = 4ULL * 1024ULL * 1024ULL;
constexpr std::uint64_t kFastCdcMax = 8ULL * 1024ULL * 1024ULL;
constexpr std::uint64_t kPrngSeed = 0xA15C0DE5BEEC0FFEULL;
constexpr std::uint64_t kShiftPrefixSeed = 0xC0FFEEF00DC0DE42ULL;

struct ByteCount {
    std::uint64_t value;
};

struct PrngSeed {
    std::uint64_t value;
};

[[nodiscard]] std::uint64_t xorshift64star(std::uint64_t& state) {
    state ^= state << 13U;
    state ^= state >> 7U;
    state ^= state << 17U;
    return state * 0x2545F4914F6CDD1DULL;
}

[[nodiscard]] std::vector<std::byte> generate_bytes(ByteCount byte_count, PrngSeed seed) {
    std::vector<std::byte> bytes;
    bytes.resize(static_cast<std::size_t>(byte_count.value));
    std::uint64_t state = seed.value;

    for (std::uint64_t index = 0; index < byte_count.value; ++index) {
        bytes[static_cast<std::size_t>(index)] = static_cast<std::byte>(xorshift64star(state) & 0xFFU);
    }

    return bytes;
}

[[nodiscard]] std::vector<std::byte> make_shifted(const std::vector<std::byte>& base, ByteCount shift_bytes) {
    std::vector<std::byte> shifted;
    shifted.reserve(base.size() + static_cast<std::size_t>(shift_bytes.value));
    const std::vector<std::byte> prefix = generate_bytes(shift_bytes, PrngSeed{kShiftPrefixSeed});
    shifted.insert(shifted.end(), prefix.begin(), prefix.end());
    shifted.insert(shifted.end(), base.begin(), base.end());
    return shifted;
}

[[nodiscard]] std::string digest_to_hex(const Sha256::Digest& digest) {
    constexpr std::array<char, 16> kHex{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    std::string hex;
    hex.resize(digest.size() * 2U);

    for (std::size_t index = 0; index < digest.size(); ++index) {
        const auto value = std::to_integer<unsigned int>(digest[index]);
        hex[index * 2U] = kHex[(value >> 4U) & 0x0FU];
        hex[index * 2U + 1U] = kHex[value & 0x0FU];
    }

    return hex;
}

[[nodiscard]] std::string hash_chunk_id(std::span<const std::byte> bytes) {
    Sha256 hasher;
    hasher.update(bytes);
    return digest_to_hex(hasher.finalize());
}

struct ChunkPassResult {
    std::vector<std::string> chunk_ids;
    std::vector<std::uint64_t> chunk_sizes;
    std::uint64_t sink = 0;
};

template <typename Chunker>
[[nodiscard]] ChunkPassResult chunk_and_hash(Chunker& chunker, std::span<const std::byte> data) {
    ChunkPassResult result;

    const auto consume = [&](ChunkBuffer chunk) {
        result.sink ^= static_cast<std::uint64_t>(chunk.bytes.size());
        result.sink ^= chunk.offset;
        result.chunk_sizes.push_back(static_cast<std::uint64_t>(chunk.bytes.size()));
        std::string chunk_id = hash_chunk_id(chunk.bytes);
        result.sink ^= static_cast<std::uint64_t>(chunk_id.empty() ? 0U : static_cast<unsigned char>(chunk_id.front()));
        result.chunk_ids.push_back(std::move(chunk_id));
    };

    constexpr std::size_t kFeed = std::size_t{1024} * std::size_t{1024};
    std::size_t offset = 0;

    while (offset < data.size()) {
        const std::size_t n = std::min(kFeed, data.size() - offset);
        chunker.update(data.subspan(offset, n), consume);
        offset += n;
    }

    chunker.finalize(consume);
    return result;
}

struct ThroughputStats {
    double median_elapsed_ms = 0.0;
    double median_mib_per_second = 0.0;
    std::uint64_t sink = 0;
};

[[nodiscard]] double median_of(std::vector<double> values) {
    if (values.empty()) {
        throw std::runtime_error("median requires at least one sample");
    }

    std::ranges::sort(values);
    const std::size_t mid = values.size() / 2U;

    if ((values.size() % 2U) == 0U) {
        return (values[mid - 1U] + values[mid]) * 0.5;
    }

    return values[mid];
}

template <typename MakeChunker>
[[nodiscard]] ThroughputStats measure_throughput(std::span<const std::byte> data, std::uint32_t iterations,
                                                 MakeChunker make_chunker) {
    std::vector<double> elapsed_ms;
    elapsed_ms.reserve(iterations);
    std::uint64_t sink = 0;

    for (std::uint32_t iteration = 0; iteration < iterations; ++iteration) {
        auto chunker = make_chunker();
        const auto started = std::chrono::steady_clock::now();
        const ChunkPassResult pass = chunk_and_hash(chunker, data);
        const auto finished = std::chrono::steady_clock::now();
        sink ^= pass.sink;
        sink ^= static_cast<std::uint64_t>(pass.chunk_ids.size());
        const double ms = std::chrono::duration<double, std::milli>(finished - started).count();
        elapsed_ms.push_back(ms);
    }

    const double median_ms = median_of(std::move(elapsed_ms));
    const double seconds = median_ms / 1000.0;
    const double mib = static_cast<double>(data.size()) / (1024.0 * 1024.0);
    const double mib_per_second = seconds > 0.0 ? (mib / seconds) : 0.0;

    return ThroughputStats{
        .median_elapsed_ms = median_ms,
        .median_mib_per_second = mib_per_second,
        .sink = sink,
    };
}

struct DedupStats {
    std::uint64_t base_chunk_count = 0;
    std::uint64_t shifted_chunk_count = 0;
    std::uint64_t shared_chunk_count = 0;
    std::uint64_t reused_logical_bytes = 0;
    double reuse_ratio = 0.0;
};

struct BaseBytes {
    std::span<const std::byte> bytes;
};

struct ShiftedBytes {
    std::span<const std::byte> bytes;
};

template <typename MakeChunker>
[[nodiscard]] DedupStats measure_dedup(BaseBytes base, ShiftedBytes shifted, MakeChunker make_chunker) {
    auto base_chunker = make_chunker();
    auto shifted_chunker = make_chunker();
    const ChunkPassResult base_pass = chunk_and_hash(base_chunker, base.bytes);
    const ChunkPassResult shifted_pass = chunk_and_hash(shifted_chunker, shifted.bytes);

    std::set<std::string> base_ids(base_pass.chunk_ids.begin(), base_pass.chunk_ids.end());
    std::set<std::string> shared;

    std::uint64_t reused_logical_bytes = 0;

    for (std::size_t index = 0; index < shifted_pass.chunk_ids.size(); ++index) {
        if (base_ids.contains(shifted_pass.chunk_ids[index])) {
            shared.insert(shifted_pass.chunk_ids[index]);
            reused_logical_bytes += shifted_pass.chunk_sizes[index];
        }
    }

    const double reuse_ratio =
        shifted.bytes.empty() ? 0.0
                              : static_cast<double>(reused_logical_bytes) / static_cast<double>(shifted.bytes.size());

    return DedupStats{
        .base_chunk_count = base_pass.chunk_ids.size(),
        .shifted_chunk_count = shifted_pass.chunk_ids.size(),
        .shared_chunk_count = shared.size(),
        .reused_logical_bytes = reused_logical_bytes,
        .reuse_ratio = reuse_ratio,
    };
}

struct FlagName {
    std::string_view value;
};

[[nodiscard]] std::uint64_t parse_u64_flag(FlagName flag_name, std::string_view text) {
    if (text.empty()) {
        throw std::invalid_argument(std::string{flag_name.value} + " requires a positive decimal integer");
    }

    std::uint64_t value = 0;

    for (const char character : text) {
        if (character < '0' || character > '9') {
            throw std::invalid_argument(std::string{flag_name.value} + " must be a decimal integer");
        }

        const auto digit = static_cast<std::uint64_t>(character - '0');

        if (value > (UINT64_MAX - digit) / 10ULL) {
            throw std::invalid_argument(std::string{flag_name.value} + " overflows uint64");
        }

        value = value * 10ULL + digit;
    }

    return value;
}

void print_help(std::ostream& out) {
    out << "Usage:\n"
           "  aistore-bench --help\n"
           "  aistore-bench chunking [options]\n"
           "\n"
           "Options:\n"
           "  --bytes <uint64>         dataset size (default 134217728)\n"
           "  --iterations <uint32>    timing iterations (default 5)\n"
           "  --shift-bytes <uint64>   shift prefix length (default 65536)\n"
           "\n"
           "Notes:\n"
           "  Measures production FixedSizeChunker + FastCdcChunker with SHA-256 chunk IDs.\n"
           "  Benchmark datasets are allocated in memory and are NOT a claim about\n"
           "  production Push/Pull RSS.\n";
}

struct ChunkingOptions {
    std::uint64_t bytes = kDefaultBytes;
    std::uint32_t iterations = kDefaultIterations;
    std::uint64_t shift_bytes = kDefaultShiftBytes;
};

[[nodiscard]] ChunkingOptions parse_chunking_options(int argc, char** argv) {
    ChunkingOptions options;

    for (int index = 2; index < argc; ++index) {
        const std::string_view arg{argv[index]};

        auto require_value = [&](std::string_view flag) -> std::string_view {
            if (index + 1 >= argc) {
                throw std::invalid_argument(std::string{flag} + " requires a value");
            }

            ++index;
            return std::string_view{argv[index]};
        };

        if (arg == "--bytes") {
            options.bytes = parse_u64_flag(FlagName{arg}, require_value(arg));

            if (options.bytes == 0U) {
                throw std::invalid_argument("--bytes must be greater than zero");
            }
        } else if (arg == "--iterations") {
            const std::uint64_t value = parse_u64_flag(FlagName{arg}, require_value(arg));

            if (value == 0U || value > UINT32_MAX) {
                throw std::invalid_argument("--iterations must be in 1..UINT32_MAX");
            }

            options.iterations = static_cast<std::uint32_t>(value);
        } else if (arg == "--shift-bytes") {
            options.shift_bytes = parse_u64_flag(FlagName{arg}, require_value(arg));

            if (options.shift_bytes == 0U) {
                throw std::invalid_argument("--shift-bytes must be greater than zero");
            }
        } else {
            throw std::invalid_argument("unknown chunking option: " + std::string{arg});
        }
    }

    if (options.bytes > UINT64_MAX - options.shift_bytes) {
        throw std::invalid_argument("bytes + shift-bytes overflows uint64");
    }

    return options;
}

[[nodiscard]] boost::json::object strategy_object(std::uint64_t chunk_size_or_zero, bool fastcdc,
                                                  const ThroughputStats& throughput, const DedupStats& dedup) {
    boost::json::object object;
    object["median_elapsed_ms"] = throughput.median_elapsed_ms;
    object["median_mib_per_second"] = throughput.median_mib_per_second;
    object["base_chunk_count"] = dedup.base_chunk_count;
    object["shifted_chunk_count"] = dedup.shifted_chunk_count;
    object["shared_chunk_count"] = dedup.shared_chunk_count;
    object["reused_logical_bytes"] = dedup.reused_logical_bytes;
    object["reuse_ratio"] = dedup.reuse_ratio;

    if (fastcdc) {
        object["min_chunk_size_bytes"] = kFastCdcMin;
        object["avg_chunk_size_bytes"] = kFastCdcAvg;
        object["max_chunk_size_bytes"] = kFastCdcMax;
    } else {
        object["chunk_size_bytes"] = chunk_size_or_zero;
    }

    return object;
}

[[nodiscard]] int run_chunking(const ChunkingOptions& options) {
    std::cerr << "aistore-bench: generating deterministic dataset (" << options.bytes << " bytes)\n";
    const std::vector<std::byte> base = generate_bytes(ByteCount{options.bytes}, PrngSeed{kPrngSeed});
    const std::vector<std::byte> shifted = make_shifted(base, ByteCount{options.shift_bytes});

    std::cerr << "aistore-bench: FixedSize throughput (" << options.iterations << " iterations)\n";
    const ThroughputStats fixed_throughput = measure_throughput(
        base, options.iterations, [] { return FixedSizeChunker{static_cast<std::size_t>(kFixedChunkSize)}; });

    std::cerr << "aistore-bench: FastCDC throughput (" << options.iterations << " iterations)\n";
    const ThroughputStats fast_throughput = measure_throughput(base, options.iterations, [] {
        return FastCdcChunker{static_cast<std::size_t>(kFastCdcMin), static_cast<std::size_t>(kFastCdcAvg),
                              static_cast<std::size_t>(kFastCdcMax)};
    });

    std::cerr << "aistore-bench: FixedSize shifted-content dedup\n";
    const DedupStats fixed_dedup = measure_dedup(BaseBytes{base}, ShiftedBytes{shifted}, [] {
        return FixedSizeChunker{static_cast<std::size_t>(kFixedChunkSize)};
    });

    std::cerr << "aistore-bench: FastCDC shifted-content dedup\n";
    const DedupStats fast_dedup = measure_dedup(BaseBytes{base}, ShiftedBytes{shifted}, [] {
        return FastCdcChunker{static_cast<std::size_t>(kFastCdcMin), static_cast<std::size_t>(kFastCdcAvg),
                              static_cast<std::size_t>(kFastCdcMax)};
    });

    if ((fixed_throughput.sink ^ fast_throughput.sink) == UINT64_MAX) {
        std::cerr << "aistore-bench: unreachable sink sentinel\n";
    }

    boost::json::object root;
    root["schema_version"] = 1;
    root["benchmark"] = "chunking";
    root["dataset_bytes"] = options.bytes;
    root["shifted_dataset_bytes"] = options.bytes + options.shift_bytes;
    root["iterations"] = options.iterations;
    root["shift_bytes"] = options.shift_bytes;
    root["fixed_size"] = strategy_object(kFixedChunkSize, false, fixed_throughput, fixed_dedup);
    root["fastcdc"] = strategy_object(0, true, fast_throughput, fast_dedup);

    std::cout << boost::json::serialize(root) << '\n';
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc < 2) {
            print_help(std::cerr);
            return 2;
        }

        const std::string_view command{argv[1]};

        if (command == "--help" || command == "help" || command == "-h") {
            print_help(std::cout);
            return 0;
        }

        if (command == "chunking") {
            return run_chunking(parse_chunking_options(argc, argv));
        }

        throw std::invalid_argument("unknown command: " + std::string{command});
    } catch (const std::exception& error) {
        std::cerr << "aistore-bench error: " << error.what() << '\n';
        return 1;
    }
}
