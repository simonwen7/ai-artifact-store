#include <algorithm>
#include <boost/asio/ip/address.hpp>
#include <boost/json.hpp>
#include <boost/system/error_code.hpp>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <map>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "aistore/client/client_error.hpp"
#include "aistore/client/metadata_client.hpp"
#include "aistore/client/storage_node_client.hpp"
#include "aistore/client/storage_node_client_pool.hpp"
#include "aistore/gc/gc_engine.hpp"
#include "aistore/http/http_client.hpp"
#include "aistore/metadata/chunking.hpp"
#include "aistore/metadata/finalize_upload.hpp"
#include "aistore/metadata/gc.hpp"
#include "aistore/metadata/storage_node.hpp"
#include "aistore/metadata/upload_session.hpp"
#include "aistore/metadata/uuid_v7.hpp"
#include "aistore/pull/pull_engine.hpp"
#include "aistore/push/push_engine.hpp"
#include "aistore/replication/repair_engine.hpp"

namespace {

class CliUsageError : public std::runtime_error {
   public:
    explicit CliUsageError(const std::string& message) : std::runtime_error{message} {}
};

[[nodiscard]] std::string_view next_arg(int argc, char** argv, int& index, std::string_view option_name) {
    if (index + 1 >= argc) {
        throw CliUsageError{std::string{"missing value for "} + std::string{option_name}};
    }

    ++index;
    return argv[index];
}

void require_unset(bool already_set, std::string_view option_name) {
    if (already_set) {
        throw CliUsageError{std::string{"duplicate option "} + std::string{option_name}};
    }
}

void print_top_level_usage(std::ostream& out) {
    out << "Usage:\n"
           "  aistore push [options]\n"
           "  aistore pull [options]\n"
           "  aistore gc [options]\n"
           "  aistore node <subcommand> [options]\n"
           "  aistore repair [options]\n"
           "\n"
           "Commands:\n"
           "  push\n"
           "  pull\n"
           "  gc\n"
           "  node\n"
           "  repair\n"
           "\n"
           "Run `aistore push --help` for push options.\n"
           "Run `aistore pull --help` for pull options.\n"
           "Run `aistore gc --help` for gc options.\n"
           "Run `aistore node --help` for node options.\n"
           "Run `aistore repair --help` for repair options.\n";
}

void print_push_usage(std::ostream& out) {
    out << "Usage:\n"
           "  aistore push [options]\n"
           "\n"
           "Required:\n"
           "  --file <path>\n"
           "  --artifact-id <uuidv7>\n"
           "\n"
           "Optional:\n"
           "  --session-id <uuidv7>\n"
           "  --replication-factor <1..8>\n"
           "  --chunking-strategy <fixed-size|fastcdc>\n"
           "  --chunk-size <bytes>\n"
           "  --min-chunk-size <bytes>\n"
           "  --avg-chunk-size <bytes>\n"
           "  --max-chunk-size <bytes>\n"
           "  --parent-version <64-lowercase-hex>\n"
           "  --metadata <KEY=VALUE>\n"
           "  --metadata-address <numeric-ip>\n"
           "  --metadata-port <port>\n"
           "\n"
           "Defaults:\n"
           "  metadata 127.0.0.1:8080\n"
           "  replication-factor 1\n"
           "  chunking-strategy fixed-size\n"
           "  chunk-size 4194304\n"
           "  min-chunk-size 2097152\n"
           "  avg-chunk-size 4194304\n"
           "  max-chunk-size 8388608\n";
}

void print_pull_usage(std::ostream& out) {
    out << "Usage:\n"
           "  aistore pull [options]\n"
           "\n"
           "Required:\n"
           "  --version-id <64-lowercase-hex>\n"
           "  --output <path>\n"
           "\n"
           "Optional:\n"
           "  --overwrite\n"
           "  --metadata-address <numeric-ip>\n"
           "  --metadata-port <port>\n"
           "\n"
           "Defaults:\n"
           "  metadata 127.0.0.1:8080\n";
}

void print_gc_usage(std::ostream& out) {
    out << "Usage:\n"
           "  aistore gc [options]\n"
           "\n"
           "Required:\n"
           "  --storage-node-id <node-id>\n"
           "\n"
           "Optional:\n"
           "  --gc-run-id <uuidv7>\n"
           "  --dry-run\n"
           "  --metadata-address <numeric-ip>\n"
           "  --metadata-port <port>\n"
           "\n"
           "Defaults:\n"
           "  metadata 127.0.0.1:8080\n";
}

[[nodiscard]] bool is_regular_file_path(const std::filesystem::path& path) {
    std::error_code error;
    return std::filesystem::is_regular_file(path, error) && !error;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] std::uint64_t parse_strict_u64(std::string_view text, std::string_view field_name) {
    if (text.empty()) {
        throw CliUsageError{std::string{field_name} + " must be a positive decimal integer"};
    }

    std::uint64_t value = 0;
    for (const char character : text) {
        if (character < '0' || character > '9') {
            throw CliUsageError{std::string{field_name} + " must be a positive decimal integer"};
        }

        const auto digit = static_cast<std::uint64_t>(character - '0');
        if (value > (UINT64_MAX - digit) / 10ULL) {
            throw CliUsageError{std::string{field_name} + " is out of range"};
        }

        value = (value * 10ULL) + digit;
    }

    return value;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
[[nodiscard]] std::uint16_t parse_port(std::string_view text, std::string_view field_name) {
    const std::uint64_t value = parse_strict_u64(text, field_name);
    if (value < 1ULL || value > 65535ULL) {
        throw CliUsageError{std::string{field_name} + " must be in range 1..65535"};
    }

    return static_cast<std::uint16_t>(value);
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
void validate_numeric_ip(std::string_view address, std::string_view field_name) {
    if (address.empty()) {
        throw CliUsageError{std::string{field_name} + " must be a numeric IPv4 or IPv6 address"};
    }

    boost::system::error_code address_error;
    (void)boost::asio::ip::make_address(std::string{address}, address_error);
    if (address_error) {
        throw CliUsageError{std::string{field_name} + " must be a numeric IPv4 or IPv6 address"};
    }
}

void validate_storage_node_id(std::string_view node_id) {
    if (node_id.empty() || node_id.size() > 128U) {
        throw CliUsageError{"--storage-node-id must be 1 to 128 characters"};
    }

    for (const char character : node_id) {
        const bool is_upper = character >= 'A' && character <= 'Z';
        const bool is_lower = character >= 'a' && character <= 'z';
        const bool is_digit = character >= '0' && character <= '9';
        const bool is_allowed_punct = character == '-' || character == '_' || character == '.';

        if (!is_upper && !is_lower && !is_digit && !is_allowed_punct) {
            throw CliUsageError{"--storage-node-id contains an invalid character"};
        }
    }
}

void validate_parent_version(std::string_view parent_version) {
    if (parent_version.size() != 64U) {
        throw CliUsageError{"--parent-version must be exactly 64 lowercase hex characters"};
    }

    for (const char character : parent_version) {
        const bool is_digit = character >= '0' && character <= '9';
        const bool is_lower_hex = character >= 'a' && character <= 'f';
        if (!is_digit && !is_lower_hex) {
            throw CliUsageError{"--parent-version must be exactly 64 lowercase hex characters"};
        }
    }
}

void validate_version_id(std::string_view version_id) {
    if (version_id.size() != 64U) {
        throw CliUsageError{"--version-id must be exactly 64 lowercase hex characters"};
    }

    for (const char character : version_id) {
        const bool is_digit = character >= '0' && character <= '9';
        const bool is_lower_hex = character >= 'a' && character <= 'f';
        if (!is_digit && !is_lower_hex) {
            throw CliUsageError{"--version-id must be exactly 64 lowercase hex characters"};
        }
    }
}

[[nodiscard]] std::pair<std::string, std::string> parse_metadata_kv(std::string_view raw) {
    const auto separator = raw.find('=');
    if (separator == std::string_view::npos) {
        throw CliUsageError{"--metadata must be KEY=VALUE"};
    }

    const std::string key{raw.substr(0, separator)};
    const std::string value{raw.substr(separator + 1)};
    if (key.empty()) {
        throw CliUsageError{"--metadata key must be nonempty"};
    }

    return {key, value};
}

struct PushOptions {
    std::filesystem::path file_path;
    aistore::metadata::UuidV7 artifact_id;
    aistore::metadata::UuidV7 session_id;
    std::uint8_t replication_factor = 1;
    aistore::metadata::ChunkingStrategy chunking_strategy = aistore::metadata::ChunkingStrategy::FixedSize;
    std::uint64_t chunk_size_bytes = 4194304ULL;
    std::uint64_t min_chunk_size_bytes = 2097152ULL;
    std::uint64_t avg_chunk_size_bytes = 4194304ULL;
    std::uint64_t max_chunk_size_bytes = 8388608ULL;
    std::optional<std::string> parent_version_id;
    aistore::metadata::UploadSession::ImmutableMetadata immutable_metadata;
    std::string metadata_address = "127.0.0.1";
    std::uint16_t metadata_port = 8080;
};

[[nodiscard]] aistore::metadata::ChunkingStrategy parse_chunking_strategy(std::string_view text) {
    if (text == "fixed-size") {
        return aistore::metadata::ChunkingStrategy::FixedSize;
    }

    if (text == "fastcdc") {
        return aistore::metadata::ChunkingStrategy::FastCdc;
    }

    throw CliUsageError{"--chunking-strategy must be fixed-size or fastcdc"};
}

[[nodiscard]] PushOptions parse_push_options(int argc, char** argv) {
    std::optional<aistore::metadata::UuidV7> artifact_id;
    std::optional<aistore::metadata::UuidV7> session_id;
    std::filesystem::path file_path;
    std::uint8_t replication_factor = 1;
    aistore::metadata::ChunkingStrategy chunking_strategy = aistore::metadata::ChunkingStrategy::FixedSize;
    std::uint64_t chunk_size_bytes = 4194304ULL;
    std::uint64_t min_chunk_size_bytes = 2097152ULL;
    std::uint64_t avg_chunk_size_bytes = 4194304ULL;
    std::uint64_t max_chunk_size_bytes = 8388608ULL;
    std::optional<std::string> parent_version_id;
    aistore::metadata::UploadSession::ImmutableMetadata immutable_metadata;
    std::string metadata_address = "127.0.0.1";
    std::uint16_t metadata_port = 8080;

    bool has_file = false;
    bool has_artifact_id = false;
    bool has_session_id = false;
    bool has_replication_factor = false;
    bool has_chunking_strategy = false;
    bool has_chunk_size = false;
    bool has_min_chunk_size = false;
    bool has_avg_chunk_size = false;
    bool has_max_chunk_size = false;
    bool has_parent_version = false;
    bool has_metadata_address = false;
    bool has_metadata_port = false;

    for (int index = 2; index < argc; ++index) {
        const std::string_view arg{argv[index]};

        if (arg == "--help") {
            print_push_usage(std::cout);
            std::exit(0);
        }

        if (!arg.empty() && arg[0] != '-') {
            throw CliUsageError{"positional arguments are not supported after push"};
        }

        if (arg == "--file") {
            require_unset(has_file, "--file");
            has_file = true;
            file_path = std::filesystem::path{std::string{next_arg(argc, argv, index, "--file")}};
            continue;
        }

        if (arg == "--artifact-id") {
            require_unset(has_artifact_id, "--artifact-id");
            has_artifact_id = true;
            const auto value = next_arg(argc, argv, index, "--artifact-id");
            try {
                artifact_id.emplace(std::string{value});
            } catch (const std::invalid_argument& error) {
                throw CliUsageError{error.what()};
            }
            continue;
        }

        if (arg == "--replication-factor") {
            require_unset(has_replication_factor, "--replication-factor");
            has_replication_factor = true;
            const std::uint64_t value =
                parse_strict_u64(next_arg(argc, argv, index, "--replication-factor"), "--replication-factor");
            if (value < 1ULL || value > 8ULL) {
                throw CliUsageError{"--replication-factor must be in range 1..8"};
            }
            replication_factor = static_cast<std::uint8_t>(value);
            continue;
        }

        if (arg == "--session-id") {
            require_unset(has_session_id, "--session-id");
            has_session_id = true;
            const auto value = next_arg(argc, argv, index, "--session-id");
            try {
                session_id.emplace(std::string{value});
            } catch (const std::invalid_argument& error) {
                throw CliUsageError{error.what()};
            }
            continue;
        }

        if (arg == "--chunking-strategy") {
            require_unset(has_chunking_strategy, "--chunking-strategy");
            has_chunking_strategy = true;
            chunking_strategy = parse_chunking_strategy(next_arg(argc, argv, index, "--chunking-strategy"));
            continue;
        }

        if (arg == "--chunk-size") {
            require_unset(has_chunk_size, "--chunk-size");
            has_chunk_size = true;
            chunk_size_bytes = parse_strict_u64(next_arg(argc, argv, index, "--chunk-size"), "--chunk-size");
            continue;
        }

        if (arg == "--min-chunk-size") {
            require_unset(has_min_chunk_size, "--min-chunk-size");
            has_min_chunk_size = true;
            min_chunk_size_bytes =
                parse_strict_u64(next_arg(argc, argv, index, "--min-chunk-size"), "--min-chunk-size");
            continue;
        }

        if (arg == "--avg-chunk-size") {
            require_unset(has_avg_chunk_size, "--avg-chunk-size");
            has_avg_chunk_size = true;
            avg_chunk_size_bytes =
                parse_strict_u64(next_arg(argc, argv, index, "--avg-chunk-size"), "--avg-chunk-size");
            continue;
        }

        if (arg == "--max-chunk-size") {
            require_unset(has_max_chunk_size, "--max-chunk-size");
            has_max_chunk_size = true;
            max_chunk_size_bytes =
                parse_strict_u64(next_arg(argc, argv, index, "--max-chunk-size"), "--max-chunk-size");
            continue;
        }

        if (arg == "--parent-version") {
            require_unset(has_parent_version, "--parent-version");
            has_parent_version = true;
            const auto value = next_arg(argc, argv, index, "--parent-version");
            validate_parent_version(value);
            parent_version_id = std::string{value};
            continue;
        }

        if (arg == "--metadata") {
            const auto value = next_arg(argc, argv, index, "--metadata");
            auto [key, metadata_value] = parse_metadata_kv(value);
            if (!immutable_metadata.emplace(std::move(key), std::move(metadata_value)).second) {
                throw CliUsageError{"duplicate --metadata key"};
            }
            continue;
        }

        if (arg == "--metadata-address") {
            require_unset(has_metadata_address, "--metadata-address");
            has_metadata_address = true;
            metadata_address = std::string{next_arg(argc, argv, index, "--metadata-address")};
            continue;
        }

        if (arg == "--metadata-port") {
            require_unset(has_metadata_port, "--metadata-port");
            has_metadata_port = true;
            metadata_port = parse_port(next_arg(argc, argv, index, "--metadata-port"), "--metadata-port");
            continue;
        }

        throw CliUsageError{std::string{"unknown option "} + std::string{arg}};
    }

    if (!has_file) {
        throw CliUsageError{"--file is required"};
    }
    if (!has_artifact_id || !artifact_id.has_value()) {
        throw CliUsageError{"--artifact-id is required"};
    }

    if (file_path.empty()) {
        throw CliUsageError{"--file must be nonempty"};
    }
    if (!is_regular_file_path(file_path)) {
        throw CliUsageError{"--file must exist and resolve to a regular file"};
    }

    validate_numeric_ip(metadata_address, "--metadata-address");

    const bool has_fastcdc_flag = has_min_chunk_size || has_avg_chunk_size || has_max_chunk_size;

    if (chunking_strategy == aistore::metadata::ChunkingStrategy::FixedSize) {
        if (has_fastcdc_flag) {
            throw CliUsageError{"FastCDC chunk size flags are not valid with fixed-size chunking strategy"};
        }

        if (chunk_size_bytes == 0ULL) {
            throw CliUsageError{"--chunk-size must be greater than 0"};
        }
        if (chunk_size_bytes > aistore::push::PushEngine::kMaxM4ChunkSize) {
            throw CliUsageError{"--chunk-size exceeds maximum supported chunk size"};
        }
    } else {
        if (has_chunk_size) {
            throw CliUsageError{"--chunk-size is not valid with fastcdc chunking strategy"};
        }

        try {
            aistore::metadata::validate_fastcdc_parameters(aistore::metadata::FastCdcParameters{
                .min_chunk_size_bytes = min_chunk_size_bytes,
                .avg_chunk_size_bytes = avg_chunk_size_bytes,
                .max_chunk_size_bytes = max_chunk_size_bytes,
            });
        } catch (const std::invalid_argument& error) {
            throw CliUsageError{error.what()};
        }
    }

    if (!session_id.has_value()) {
        session_id = aistore::metadata::UuidV7::generate();
    }

    return PushOptions{
        .file_path = std::move(file_path),
        .artifact_id = std::move(*artifact_id),
        .session_id = std::move(*session_id),
        .replication_factor = replication_factor,
        .chunking_strategy = chunking_strategy,
        .chunk_size_bytes = chunk_size_bytes,
        .min_chunk_size_bytes = min_chunk_size_bytes,
        .avg_chunk_size_bytes = avg_chunk_size_bytes,
        .max_chunk_size_bytes = max_chunk_size_bytes,
        .parent_version_id = std::move(parent_version_id),
        .immutable_metadata = std::move(immutable_metadata),
        .metadata_address = std::move(metadata_address),
        .metadata_port = metadata_port,
    };
}

struct PullOptions {
    std::string version_id;
    std::filesystem::path output_path;
    bool overwrite = false;
    std::string metadata_address = "127.0.0.1";
    std::uint16_t metadata_port = 8080;
};

[[nodiscard]] PullOptions parse_pull_options(int argc, char** argv) {
    std::string version_id;
    std::filesystem::path output_path;
    bool overwrite = false;
    std::string metadata_address = "127.0.0.1";
    std::uint16_t metadata_port = 8080;

    bool has_version_id = false;
    bool has_output = false;
    bool has_overwrite = false;
    bool has_metadata_address = false;
    bool has_metadata_port = false;

    for (int index = 2; index < argc; ++index) {
        const std::string_view arg{argv[index]};

        if (arg == "--help") {
            print_pull_usage(std::cout);
            std::exit(0);
        }

        if (!arg.empty() && arg[0] != '-') {
            throw CliUsageError{"positional arguments are not supported after pull"};
        }

        if (arg == "--version-id") {
            require_unset(has_version_id, "--version-id");
            has_version_id = true;
            version_id = std::string{next_arg(argc, argv, index, "--version-id")};
            continue;
        }

        if (arg == "--output") {
            require_unset(has_output, "--output");
            has_output = true;
            output_path = std::filesystem::path{std::string{next_arg(argc, argv, index, "--output")}};
            continue;
        }

        if (arg == "--overwrite") {
            require_unset(has_overwrite, "--overwrite");
            has_overwrite = true;
            overwrite = true;
            continue;
        }

        if (arg == "--metadata-address") {
            require_unset(has_metadata_address, "--metadata-address");
            has_metadata_address = true;
            metadata_address = std::string{next_arg(argc, argv, index, "--metadata-address")};
            continue;
        }

        if (arg == "--metadata-port") {
            require_unset(has_metadata_port, "--metadata-port");
            has_metadata_port = true;
            metadata_port = parse_port(next_arg(argc, argv, index, "--metadata-port"), "--metadata-port");
            continue;
        }

        throw CliUsageError{std::string{"unknown option "} + std::string{arg}};
    }

    if (!has_version_id) {
        throw CliUsageError{"--version-id is required"};
    }
    if (!has_output) {
        throw CliUsageError{"--output is required"};
    }

    validate_version_id(version_id);

    if (output_path.empty()) {
        throw CliUsageError{"--output must be nonempty"};
    }

    validate_numeric_ip(metadata_address, "--metadata-address");

    return PullOptions{
        .version_id = std::move(version_id),
        .output_path = std::move(output_path),
        .overwrite = overwrite,
        .metadata_address = std::move(metadata_address),
        .metadata_port = metadata_port,
    };
}

struct GcOptions {
    aistore::metadata::UuidV7 gc_run_id;
    std::string storage_node_id;
    bool dry_run = false;
    std::string metadata_address = "127.0.0.1";
    std::uint16_t metadata_port = 8080;
};

[[nodiscard]] GcOptions parse_gc_options(int argc, char** argv) {
    std::optional<aistore::metadata::UuidV7> gc_run_id;
    std::string storage_node_id;
    bool dry_run = false;
    std::string metadata_address = "127.0.0.1";
    std::uint16_t metadata_port = 8080;

    bool has_storage_node_id = false;
    bool has_gc_run_id = false;
    bool has_dry_run = false;
    bool has_metadata_address = false;
    bool has_metadata_port = false;

    for (int index = 2; index < argc; ++index) {
        const std::string_view arg{argv[index]};

        if (arg == "--help") {
            print_gc_usage(std::cout);
            std::exit(0);
        }

        if (!arg.empty() && arg[0] != '-') {
            throw CliUsageError{"positional arguments are not supported after gc"};
        }

        if (arg == "--storage-node-id") {
            require_unset(has_storage_node_id, "--storage-node-id");
            has_storage_node_id = true;
            storage_node_id = std::string{next_arg(argc, argv, index, "--storage-node-id")};
            continue;
        }

        if (arg == "--gc-run-id") {
            require_unset(has_gc_run_id, "--gc-run-id");
            has_gc_run_id = true;
            const auto value = next_arg(argc, argv, index, "--gc-run-id");
            try {
                gc_run_id.emplace(std::string{value});
            } catch (const std::invalid_argument& error) {
                throw CliUsageError{error.what()};
            }
            continue;
        }

        if (arg == "--dry-run") {
            require_unset(has_dry_run, "--dry-run");
            has_dry_run = true;
            dry_run = true;
            continue;
        }

        if (arg == "--metadata-address") {
            require_unset(has_metadata_address, "--metadata-address");
            has_metadata_address = true;
            metadata_address = std::string{next_arg(argc, argv, index, "--metadata-address")};
            continue;
        }

        if (arg == "--metadata-port") {
            require_unset(has_metadata_port, "--metadata-port");
            has_metadata_port = true;
            metadata_port = parse_port(next_arg(argc, argv, index, "--metadata-port"), "--metadata-port");
            continue;
        }

        throw CliUsageError{std::string{"unknown option "} + std::string{arg}};
    }

    if (!has_storage_node_id) {
        throw CliUsageError{"--storage-node-id is required"};
    }

    validate_storage_node_id(storage_node_id);
    validate_numeric_ip(metadata_address, "--metadata-address");

    if (!gc_run_id.has_value()) {
        gc_run_id = aistore::metadata::UuidV7::generate();
    }

    return GcOptions{
        .gc_run_id = std::move(*gc_run_id),
        .storage_node_id = std::move(storage_node_id),
        .dry_run = dry_run,
        .metadata_address = std::move(metadata_address),
        .metadata_port = metadata_port,
    };
}

[[nodiscard]] bool creation_identity_matches(const aistore::metadata::UploadSession& requested,
                                             const aistore::metadata::UploadSession& existing) {
    if (existing.artifact_id() != requested.artifact_id() || existing.target_node_id() != requested.target_node_id() ||
        existing.replication_factor() != requested.replication_factor() ||
        existing.placement_node_ids() != requested.placement_node_ids() ||
        existing.chunking_strategy() != requested.chunking_strategy() ||
        existing.parent_version_id() != requested.parent_version_id() ||
        existing.immutable_metadata() != requested.immutable_metadata()) {
        return false;
    }

    if (requested.chunking_strategy() == aistore::metadata::ChunkingStrategy::FixedSize) {
        return existing.fixed_chunk_size_bytes() == requested.fixed_chunk_size_bytes();
    }

    return existing.fastcdc_parameters() == requested.fastcdc_parameters();
}

[[nodiscard]] std::vector<std::string> collect_active_node_ids(
    const std::vector<aistore::metadata::StorageNode>& nodes) {
    std::vector<std::string> active_node_ids;

    for (const aistore::metadata::StorageNode& node : nodes) {
        if (node.state == aistore::metadata::StorageNodeState::Active) {
            active_node_ids.push_back(node.node_id);
        }
    }

    std::ranges::sort(active_node_ids);
    return active_node_ids;
}

[[nodiscard]] aistore::metadata::UploadSession build_push_session(const PushOptions& options,
                                                                  std::vector<std::string> placement_node_ids) {
    if (placement_node_ids.empty()) {
        throw std::runtime_error("push requires at least one placement node");
    }

    if (options.chunking_strategy == aistore::metadata::ChunkingStrategy::FixedSize) {
        return aistore::metadata::UploadSession{
            options.session_id,
            options.artifact_id,
            options.replication_factor,
            std::move(placement_node_ids),
            aistore::metadata::ChunkingStrategy::FixedSize,
            options.chunk_size_bytes,
            options.parent_version_id,
            options.immutable_metadata,
            aistore::metadata::UploadSessionState::Open,
            std::nullopt,
        };
    }

    return aistore::metadata::UploadSession{
        options.session_id,
        options.artifact_id,
        options.replication_factor,
        std::move(placement_node_ids),
        aistore::metadata::FastCdcParameters{
            .min_chunk_size_bytes = options.min_chunk_size_bytes,
            .avg_chunk_size_bytes = options.avg_chunk_size_bytes,
            .max_chunk_size_bytes = options.max_chunk_size_bytes,
        },
        options.parent_version_id,
        options.immutable_metadata,
        aistore::metadata::UploadSessionState::Open,
        std::nullopt,
    };
}

[[nodiscard]] aistore::client::StorageNodeClientPool build_pool_for_nodes(
    const std::vector<aistore::metadata::StorageNode>& nodes, const std::vector<std::string>& node_ids) {
    std::vector<std::pair<std::string, aistore::client::StorageNodeClient>> clients;

    for (const std::string& node_id : node_ids) {
        const auto iterator = std::ranges::find_if(
            nodes, [&](const aistore::metadata::StorageNode& node) { return node.node_id == node_id; });

        if (iterator == nodes.end()) {
            throw std::runtime_error("required storage node is not registered: " + node_id);
        }

        if (iterator->state == aistore::metadata::StorageNodeState::Disabled) {
            throw std::runtime_error("required storage node is disabled: " + node_id);
        }

        clients.emplace_back(node_id, aistore::client::StorageNodeClient{aistore::http::HttpClientConfig{
                                          .endpoint =
                                              aistore::http::HttpEndpoint{
                                                  .address = iterator->address,
                                                  .port = iterator->port,
                                              },
                                      }});
    }

    return aistore::client::StorageNodeClientPool{std::move(clients)};
}

void emit_success_json(const std::string& target_node_id, const aistore::metadata::UuidV7& artifact_id,
                       const aistore::push::PreparedPush& prepared,
                       const aistore::metadata::FinalizeUploadResult& finalize_result) {
    if (finalize_result.session_id != prepared.session_id) {
        throw std::runtime_error("finalize result session_id does not match push session_id");
    }

    if (finalize_result.object_id != prepared.layout_descriptor.object_id()) {
        throw std::runtime_error("finalize result object_id does not match prepared layout object_id");
    }

    if (finalize_result.layout_id != prepared.layout_descriptor.layout_id()) {
        throw std::runtime_error("finalize result layout_id does not match prepared layout layout_id");
    }

    boost::json::object body;
    body["status"] = "committed";
    body["session_id"] = prepared.session_id.str();
    body["artifact_id"] = artifact_id.str();
    body["target_node_id"] = target_node_id;
    body["version_id"] = finalize_result.version_id;
    body["object_id"] = finalize_result.object_id;
    body["layout_id"] = finalize_result.layout_id;
    body["bytes_read"] = prepared.stats.bytes_read;
    body["total_chunks"] = prepared.stats.total_chunks;
    body["unique_chunks"] = prepared.stats.unique_chunks;
    body["put_requests"] = prepared.stats.put_requests;
    body["verified_target_chunks"] = prepared.stats.verified_target_chunks;
    body["repaired_target_chunks"] = prepared.stats.repaired_target_chunks;
    body["bytes_sent_to_storage"] = prepared.stats.bytes_sent_to_storage;

    std::cout << boost::json::serialize(body) << '\n';
}

void emit_pull_success_json(const aistore::pull::PullResult& result) {
    boost::json::object body;
    body["status"] = "restored";
    body["version_id"] = result.version_id;
    body["artifact_id"] = result.artifact_id.str();
    body["source_node_id"] = result.source_node_id;
    body["object_id"] = result.object_id;
    body["layout_id"] = result.layout_id;
    body["destination"] = result.destination_path.string();
    body["bytes_restored"] = result.stats.bytes_restored;
    body["total_chunks"] = result.stats.total_chunks;
    body["chunks_downloaded"] = result.stats.chunks_downloaded;
    body["chunks_reused_from_partial"] = result.stats.chunks_reused_from_partial;
    body["bytes_received_from_storage"] = result.stats.bytes_received_from_storage;

    std::cout << boost::json::serialize(body) << '\n';
}

void emit_gc_success_json(std::string_view storage_node_id, const aistore::metadata::GcRun& run) {
    const bool is_dry_run = run.mode == aistore::metadata::GcRunMode::DryRun;

    boost::json::object body;
    body["status"] = is_dry_run ? "gc_dry_run" : "gc_completed";
    body["gc_run_id"] = run.run_id.str();
    body["storage_node_id"] = storage_node_id;
    body["dry_run"] = is_dry_run;
    body["physical_chunks_scanned"] = run.physical_stats.physical_chunks_scanned;
    body["physical_bytes_scanned"] = run.physical_stats.physical_bytes_scanned;
    body["collectible_chunks"] = run.physical_stats.collectible_chunks;
    body["collectible_bytes"] = run.physical_stats.collectible_bytes;
    body["physically_deleted_chunks"] = run.physical_stats.physically_deleted_chunks;
    body["physically_deleted_bytes"] = run.physical_stats.physically_deleted_bytes;
    body["storage_locations_swept"] = run.metadata_stats.storage_locations_swept;
    body["chunk_rows_swept"] = run.metadata_stats.chunk_rows_swept;
    body["object_layouts_swept"] = run.metadata_stats.object_layouts_swept;
    body["objects_swept"] = run.metadata_stats.objects_swept;

    std::cout << boost::json::serialize(body) << '\n';
}

[[nodiscard]] int run_push(const PushOptions& options) {
    const aistore::metadata::UuidV7& session_id = options.session_id;
    const aistore::metadata::UuidV7& artifact_id = options.artifact_id;
    bool session_established = false;

    try {
        aistore::client::MetadataClient metadata_client{aistore::http::HttpClientConfig{
            .endpoint =
                aistore::http::HttpEndpoint{
                    .address = options.metadata_address,
                    .port = options.metadata_port,
                },
        }};

        const std::vector<aistore::metadata::StorageNode> registry_nodes = metadata_client.list_storage_nodes();
        std::vector<std::string> placement_node_ids;

        const std::optional<aistore::metadata::UploadSession> existing_session =
            metadata_client.get_upload_session(session_id);

        if (existing_session.has_value()) {
            placement_node_ids = existing_session->placement_node_ids();

            if (existing_session->replication_factor() != options.replication_factor) {
                throw std::runtime_error("existing upload session replication factor does not match request");
            }
        } else {
            placement_node_ids = collect_active_node_ids(registry_nodes);

            if (placement_node_ids.size() > 64U) {
                throw std::runtime_error("too many active storage nodes for upload session placement snapshot");
            }

            if (placement_node_ids.size() < options.replication_factor) {
                throw std::runtime_error("not enough active storage nodes for requested replication factor");
            }
        }

        aistore::client::StorageNodeClientPool storage_pool = build_pool_for_nodes(registry_nodes, placement_node_ids);
        aistore::push::PushEngine push_engine{metadata_client, storage_pool};

        const aistore::metadata::UploadSession session = build_push_session(options, placement_node_ids);

        std::optional<aistore::push::PreparedPush> prepared;

        try {
            (void)metadata_client.create_upload_session(session);
            session_established = true;

            prepared = push_engine.push(aistore::push::PushRequest{
                .source_path = options.file_path,
                .session_id = session_id,
            });
        } catch (const aistore::client::RemoteApiError& create_error) {
            if (create_error.status_code() != 409U || create_error.error_code() != "upload_session_conflict") {
                throw;
            }

            const std::optional<aistore::metadata::UploadSession> existing =
                metadata_client.get_upload_session(session_id);

            if (!existing.has_value() || existing->state() != aistore::metadata::UploadSessionState::Committed ||
                !creation_identity_matches(session, *existing)) {
                throw;
            }

            session_established = true;

            prepared = push_engine.prepare_committed_retry(
                aistore::push::PushRequest{
                    .source_path = options.file_path,
                    .session_id = session_id,
                },
                *existing);
        }

        const aistore::metadata::FinalizeUploadResult finalize_result =
            metadata_client.finalize_upload(prepared->session_id, prepared->layout_descriptor);

        emit_success_json(session.target_node_id(), artifact_id, *prepared, finalize_result);
        return 0;
    } catch (const aistore::client::RemoteApiError& error) {
        std::cerr << "aistore push error:\n"
                  << "HTTP status " << error.status_code() << '\n'
                  << "remote error_code " << error.error_code() << '\n';
        if (session_established) {
            std::cerr << "session_id=" << session_id.str() << '\n'
                      << "resume with --session-id " << session_id.str() << '\n';
        }
        return 1;
    } catch (const std::exception& error) {
        std::cerr << "aistore push error:\n" << error.what() << '\n';
        if (session_established) {
            std::cerr << "session_id=" << session_id.str() << '\n'
                      << "resume with --session-id " << session_id.str() << '\n';
        }
        return 1;
    }
}

[[nodiscard]] int run_pull(const PullOptions& options) {
    try {
        aistore::client::MetadataClient metadata_client{aistore::http::HttpClientConfig{
            .endpoint =
                aistore::http::HttpEndpoint{
                    .address = options.metadata_address,
                    .port = options.metadata_port,
                },
        }};

        const aistore::metadata::MultiNodeRestorePlan plan =
            metadata_client.get_multi_node_restore_plan(options.version_id);

        std::vector<std::string> node_ids;
        std::vector<aistore::metadata::StorageNode> endpoint_nodes;

        for (const aistore::metadata::RestoreChunkSources& chunk : plan.chunks) {
            for (const aistore::metadata::RestoreNodeEndpoint& source : chunk.sources) {
                if (std::ranges::find(node_ids, source.node_id) == node_ids.end()) {
                    node_ids.push_back(source.node_id);
                    endpoint_nodes.push_back(aistore::metadata::StorageNode{
                        .node_id = source.node_id,
                        .address = source.address,
                        .port = source.port,
                        .state = aistore::metadata::StorageNodeState::Active,
                    });
                }
            }
        }

        aistore::client::StorageNodeClientPool storage_pool =
            aistore::client::StorageNodeClientPool::from_registry_nodes(endpoint_nodes,
                                                                        aistore::http::HttpClientConfig{});
        aistore::pull::PullEngine pull_engine{metadata_client, storage_pool};

        const aistore::pull::PullResult result = pull_engine.pull(aistore::pull::PullRequest{
            .version_id = options.version_id,
            .destination_path = options.output_path,
            .overwrite = options.overwrite,
        });

        emit_pull_success_json(result);
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "aistore pull error: " << error.what() << '\n';
        return 1;
    }
}

[[nodiscard]] int run_gc(const GcOptions& options) {
    const aistore::metadata::UuidV7& gc_run_id = options.gc_run_id;

    try {
        aistore::client::MetadataClient metadata_client{aistore::http::HttpClientConfig{
            .endpoint =
                aistore::http::HttpEndpoint{
                    .address = options.metadata_address,
                    .port = options.metadata_port,
                },
        }};

        const std::optional<aistore::metadata::StorageNode> target_node =
            metadata_client.get_storage_node(options.storage_node_id);

        if (!target_node.has_value()) {
            throw std::runtime_error("storage node is not registered: " + options.storage_node_id);
        }

        aistore::client::StorageNodeClient storage_client{aistore::http::HttpClientConfig{
            .endpoint =
                aistore::http::HttpEndpoint{
                    .address = target_node->address,
                    .port = target_node->port,
                },
        }};

        aistore::gc::GcEngine gc_engine{metadata_client, storage_client, options.storage_node_id};

        const aistore::metadata::GcRun result = gc_engine.collect(aistore::gc::GcRequest{
            .run_id = gc_run_id,
            .dry_run = options.dry_run,
        });

        emit_gc_success_json(options.storage_node_id, result);
        return 0;
    } catch (const aistore::client::RemoteApiError& error) {
        std::cerr << "aistore gc error:\n"
                  << "HTTP status " << error.status_code() << '\n'
                  << "remote error_code " << error.error_code() << '\n';
        std::cerr << "gc_run_id=" << gc_run_id.str() << '\n' << "resume with --gc-run-id " << gc_run_id.str() << '\n';
        return 1;
    } catch (const std::exception& error) {
        std::cerr << "aistore gc error: " << error.what() << '\n';
        std::cerr << "gc_run_id=" << gc_run_id.str() << '\n' << "resume with --gc-run-id " << gc_run_id.str() << '\n';
        return 1;
    }
}

void print_node_usage(std::ostream& out) {
    out << "Usage:\n"
           "  aistore node register [options]\n"
           "  aistore node list [options]\n"
           "  aistore node set-state [options]\n";
}

void print_repair_usage(std::ostream& out) {
    out << "Usage:\n"
           "  aistore repair [options]\n"
           "\n"
           "Required:\n"
           "  --version-id <64-lowercase-hex>\n"
           "  --replication-factor <1..8>\n"
           "\n"
           "Optional:\n"
           "  --repair-run-id <uuidv7>\n"
           "  --metadata-address <numeric-ip>\n"
           "  --metadata-port <port>\n";
}

[[nodiscard]] int run_node(int argc, char** argv) {
    if (argc <= 2) {
        throw CliUsageError{"node subcommand is required"};
    }

    const std::string_view subcommand{argv[2]};
    if (subcommand == "--help" || subcommand == "help") {
        print_node_usage(std::cout);
        return 0;
    }

    std::string metadata_address = "127.0.0.1";
    std::uint16_t metadata_port = 8080;

    aistore::client::MetadataClient metadata_client{aistore::http::HttpClientConfig{
        .endpoint =
            aistore::http::HttpEndpoint{
                .address = metadata_address,
                .port = metadata_port,
            },
    }};

    if (subcommand == "list") {
        for (int index = 3; index < argc; ++index) {
            const std::string_view arg{argv[index]};
            if (arg == "--help") {
                print_node_usage(std::cout);
                return 0;
            }
            if (arg == "--metadata-address") {
                metadata_address = std::string{next_arg(argc, argv, index, "--metadata-address")};
                continue;
            }
            if (arg == "--metadata-port") {
                metadata_port = parse_port(next_arg(argc, argv, index, "--metadata-port"), "--metadata-port");
                continue;
            }
            throw CliUsageError{std::string{"unknown option "} + std::string{arg}};
        }

        validate_numeric_ip(metadata_address, "--metadata-address");
        metadata_client = aistore::client::MetadataClient{aistore::http::HttpClientConfig{
            .endpoint = {.address = metadata_address, .port = metadata_port},
        }};

        const std::vector<aistore::metadata::StorageNode> nodes = metadata_client.list_storage_nodes();
        boost::json::array node_array;
        for (const aistore::metadata::StorageNode& node : nodes) {
            node_array.push_back(boost::json::object{
                {"node_id", node.node_id},
                {"address", node.address},
                {"port", node.port},
                {"state", aistore::metadata::storage_node_state_to_string(node.state)},
            });
        }
        std::cout << boost::json::serialize(boost::json::object{{"nodes", std::move(node_array)}}) << '\n';
        return 0;
    }

    if (subcommand == "register") {
        std::string node_id;
        std::string storage_address;
        std::uint16_t storage_port = 8081;
        aistore::metadata::StorageNodeState state = aistore::metadata::StorageNodeState::Active;

        for (int index = 3; index < argc; ++index) {
            const std::string_view arg{argv[index]};
            if (arg == "--help") {
                print_node_usage(std::cout);
                return 0;
            }
            if (arg == "--storage-node-id") {
                node_id = std::string{next_arg(argc, argv, index, "--storage-node-id")};
                continue;
            }
            if (arg == "--storage-address") {
                storage_address = std::string{next_arg(argc, argv, index, "--storage-address")};
                continue;
            }
            if (arg == "--storage-port") {
                storage_port = parse_port(next_arg(argc, argv, index, "--storage-port"), "--storage-port");
                continue;
            }
            if (arg == "--state") {
                const std::string state_text{next_arg(argc, argv, index, "--state")};
                state = aistore::metadata::storage_node_state_from_string(state_text);
                continue;
            }
            if (arg == "--metadata-address") {
                metadata_address = std::string{next_arg(argc, argv, index, "--metadata-address")};
                continue;
            }
            if (arg == "--metadata-port") {
                metadata_port = parse_port(next_arg(argc, argv, index, "--metadata-port"), "--metadata-port");
                continue;
            }
            throw CliUsageError{std::string{"unknown option "} + std::string{arg}};
        }

        if (node_id.empty() || storage_address.empty()) {
            throw CliUsageError{"--storage-node-id and --storage-address are required"};
        }

        validate_storage_node_id(node_id);
        validate_numeric_ip(storage_address, "--storage-address");
        validate_numeric_ip(metadata_address, "--metadata-address");

        aistore::client::StorageNodeClient probe_client{aistore::http::HttpClientConfig{
            .endpoint = {.address = storage_address, .port = storage_port},
        }};

        if (probe_client.probe_node_id() != node_id) {
            throw std::runtime_error("storage node probe identity mismatch");
        }

        metadata_client = aistore::client::MetadataClient{aistore::http::HttpClientConfig{
            .endpoint = {.address = metadata_address, .port = metadata_port},
        }};
        metadata_client.register_storage_node(aistore::metadata::StorageNode{
            .node_id = node_id,
            .address = storage_address,
            .port = storage_port,
            .state = state,
        });
        return 0;
    }

    if (subcommand == "set-state") {
        std::string node_id;
        aistore::metadata::StorageNodeState state = aistore::metadata::StorageNodeState::Active;

        for (int index = 3; index < argc; ++index) {
            const std::string_view arg{argv[index]};
            if (arg == "--help") {
                print_node_usage(std::cout);
                return 0;
            }
            if (arg == "--storage-node-id") {
                node_id = std::string{next_arg(argc, argv, index, "--storage-node-id")};
                continue;
            }
            if (arg == "--state") {
                state = aistore::metadata::storage_node_state_from_string(
                    std::string{next_arg(argc, argv, index, "--state")});
                continue;
            }
            if (arg == "--metadata-address") {
                metadata_address = std::string{next_arg(argc, argv, index, "--metadata-address")};
                continue;
            }
            if (arg == "--metadata-port") {
                metadata_port = parse_port(next_arg(argc, argv, index, "--metadata-port"), "--metadata-port");
                continue;
            }
            throw CliUsageError{std::string{"unknown option "} + std::string{arg}};
        }

        if (node_id.empty()) {
            throw CliUsageError{"--storage-node-id and --state are required"};
        }

        validate_storage_node_id(node_id);
        validate_numeric_ip(metadata_address, "--metadata-address");
        metadata_client = aistore::client::MetadataClient{aistore::http::HttpClientConfig{
            .endpoint = {.address = metadata_address, .port = metadata_port},
        }};

        const std::optional<aistore::metadata::StorageNode> existing = metadata_client.get_storage_node(node_id);
        if (!existing.has_value()) {
            throw std::runtime_error("storage node is not registered");
        }

        metadata_client.register_storage_node(aistore::metadata::StorageNode{
            .node_id = existing->node_id,
            .address = existing->address,
            .port = existing->port,
            .state = state,
        });
        return 0;
    }

    throw CliUsageError{std::string{"unknown node subcommand "} + std::string{subcommand}};
}

[[nodiscard]] int run_repair(int argc, char** argv) {
    std::string version_id;
    std::uint8_t replication_factor = 1;
    std::optional<aistore::metadata::UuidV7> repair_run_id;
    std::string metadata_address = "127.0.0.1";
    std::uint16_t metadata_port = 8080;

    for (int index = 2; index < argc; ++index) {
        const std::string_view arg{argv[index]};
        if (arg == "--help") {
            print_repair_usage(std::cout);
            return 0;
        }
        if (arg == "--version-id") {
            version_id = std::string{next_arg(argc, argv, index, "--version-id")};
            continue;
        }
        if (arg == "--replication-factor") {
            const std::uint64_t value =
                parse_strict_u64(next_arg(argc, argv, index, "--replication-factor"), "--replication-factor");
            if (value < 1ULL || value > 8ULL) {
                throw CliUsageError{"--replication-factor must be in range 1..8"};
            }
            replication_factor = static_cast<std::uint8_t>(value);
            continue;
        }
        if (arg == "--repair-run-id") {
            repair_run_id.emplace(std::string{next_arg(argc, argv, index, "--repair-run-id")});
            continue;
        }
        if (arg == "--metadata-address") {
            metadata_address = std::string{next_arg(argc, argv, index, "--metadata-address")};
            continue;
        }
        if (arg == "--metadata-port") {
            metadata_port = parse_port(next_arg(argc, argv, index, "--metadata-port"), "--metadata-port");
            continue;
        }
        throw CliUsageError{std::string{"unknown option "} + std::string{arg}};
    }

    if (version_id.empty()) {
        throw CliUsageError{"--version-id is required"};
    }

    validate_version_id(version_id);
    validate_numeric_ip(metadata_address, "--metadata-address");

    if (!repair_run_id.has_value()) {
        repair_run_id = aistore::metadata::UuidV7::generate();
    }

    try {
        aistore::client::MetadataClient metadata_client{aistore::http::HttpClientConfig{
            .endpoint = {.address = metadata_address, .port = metadata_port},
        }};

        const aistore::metadata::ReplicationRun started =
            metadata_client.start_replication_run(*repair_run_id, version_id, replication_factor);

        if (started.state == aistore::metadata::ReplicationRunState::Completed) {
            boost::json::object body;
            body["status"] = "replication_repaired";
            body["repair_run_id"] = started.run_id.str();
            body["version_id"] = started.version_id;
            body["layout_id"] = started.layout_id;
            body["replication_factor"] = started.replication_factor;
            body["chunks_scanned"] = started.stats.chunks_scanned;
            body["chunks_under_replicated"] = started.stats.chunks_under_replicated;
            body["replicas_verified"] = started.stats.replicas_verified;
            body["replicas_written"] = started.stats.replicas_written;
            body["bytes_copied"] = started.stats.bytes_copied;
            body["source_failovers"] = started.stats.source_failovers;
            std::cout << boost::json::serialize(body) << '\n';
            return 0;
        }

        const aistore::metadata::ReplicationPlan plan = metadata_client.get_replication_plan(*repair_run_id);
        std::vector<aistore::metadata::StorageNode> endpoint_nodes;

        for (const aistore::metadata::ReplicationChunkPlan& chunk : plan.chunks) {
            for (const aistore::metadata::ReplicationNodeEndpoint& endpoint : chunk.source_nodes) {
                if (std::ranges::none_of(endpoint_nodes, [&](const aistore::metadata::StorageNode& node) {
                        return node.node_id == endpoint.node_id;
                    })) {
                    endpoint_nodes.push_back(aistore::metadata::StorageNode{
                        .node_id = endpoint.node_id,
                        .address = endpoint.address,
                        .port = endpoint.port,
                    });
                }
            }
            for (const aistore::metadata::ReplicationNodeEndpoint& endpoint : chunk.target_nodes) {
                if (std::ranges::none_of(endpoint_nodes, [&](const aistore::metadata::StorageNode& node) {
                        return node.node_id == endpoint.node_id;
                    })) {
                    endpoint_nodes.push_back(aistore::metadata::StorageNode{
                        .node_id = endpoint.node_id,
                        .address = endpoint.address,
                        .port = endpoint.port,
                    });
                }
            }
        }

        aistore::client::StorageNodeClientPool storage_pool =
            aistore::client::StorageNodeClientPool::from_registry_nodes(endpoint_nodes,
                                                                        aistore::http::HttpClientConfig{});

        aistore::replication::RepairEngine repair_engine{metadata_client, storage_pool};
        const aistore::replication::RepairResult result = repair_engine.repair(aistore::replication::RepairRequest{
            .run_id = *repair_run_id,
            .version_id = version_id,
            .replication_factor = replication_factor,
        });

        boost::json::object body;
        body["status"] = "replication_repaired";
        body["repair_run_id"] = result.run_id.str();
        body["version_id"] = result.version_id;
        body["layout_id"] = result.layout_id;
        body["replication_factor"] = result.replication_factor;
        body["chunks_scanned"] = result.stats.chunks_scanned;
        body["chunks_under_replicated"] = result.stats.chunks_under_replicated;
        body["replicas_verified"] = result.stats.replicas_verified;
        body["replicas_written"] = result.stats.replicas_written;
        body["bytes_copied"] = result.stats.bytes_copied;
        body["source_failovers"] = result.stats.source_failovers;
        std::cout << boost::json::serialize(body) << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "aistore repair error: " << error.what() << '\n'
                  << "resume with --repair-run-id " << repair_run_id->str() << '\n';
        return 1;
    }
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc <= 1) {
            std::cerr << "aistore: missing command\n";
            print_top_level_usage(std::cerr);
            return 2;
        }

        const std::string_view command{argv[1]};

        if (command == "--help" || command == "help") {
            print_top_level_usage(std::cout);
            return 0;
        }

        if (command == "push") {
            try {
                const PushOptions options = parse_push_options(argc, argv);
                return run_push(options);
            } catch (const CliUsageError& error) {
                std::cerr << "aistore: " << error.what() << '\n';
                print_push_usage(std::cerr);
                return 2;
            }
        }

        if (command == "pull") {
            try {
                const PullOptions options = parse_pull_options(argc, argv);
                return run_pull(options);
            } catch (const CliUsageError& error) {
                std::cerr << "aistore: " << error.what() << '\n';
                print_pull_usage(std::cerr);
                return 2;
            }
        }

        if (command == "gc") {
            try {
                const GcOptions options = parse_gc_options(argc, argv);
                return run_gc(options);
            } catch (const CliUsageError& error) {
                std::cerr << "aistore: " << error.what() << '\n';
                print_gc_usage(std::cerr);
                return 2;
            }
        }

        if (command == "node") {
            try {
                return run_node(argc, argv);
            } catch (const CliUsageError& error) {
                std::cerr << "aistore: " << error.what() << '\n';
                print_node_usage(std::cerr);
                return 2;
            }
        }

        if (command == "repair") {
            try {
                return run_repair(argc, argv);
            } catch (const CliUsageError& error) {
                std::cerr << "aistore: " << error.what() << '\n';
                print_repair_usage(std::cerr);
                return 2;
            }
        }

        throw CliUsageError{std::string{"unknown command "} + std::string{command}};
    } catch (const CliUsageError& error) {
        std::cerr << "aistore: " << error.what() << '\n';
        print_top_level_usage(std::cerr);
        return 2;
    } catch (const std::exception& error) {
        std::cerr << "aistore push error:\n" << error.what() << '\n';
        return 1;
    }
}
