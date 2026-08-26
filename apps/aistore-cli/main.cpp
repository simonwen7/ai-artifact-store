#include <boost/asio/ip/address.hpp>
#include <boost/json.hpp>
#include <boost/system/error_code.hpp>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "aistore/client/client_error.hpp"
#include "aistore/client/metadata_client.hpp"
#include "aistore/client/storage_node_client.hpp"
#include "aistore/http/http_client.hpp"
#include "aistore/metadata/finalize_upload.hpp"
#include "aistore/metadata/upload_session.hpp"
#include "aistore/metadata/uuid_v7.hpp"
#include "aistore/push/push_engine.hpp"

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
           "\n"
           "Commands:\n"
           "  push\n"
           "\n"
           "Run `aistore push --help` for push options.\n";
}

void print_push_usage(std::ostream& out) {
    out << "Usage:\n"
           "  aistore push [options]\n"
           "\n"
           "Required:\n"
           "  --file <path>\n"
           "  --artifact-id <uuidv7>\n"
           "  --storage-node-id <node-id>\n"
           "\n"
           "Optional:\n"
           "  --session-id <uuidv7>\n"
           "  --chunk-size <bytes>\n"
           "  --parent-version <64-lowercase-hex>\n"
           "  --metadata <KEY=VALUE>\n"
           "  --metadata-address <numeric-ip>\n"
           "  --metadata-port <port>\n"
           "  --storage-address <numeric-ip>\n"
           "  --storage-port <port>\n"
           "\n"
           "Defaults:\n"
           "  metadata 127.0.0.1:8080\n"
           "  storage  127.0.0.1:8081\n"
           "  chunk-size 4194304\n";
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
    std::string storage_node_id;
    aistore::metadata::UuidV7 session_id;
    std::uint64_t chunk_size_bytes = 4194304ULL;
    std::optional<std::string> parent_version_id;
    aistore::metadata::UploadSession::ImmutableMetadata immutable_metadata;
    std::string metadata_address = "127.0.0.1";
    std::uint16_t metadata_port = 8080;
    std::string storage_address = "127.0.0.1";
    std::uint16_t storage_port = 8081;
};

[[nodiscard]] PushOptions parse_push_options(int argc, char** argv) {
    std::optional<aistore::metadata::UuidV7> artifact_id;
    std::optional<aistore::metadata::UuidV7> session_id;
    std::filesystem::path file_path;
    std::string storage_node_id;
    std::uint64_t chunk_size_bytes = 4194304ULL;
    std::optional<std::string> parent_version_id;
    aistore::metadata::UploadSession::ImmutableMetadata immutable_metadata;
    std::string metadata_address = "127.0.0.1";
    std::uint16_t metadata_port = 8080;
    std::string storage_address = "127.0.0.1";
    std::uint16_t storage_port = 8081;

    bool has_file = false;
    bool has_artifact_id = false;
    bool has_storage_node_id = false;
    bool has_session_id = false;
    bool has_chunk_size = false;
    bool has_parent_version = false;
    bool has_metadata_address = false;
    bool has_metadata_port = false;
    bool has_storage_address = false;
    bool has_storage_port = false;

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

        if (arg == "--storage-node-id") {
            require_unset(has_storage_node_id, "--storage-node-id");
            has_storage_node_id = true;
            storage_node_id = std::string{next_arg(argc, argv, index, "--storage-node-id")};
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

        if (arg == "--chunk-size") {
            require_unset(has_chunk_size, "--chunk-size");
            has_chunk_size = true;
            chunk_size_bytes = parse_strict_u64(next_arg(argc, argv, index, "--chunk-size"), "--chunk-size");
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

        if (arg == "--storage-address") {
            require_unset(has_storage_address, "--storage-address");
            has_storage_address = true;
            storage_address = std::string{next_arg(argc, argv, index, "--storage-address")};
            continue;
        }

        if (arg == "--storage-port") {
            require_unset(has_storage_port, "--storage-port");
            has_storage_port = true;
            storage_port = parse_port(next_arg(argc, argv, index, "--storage-port"), "--storage-port");
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
    if (!has_storage_node_id) {
        throw CliUsageError{"--storage-node-id is required"};
    }

    if (file_path.empty()) {
        throw CliUsageError{"--file must be nonempty"};
    }
    if (!is_regular_file_path(file_path)) {
        throw CliUsageError{"--file must exist and resolve to a regular file"};
    }

    validate_storage_node_id(storage_node_id);

    if (chunk_size_bytes == 0ULL) {
        throw CliUsageError{"--chunk-size must be greater than 0"};
    }
    if (chunk_size_bytes > aistore::push::PushEngine::kMaxM4ChunkSize) {
        throw CliUsageError{"--chunk-size exceeds maximum supported chunk size"};
    }

    validate_numeric_ip(metadata_address, "--metadata-address");
    validate_numeric_ip(storage_address, "--storage-address");

    if (!session_id.has_value()) {
        session_id = aistore::metadata::UuidV7::generate();
    }

    return PushOptions{
        .file_path = std::move(file_path),
        .artifact_id = std::move(*artifact_id),
        .storage_node_id = std::move(storage_node_id),
        .session_id = std::move(*session_id),
        .chunk_size_bytes = chunk_size_bytes,
        .parent_version_id = std::move(parent_version_id),
        .immutable_metadata = std::move(immutable_metadata),
        .metadata_address = std::move(metadata_address),
        .metadata_port = metadata_port,
        .storage_address = std::move(storage_address),
        .storage_port = storage_port,
    };
}

[[nodiscard]] bool creation_identity_matches(const aistore::metadata::UploadSession& requested,
                                             const aistore::metadata::UploadSession& existing) {
    return existing.artifact_id() == requested.artifact_id() &&
           existing.target_node_id() == requested.target_node_id() &&
           existing.chunking_strategy() == requested.chunking_strategy() &&
           existing.chunk_size_bytes() == requested.chunk_size_bytes() &&
           existing.parent_version_id() == requested.parent_version_id() &&
           existing.immutable_metadata() == requested.immutable_metadata();
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

        aistore::client::StorageNodeClient storage_client{aistore::http::HttpClientConfig{
            .endpoint =
                aistore::http::HttpEndpoint{
                    .address = options.storage_address,
                    .port = options.storage_port,
                },
        }};

        aistore::push::PushEngine push_engine{metadata_client, storage_client, options.storage_node_id};

        const aistore::metadata::UploadSession session{
            session_id,
            artifact_id,
            options.storage_node_id,
            aistore::metadata::ChunkingStrategy::FixedSize,
            options.chunk_size_bytes,
            options.parent_version_id,
            options.immutable_metadata,
            aistore::metadata::UploadSessionState::Open,
            std::nullopt,
        };

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

        emit_success_json(options.storage_node_id, artifact_id, *prepared, finalize_result);
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
