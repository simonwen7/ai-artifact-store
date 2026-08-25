#include "aistore/metadata/postgres_metadata_repository.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <pqxx/pqxx>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace aistore::metadata {

namespace {

long long to_postgres_bigint(std::uint64_t value, std::string_view field_name) {
    const auto maximum = static_cast<std::uint64_t>(std::numeric_limits<long long>::max());

    if (value > maximum) {
        throw std::overflow_error(std::string{field_name} + " exceeds PostgreSQL BIGINT range");
    }

    return static_cast<long long>(value);
}

long long size_to_postgres_bigint(std::size_t value, std::string_view field_name) {
    const auto maximum = static_cast<std::size_t>(std::numeric_limits<long long>::max());

    if (value > maximum) {
        throw std::overflow_error(std::string{field_name} + " exceeds PostgreSQL BIGINT range");
    }

    return static_cast<long long>(value);
}

std::string bytes_to_hex(std::span<const std::byte> bytes) {
    constexpr std::array<char, 16> kHexDigits{
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    };

    std::string result;
    result.reserve(bytes.size() * 2);

    for (const std::byte byte : bytes) {
        const auto value = std::to_integer<unsigned int>(byte);

        result.push_back(kHexDigits[(value >> 4U) & 0x0FU]);

        result.push_back(kHexDigits[value & 0x0FU]);
    }

    return result;
}

void validate_object_id(std::string_view object_id) {
    if (object_id.size() != 64) {
        throw std::invalid_argument("object ID must contain exactly 64 hexadecimal characters");
    }

    for (const char character : object_id) {
        const bool is_digit = character >= '0' && character <= '9';

        const bool is_lower_hex = character >= 'a' && character <= 'f';

        if (!is_digit && !is_lower_hex) {
            throw std::invalid_argument("object ID must use lowercase hexadecimal characters");
        }
    }
}

std::string_view version_state_to_string(VersionState state) {
    switch (state) {
        case VersionState::Staging:
            return "staging";

        case VersionState::Committed:
            return "committed";

        case VersionState::Failed:
            return "failed";
    }

    throw std::logic_error("unsupported artifact version state");
}

VersionState version_state_from_string(std::string_view state) {
    if (state == "staging") {
        return VersionState::Staging;
    }

    if (state == "committed") {
        return VersionState::Committed;
    }

    if (state == "failed") {
        return VersionState::Failed;
    }

    throw std::runtime_error("database contains an unsupported artifact version state");
}

}  // namespace

class PostgresMetadataRepository::Impl {
   public:
    explicit Impl(const std::string& connection_string) : connection_(connection_string) {}

    void create_artifact(const Artifact& artifact) {
        pqxx::work transaction{connection_};

        transaction
            .exec(
                "INSERT INTO artifacts ("
                "    artifact_id, "
                "    project, "
                "    name"
                ") "
                "VALUES ("
                "    $1::uuid, "
                "    $2, "
                "    $3"
                ")",
                pqxx::params{
                    artifact.artifact_id().str(),
                    artifact.project(),
                    artifact.name(),
                })
            .no_rows();

        transaction.commit();
    }

    [[nodiscard]] std::optional<Artifact> get_artifact(const UuidV7& artifact_id) {
        pqxx::work transaction{connection_};

        auto stored = transaction.query01<std::string, std::string, std::string>(
            "SELECT "
            "    artifact_id::text, "
            "    name, "
            "    project "
            "FROM artifacts "
            "WHERE artifact_id = $1::uuid",
            pqxx::params{
                artifact_id.str(),
            });

        if (!stored.has_value()) {
            transaction.commit();
            return std::nullopt;
        }

        auto [stored_artifact_id, name, project] = std::move(*stored);

        transaction.commit();

        return Artifact{
            UuidV7{
                std::move(stored_artifact_id),
            },
            std::move(name),
            std::move(project),
        };
    }

    void register_object(const ObjectDescriptor& descriptor) {
        pqxx::work transaction{connection_};

        const ObjectLayout& layout = descriptor.layout();

        for (const ChunkRef& chunk : layout.chunks()) {
            const long long chunk_size = to_postgres_bigint(chunk.size, "chunk size");

            transaction
                .exec(
                    "INSERT INTO chunks ("
                    "    chunk_id, "
                    "    size_bytes"
                    ") "
                    "VALUES ("
                    "    $1, "
                    "    $2"
                    ") "
                    "ON CONFLICT (chunk_id) DO NOTHING",
                    pqxx::params{
                        chunk.chunk_id,
                        chunk_size,
                    })
                .no_rows();

            const auto stored_size = transaction.query_value<long long>(
                "SELECT size_bytes "
                "FROM chunks "
                "WHERE chunk_id = $1",
                pqxx::params{
                    chunk.chunk_id,
                });

            if (stored_size != chunk_size) {
                throw std::runtime_error("existing chunk size does not match object layout");
            }
        }

        const long long total_size = to_postgres_bigint(layout.total_size(), "object total size");

        const long long chunk_count = size_to_postgres_bigint(layout.chunks().size(), "object chunk count");

        const pqxx::result object_insert = transaction.exec(
            "INSERT INTO objects ("
            "    object_id, "
            "    descriptor_version, "
            "    canonical_descriptor, "
            "    total_size_bytes, "
            "    chunk_count"
            ") "
            "VALUES ("
            "    $1, "
            "    $2, "
            "    $3::bytea, "
            "    $4, "
            "    $5"
            ") "
            "ON CONFLICT (object_id) DO NOTHING",
            pqxx::params{
                descriptor.object_id(),
                static_cast<int>(ObjectDescriptor::kFormatVersion),
                descriptor.canonical_bytes(),
                total_size,
                chunk_count,
            });

        const bool object_was_inserted = object_insert.affected_rows() == 1U;

        if (object_was_inserted) {
            for (std::size_t index = 0; index < layout.chunks().size(); ++index) {
                const ChunkRef& chunk = layout.chunks()[index];

                transaction
                    .exec(
                        "INSERT INTO object_chunks ("
                        "    object_id, "
                        "    chunk_index, "
                        "    chunk_id, "
                        "    byte_offset, "
                        "    chunk_size_bytes"
                        ") "
                        "VALUES ("
                        "    $1, "
                        "    $2, "
                        "    $3, "
                        "    $4, "
                        "    $5"
                        ")",
                        pqxx::params{
                            descriptor.object_id(),
                            size_to_postgres_bigint(index, "chunk index"),
                            chunk.chunk_id,
                            to_postgres_bigint(chunk.offset, "chunk byte offset"),
                            to_postgres_bigint(chunk.size, "chunk size"),
                        })
                    .no_rows();
            }
        }

        verify_registered_object(transaction, descriptor);

        transaction.commit();
    }

    [[nodiscard]] std::optional<ObjectDescriptor> get_object(std::string_view object_id) {
        validate_object_id(object_id);

        pqxx::work transaction{connection_};

        auto stored_object = transaction.query01<int, std::string, long long, long long>(
            "SELECT "
            "    descriptor_version, "
            "    encode(canonical_descriptor, 'hex'), "
            "    total_size_bytes, "
            "    chunk_count "
            "FROM objects "
            "WHERE object_id = $1",
            pqxx::params{
                object_id,
            });

        if (!stored_object.has_value()) {
            transaction.commit();
            return std::nullopt;
        }

        auto [descriptor_version, stored_descriptor_hex, stored_total_size, stored_chunk_count] =
            std::move(*stored_object);

        if (std::cmp_not_equal(descriptor_version, ObjectDescriptor::kFormatVersion)) {
            throw std::runtime_error("stored object uses an unsupported descriptor version");
        }

        if (stored_total_size < 0 || stored_chunk_count < 0) {
            throw std::runtime_error("stored object contains invalid negative metadata");
        }

        std::vector<ChunkRef> chunks;
        chunks.reserve(static_cast<std::size_t>(stored_chunk_count));

        std::size_t expected_index = 0;

        for (auto [stored_index, chunk_id, byte_offset, chunk_size] :
             transaction.query<long long, std::string, long long, long long>("SELECT "
                                                                             "    chunk_index, "
                                                                             "    chunk_id, "
                                                                             "    byte_offset, "
                                                                             "    chunk_size_bytes "
                                                                             "FROM object_chunks "
                                                                             "WHERE object_id = $1 "
                                                                             "ORDER BY chunk_index",
                                                                             pqxx::params{
                                                                                 object_id,
                                                                             })) {
            if (stored_index != size_to_postgres_bigint(expected_index, "chunk index")) {
                throw std::runtime_error("stored object chunk indexes are not contiguous");
            }

            if (byte_offset < 0 || chunk_size <= 0) {
                throw std::runtime_error("stored object chunk contains invalid size metadata");
            }

            chunks.push_back(ChunkRef{
                .chunk_id = std::move(chunk_id),
                .offset = static_cast<std::uint64_t>(byte_offset),
                .size = static_cast<std::uint64_t>(chunk_size),
            });

            ++expected_index;
        }

        if (std::cmp_not_equal(expected_index, stored_chunk_count)) {
            throw std::runtime_error("stored object chunk count does not match layout rows");
        }

        ObjectDescriptor descriptor{
            ObjectLayout{
                std::move(chunks),
            },
        };

        if (descriptor.object_id() != object_id) {
            throw std::runtime_error("stored object layout does not match object ID");
        }

        if (descriptor.layout().total_size() != static_cast<std::uint64_t>(stored_total_size)) {
            throw std::runtime_error("stored object total size does not match layout");
        }

        const std::string reconstructed_descriptor_hex = bytes_to_hex(std::span<const std::byte>{
            descriptor.canonical_bytes(),
        });

        if (reconstructed_descriptor_hex != stored_descriptor_hex) {
            throw std::runtime_error("stored canonical descriptor does not match object layout");
        }

        transaction.commit();

        return descriptor;
    }

    void create_version(const ArtifactVersion& version) {
        pqxx::work transaction{connection_};

        transaction
            .exec(
                "INSERT INTO artifact_versions ("
                "    version_id, "
                "    artifact_id, "
                "    root_object_id, "
                "    state"
                ") "
                "VALUES ("
                "    $1::uuid, "
                "    $2::uuid, "
                "    $3, "
                "    $4"
                ")",
                pqxx::params{
                    version.version_id().str(),
                    version.artifact_id().str(),
                    version.root_object_id(),
                    version_state_to_string(version.state()),
                })
            .no_rows();

        transaction.commit();
    }

    [[nodiscard]] std::optional<ArtifactVersion> get_version(const UuidV7& version_id) {
        pqxx::work transaction{connection_};

        auto stored = transaction.query01<std::string, std::string, std::string, std::string>(
            "SELECT "
            "    version_id::text, "
            "    artifact_id::text, "
            "    root_object_id, "
            "    state "
            "FROM artifact_versions "
            "WHERE version_id = $1::uuid",
            pqxx::params{
                version_id.str(),
            });

        if (!stored.has_value()) {
            transaction.commit();
            return std::nullopt;
        }

        auto [stored_version_id, stored_artifact_id, root_object_id, state] = std::move(*stored);

        const VersionState version_state = version_state_from_string(state);

        transaction.commit();

        return ArtifactVersion{
            UuidV7{
                std::move(stored_version_id),
            },
            UuidV7{
                std::move(stored_artifact_id),
            },
            std::move(root_object_id),
            version_state,
        };
    }

   private:
    void verify_registered_object(pqxx::work& transaction, const ObjectDescriptor& descriptor) {
        const ObjectLayout& layout = descriptor.layout();

        auto stored_object = transaction.query01<int, std::string, long long, long long>(
            "SELECT "
            "    descriptor_version, "
            "    encode(canonical_descriptor, 'hex'), "
            "    total_size_bytes, "
            "    chunk_count "
            "FROM objects "
            "WHERE object_id = $1",
            pqxx::params{
                descriptor.object_id(),
            });

        if (!stored_object.has_value()) {
            throw std::runtime_error("registered object is missing from database");
        }

        auto [descriptor_version, stored_descriptor_hex, stored_total_size, stored_chunk_count] =
            std::move(*stored_object);

        if (std::cmp_not_equal(descriptor_version, ObjectDescriptor::kFormatVersion) ||
            stored_descriptor_hex != bytes_to_hex(std::span<const std::byte>{
                                         descriptor.canonical_bytes(),
                                     }) ||
            stored_total_size != to_postgres_bigint(layout.total_size(), "object total size") ||
            stored_chunk_count != size_to_postgres_bigint(layout.chunks().size(), "object chunk count")) {
            throw std::runtime_error("existing object metadata does not match descriptor");
        }

        std::size_t expected_index = 0;

        for (const auto& [stored_index, stored_chunk_id, stored_offset, stored_size] :
             transaction.query<long long, std::string, long long, long long>("SELECT "
                                                                             "    chunk_index, "
                                                                             "    chunk_id, "
                                                                             "    byte_offset, "
                                                                             "    chunk_size_bytes "
                                                                             "FROM object_chunks "
                                                                             "WHERE object_id = $1 "
                                                                             "ORDER BY chunk_index",
                                                                             pqxx::params{
                                                                                 descriptor.object_id(),
                                                                             })) {
            if (expected_index >= layout.chunks().size()) {
                throw std::runtime_error("stored object contains unexpected chunk rows");
            }

            const ChunkRef& expected = layout.chunks()[expected_index];

            if (stored_index != size_to_postgres_bigint(expected_index, "chunk index") ||
                stored_chunk_id != expected.chunk_id ||
                stored_offset != to_postgres_bigint(expected.offset, "chunk byte offset") ||
                stored_size != to_postgres_bigint(expected.size, "chunk size")) {
                throw std::runtime_error("stored object layout does not match descriptor");
            }

            ++expected_index;
        }

        if (expected_index != layout.chunks().size()) {
            throw std::runtime_error("stored object is missing chunk rows");
        }
    }

    pqxx::connection connection_;
};

PostgresMetadataRepository::PostgresMetadataRepository(const std::string& connection_string)
    : impl_(std::make_unique<Impl>(connection_string)) {}

PostgresMetadataRepository::~PostgresMetadataRepository() = default;

PostgresMetadataRepository::PostgresMetadataRepository(PostgresMetadataRepository&&) noexcept = default;

PostgresMetadataRepository& PostgresMetadataRepository::operator=(PostgresMetadataRepository&&) noexcept = default;

void PostgresMetadataRepository::create_artifact(const Artifact& artifact) { impl_->create_artifact(artifact); }

std::optional<Artifact> PostgresMetadataRepository::get_artifact(const UuidV7& artifact_id) {
    return impl_->get_artifact(artifact_id);
}

void PostgresMetadataRepository::register_object(const ObjectDescriptor& descriptor) {
    impl_->register_object(descriptor);
}

std::optional<ObjectDescriptor> PostgresMetadataRepository::get_object(std::string_view object_id) {
    return impl_->get_object(object_id);
}

void PostgresMetadataRepository::create_version(const ArtifactVersion& version) { impl_->create_version(version); }

std::optional<ArtifactVersion> PostgresMetadataRepository::get_version(const UuidV7& version_id) {
    return impl_->get_version(version_id);
}

}  // namespace aistore::metadata
