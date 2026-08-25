#include "aistore/metadata/artifact_model.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "aistore/hashing/sha256.hpp"

namespace aistore::metadata {

namespace {

using aistore::hashing::Sha256;

constexpr std::string_view kVersionMagic = "AISTORE_VERSION";

void append_uint32_big_endian(std::vector<std::byte>& output, std::uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) {
        output.push_back(static_cast<std::byte>((value >> static_cast<unsigned int>(shift)) & 0xFFU));
    }
}

void append_uint64_big_endian(std::vector<std::byte>& output, std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        output.push_back(static_cast<std::byte>((value >> static_cast<unsigned int>(shift)) & 0xFFULL));
    }
}

std::uint8_t hex_nibble(char character) {
    if (character >= '0' && character <= '9') {
        return static_cast<std::uint8_t>(character - '0');
    }

    if (character >= 'a' && character <= 'f') {
        return static_cast<std::uint8_t>(10 + (character - 'a'));
    }

    throw std::logic_error(
        "canonical identity contains "
        "a non-hexadecimal character");
}

void append_hex_id(std::vector<std::byte>& output, std::string_view hex_id, std::size_t expected_hex_length) {
    if (hex_id.size() != expected_hex_length) {
        throw std::logic_error(
            "canonical identity has "
            "an unexpected length");
    }

    for (std::size_t index = 0; index < hex_id.size(); index += 2) {
        const std::uint8_t high = hex_nibble(hex_id[index]);

        const std::uint8_t low = hex_nibble(hex_id[index + 1]);

        output.push_back(
            static_cast<std::byte>(static_cast<std::uint8_t>(static_cast<std::uint8_t>(high << 4U) | low)));
    }
}

void append_uuid(std::vector<std::byte>& output, const UuidV7& uuid) {
    std::string compact;

    compact.reserve(32);

    for (const char character : uuid.str()) {
        if (character != '-') {
            compact.push_back(character);
        }
    }

    append_hex_id(output, compact, 32);
}

void append_string(std::vector<std::byte>& output, std::string_view value) {
    append_uint64_big_endian(output, static_cast<std::uint64_t>(value.size()));

    const auto bytes = std::as_bytes(std::span{
        value.data(),
        value.size(),
    });

    output.insert(output.end(), bytes.begin(), bytes.end());
}

std::string digest_to_hex(const Sha256::Digest& digest) {
    constexpr std::array<char, 16> kHexDigits{
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    };

    std::string result;

    result.reserve(digest.size() * 2);

    for (const std::byte byte : digest) {
        const auto value = std::to_integer<unsigned int>(byte);

        result.push_back(kHexDigits[(value >> 4U) & 0x0FU]);

        result.push_back(kHexDigits[value & 0x0FU]);
    }

    return result;
}

}  // namespace

Artifact::Artifact(UuidV7 artifact_id, std::string name, std::string project)
    : artifact_id_(std::move(artifact_id)), name_(std::move(name)), project_(std::move(project)) {
    if (name_.empty()) {
        throw std::invalid_argument("artifact name must not be empty");
    }

    if (project_.empty()) {
        throw std::invalid_argument("artifact project must not be empty");
    }
}

const UuidV7& Artifact::artifact_id() const noexcept { return artifact_id_; }

const std::string& Artifact::name() const noexcept { return name_; }

const std::string& Artifact::project() const noexcept { return project_; }

ArtifactVersion::ArtifactVersion(UuidV7 artifact_id, std::string root_object_id,
                                 std::optional<std::string> parent_version_id, ImmutableMetadata immutable_metadata,
                                 VersionState state)
    : artifact_id_(std::move(artifact_id)),
      root_object_id_(std::move(root_object_id)),
      parent_version_id_(std::move(parent_version_id)),
      immutable_metadata_(std::move(immutable_metadata)),
      state_(state) {
    validate_content_id(root_object_id_, "root object ID");

    if (parent_version_id_.has_value()) {
        validate_content_id(*parent_version_id_, "parent version ID");
    }

    validate_metadata(immutable_metadata_);

    canonical_bytes_ = serialize(artifact_id_, root_object_id_, parent_version_id_, immutable_metadata_);

    version_id_ = hash_canonical_bytes(canonical_bytes_);
}

const std::string& ArtifactVersion::version_id() const noexcept { return version_id_; }

const UuidV7& ArtifactVersion::artifact_id() const noexcept { return artifact_id_; }

const std::string& ArtifactVersion::root_object_id() const noexcept { return root_object_id_; }

const std::optional<std::string>& ArtifactVersion::parent_version_id() const noexcept { return parent_version_id_; }

const ArtifactVersion::ImmutableMetadata& ArtifactVersion::immutable_metadata() const noexcept {
    return immutable_metadata_;
}

const std::vector<std::byte>& ArtifactVersion::canonical_bytes() const noexcept { return canonical_bytes_; }

VersionState ArtifactVersion::state() const noexcept { return state_; }

void ArtifactVersion::validate_content_id(const std::string& content_id, const char* field_name) {
    if (content_id.size() != 64) {
        throw std::invalid_argument(std::string{field_name} +
                                    " must contain exactly 64 "
                                    "hexadecimal characters");
    }

    for (const char character : content_id) {
        const bool is_digit = character >= '0' && character <= '9';

        const bool is_lower_hex = character >= 'a' && character <= 'f';

        if (!is_digit && !is_lower_hex) {
            throw std::invalid_argument(std::string{field_name} +
                                        " must use lowercase "
                                        "hexadecimal characters");
        }
    }
}

void ArtifactVersion::validate_metadata(const ImmutableMetadata& immutable_metadata) {
    for (const auto& [key, value] : immutable_metadata) {
        static_cast<void>(value);

        if (key.empty()) {
            throw std::invalid_argument(
                "immutable metadata key "
                "must not be empty");
        }
    }
}

std::vector<std::byte> ArtifactVersion::serialize(const UuidV7& artifact_id, const std::string& root_object_id,
                                                  const std::optional<std::string>& parent_version_id,
                                                  const ImmutableMetadata& immutable_metadata) {
    std::vector<std::byte> output;

    const auto magic_bytes = std::as_bytes(std::span{
        kVersionMagic.data(),
        kVersionMagic.size(),
    });

    output.insert(output.end(), magic_bytes.begin(), magic_bytes.end());

    append_uint32_big_endian(output, kFormatVersion);

    append_uuid(output, artifact_id);

    append_hex_id(output, root_object_id, 64);

    if (parent_version_id.has_value()) {
        output.push_back(static_cast<std::byte>(1));

        append_hex_id(output, *parent_version_id, 64);
    } else {
        output.push_back(static_cast<std::byte>(0));
    }

    append_uint64_big_endian(output, static_cast<std::uint64_t>(immutable_metadata.size()));

    for (const auto& [key, value] : immutable_metadata) {
        append_string(output, key);

        append_string(output, value);
    }

    return output;
}

std::string ArtifactVersion::hash_canonical_bytes(const std::vector<std::byte>& canonical_bytes) {
    Sha256 hasher;

    hasher.update(canonical_bytes);

    return digest_to_hex(hasher.finalize());
}

}  // namespace aistore::metadata
