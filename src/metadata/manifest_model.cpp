#include "aistore/metadata/manifest_model.hpp"

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

constexpr std::string_view kManifestMagic = "AISTORE_MANIFEST";

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

Manifest::Manifest(Entries entries) : entries_(std::move(entries)) {
    validate_entries(entries_);

    canonical_bytes_ = serialize(entries_);

    manifest_id_ = hash_canonical_bytes(canonical_bytes_);
}

const std::string& Manifest::manifest_id() const noexcept { return manifest_id_; }

const Manifest::Entries& Manifest::entries() const noexcept { return entries_; }

const std::vector<std::byte>& Manifest::canonical_bytes() const noexcept { return canonical_bytes_; }

void Manifest::validate_entries(const Entries& entries) {
    for (const auto& [role, version_id] : entries) {
        if (role.empty()) {
            throw std::invalid_argument("manifest role must not be empty");
        }

        if (version_id.size() != 64) {
            throw std::invalid_argument("manifest version ID must contain exactly 64 hexadecimal characters");
        }

        for (const char character : version_id) {
            const bool is_digit = character >= '0' && character <= '9';

            const bool is_lower_hex = character >= 'a' && character <= 'f';

            if (!is_digit && !is_lower_hex) {
                throw std::invalid_argument("manifest version ID must use lowercase hexadecimal characters");
            }
        }
    }
}

std::vector<std::byte> Manifest::serialize(const Entries& entries) {
    std::vector<std::byte> output;

    const auto magic_bytes = std::as_bytes(std::span{
        kManifestMagic.data(),
        kManifestMagic.size(),
    });

    output.insert(output.end(), magic_bytes.begin(), magic_bytes.end());

    append_uint32_big_endian(output, kFormatVersion);

    append_uint64_big_endian(output, static_cast<std::uint64_t>(entries.size()));

    for (const auto& [role, version_id] : entries) {
        append_string(output, role);

        append_hex_id(output, version_id, 64);
    }

    return output;
}

std::string Manifest::hash_canonical_bytes(const std::vector<std::byte>& canonical_bytes) {
    Sha256 hasher;

    hasher.update(canonical_bytes);

    return digest_to_hex(hasher.finalize());
}

}  // namespace aistore::metadata
