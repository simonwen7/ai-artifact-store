#include "aistore/metadata/object_descriptor.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
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

constexpr std::string_view kMagic = "AISTORE1";
constexpr std::size_t kChunkIdBytes = 32;
constexpr std::size_t kChunkEntryBytes = kChunkIdBytes + sizeof(std::uint64_t) + sizeof(std::uint64_t);
constexpr std::size_t kDescriptorHeaderBytes = kMagic.size() + sizeof(std::uint32_t) + sizeof(std::uint64_t);

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

    throw std::logic_error("object layout contains an invalid hexadecimal chunk ID");
}

void append_chunk_id(std::vector<std::byte>& output, std::string_view chunk_id) {
    for (std::size_t index = 0; index < chunk_id.size(); index += 2) {
        const std::uint8_t high = hex_nibble(chunk_id[index]);

        const std::uint8_t low = hex_nibble(chunk_id[index + 1]);

        const auto byte_value = static_cast<std::uint8_t>(static_cast<std::uint8_t>(high << 4U) | low);

        output.push_back(static_cast<std::byte>(byte_value));
    }
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

ObjectDescriptor::ObjectDescriptor(ObjectLayout layout)
    : layout_(std::move(layout)),
      canonical_bytes_(serialize(layout_)),
      object_id_(hash_canonical_bytes(canonical_bytes_)) {}

const ObjectLayout& ObjectDescriptor::layout() const noexcept { return layout_; }

const std::vector<std::byte>& ObjectDescriptor::canonical_bytes() const noexcept { return canonical_bytes_; }

const std::string& ObjectDescriptor::object_id() const noexcept { return object_id_; }

std::vector<std::byte> ObjectDescriptor::serialize(const ObjectLayout& layout) {
    const std::size_t chunk_count = layout.chunks().size();

    if (chunk_count > (std::numeric_limits<std::size_t>::max() - kDescriptorHeaderBytes) / kChunkEntryBytes) {
        throw std::length_error("object descriptor is too large to serialize");
    }

    std::vector<std::byte> output;

    output.reserve(kDescriptorHeaderBytes + (chunk_count * kChunkEntryBytes));

    const auto magic_bytes = std::as_bytes(std::span{kMagic.data(), kMagic.size()});

    output.insert(output.end(), magic_bytes.begin(), magic_bytes.end());

    append_uint32_big_endian(output, kFormatVersion);

    append_uint64_big_endian(output, static_cast<std::uint64_t>(chunk_count));

    for (const ChunkRef& chunk : layout.chunks()) {
        append_chunk_id(output, chunk.chunk_id);

        append_uint64_big_endian(output, chunk.offset);

        append_uint64_big_endian(output, chunk.size);
    }

    return output;
}

std::string ObjectDescriptor::hash_canonical_bytes(const std::vector<std::byte>& canonical_bytes) {
    Sha256 hasher;

    hasher.update(canonical_bytes);

    return digest_to_hex(hasher.finalize());
}

}  // namespace aistore::metadata
