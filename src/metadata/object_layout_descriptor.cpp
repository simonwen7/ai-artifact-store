#include "aistore/metadata/object_layout_descriptor.hpp"

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

constexpr std::string_view kMagic = "AISTORE_LAYOUT";

constexpr std::size_t kContentIdBytes = 32;

constexpr std::size_t kChunkEntryBytes = kContentIdBytes + sizeof(std::uint64_t) + sizeof(std::uint64_t);

constexpr std::size_t kFixedSizeDescriptorHeaderBytes = kMagic.size() + sizeof(std::uint32_t) + kContentIdBytes +
                                                        sizeof(std::uint8_t) + sizeof(std::uint64_t) +
                                                        sizeof(std::uint64_t);

constexpr std::size_t kFastCdcDescriptorHeaderBytes = kFixedSizeDescriptorHeaderBytes + (3U * sizeof(std::uint64_t));

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

    throw std::logic_error("content ID contains a non-hexadecimal character");
}

void append_content_id(std::vector<std::byte>& output, std::string_view content_id) {
    if (content_id.size() != 64) {
        throw std::logic_error("content ID must contain exactly 64 hexadecimal characters");
    }

    for (std::size_t index = 0; index < content_id.size(); index += 2) {
        const std::uint8_t high = hex_nibble(content_id[index]);

        const std::uint8_t low = hex_nibble(content_id[index + 1]);

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

ObjectLayoutDescriptor::ObjectLayoutDescriptor(Object object, ChunkingStrategy chunking_strategy, ObjectLayout layout)
    : object_(std::move(object)),
      chunking_strategy_(chunking_strategy),
      fastcdc_parameters_(std::nullopt),
      layout_(std::move(layout)) {
    if (chunking_strategy_ != ChunkingStrategy::FixedSize) {
        throw std::invalid_argument("FixedSize ObjectLayoutDescriptor constructor requires FixedSize strategy");
    }

    if (object_.total_size() != layout_.total_size()) {
        throw std::invalid_argument("object size must match object layout size");
    }

    canonical_bytes_ = serialize(object_, chunking_strategy_, fastcdc_parameters_, layout_);

    layout_id_ = hash_canonical_bytes(canonical_bytes_);
}

ObjectLayoutDescriptor::ObjectLayoutDescriptor(Object object, FastCdcParameters fastcdc_parameters, ObjectLayout layout)
    : object_(std::move(object)),
      chunking_strategy_(ChunkingStrategy::FastCdc),
      fastcdc_parameters_(fastcdc_parameters),
      layout_(std::move(layout)) {
    validate_fastcdc_parameters(*fastcdc_parameters_);

    if (object_.total_size() != layout_.total_size()) {
        throw std::invalid_argument("object size must match object layout size");
    }

    canonical_bytes_ = serialize(object_, chunking_strategy_, fastcdc_parameters_, layout_);

    layout_id_ = hash_canonical_bytes(canonical_bytes_);
}

const Object& ObjectLayoutDescriptor::object() const noexcept { return object_; }

const std::string& ObjectLayoutDescriptor::object_id() const noexcept { return object_.object_id(); }

ChunkingStrategy ObjectLayoutDescriptor::chunking_strategy() const noexcept { return chunking_strategy_; }

std::optional<FastCdcParameters> ObjectLayoutDescriptor::fastcdc_parameters() const noexcept {
    return fastcdc_parameters_;
}

const ObjectLayout& ObjectLayoutDescriptor::layout() const noexcept { return layout_; }

const std::vector<std::byte>& ObjectLayoutDescriptor::canonical_bytes() const noexcept { return canonical_bytes_; }

const std::string& ObjectLayoutDescriptor::layout_id() const noexcept { return layout_id_; }

std::vector<std::byte> ObjectLayoutDescriptor::serialize(const Object& object, ChunkingStrategy chunking_strategy,
                                                         const std::optional<FastCdcParameters>& fastcdc_parameters,
                                                         const ObjectLayout& layout) {
    const std::size_t chunk_count = layout.chunks().size();

    const std::size_t header_bytes = chunking_strategy == ChunkingStrategy::FastCdc ? kFastCdcDescriptorHeaderBytes
                                                                                    : kFixedSizeDescriptorHeaderBytes;

    if (chunk_count > (std::numeric_limits<std::size_t>::max() - header_bytes) / kChunkEntryBytes) {
        throw std::length_error("object layout descriptor is too large to serialize");
    }

    std::vector<std::byte> output;

    output.reserve(header_bytes + (chunk_count * kChunkEntryBytes));

    const auto magic_bytes = std::as_bytes(std::span{
        kMagic.data(),
        kMagic.size(),
    });

    output.insert(output.end(), magic_bytes.begin(), magic_bytes.end());

    append_uint32_big_endian(output, kFormatVersion);

    append_content_id(output, object.object_id());

    output.push_back(static_cast<std::byte>(chunking_strategy_code(chunking_strategy)));

    if (chunking_strategy == ChunkingStrategy::FastCdc) {
        if (!fastcdc_parameters.has_value()) {
            throw std::logic_error("FastCDC layout serialization requires FastCDC parameters");
        }

        append_uint64_big_endian(output, fastcdc_parameters->min_chunk_size_bytes);
        append_uint64_big_endian(output, fastcdc_parameters->avg_chunk_size_bytes);
        append_uint64_big_endian(output, fastcdc_parameters->max_chunk_size_bytes);
    }

    append_uint64_big_endian(output, object.total_size());

    append_uint64_big_endian(output, static_cast<std::uint64_t>(chunk_count));

    for (const ChunkRef& chunk : layout.chunks()) {
        append_content_id(output, chunk.chunk_id);

        append_uint64_big_endian(output, chunk.offset);

        append_uint64_big_endian(output, chunk.size);
    }

    return output;
}

std::string ObjectLayoutDescriptor::hash_canonical_bytes(const std::vector<std::byte>& canonical_bytes) {
    Sha256 hasher;

    hasher.update(canonical_bytes);

    return digest_to_hex(hasher.finalize());
}

}  // namespace aistore::metadata
