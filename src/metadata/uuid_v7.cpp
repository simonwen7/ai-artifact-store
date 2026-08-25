#include "aistore/metadata/uuid_v7.hpp"

#include <openssl/rand.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

namespace aistore::metadata {

namespace {

constexpr std::size_t kUuidByteCount = 16;
constexpr std::size_t kRandomByteCount = 10;
constexpr std::uint64_t kMaxTimestamp = (std::uint64_t{1} << 48U) - 1U;

bool is_lower_hex(char character) {
    return (character >= '0' && character <= '9') || (character >= 'a' && character <= 'f');
}

std::string format_uuid(const std::array<unsigned char, kUuidByteCount>& bytes) {
    constexpr std::array<char, 16> kHexDigits{
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    };

    std::string result;
    result.reserve(36);

    for (std::size_t index = 0; index < bytes.size(); ++index) {
        if (index == 4 || index == 6 || index == 8 || index == 10) {
            result.push_back('-');
        }

        const unsigned char value = bytes[index];

        result.push_back(kHexDigits[(value >> 4U) & 0x0FU]);

        result.push_back(kHexDigits[value & 0x0FU]);
    }

    return result;
}

}  // namespace

UuidV7 UuidV7::generate() {
    const auto now = std::chrono::system_clock::now().time_since_epoch();

    const auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(now).count();

    if (milliseconds < 0) {
        throw std::runtime_error("cannot generate UUIDv7 before Unix epoch");
    }

    const auto timestamp = static_cast<std::uint64_t>(milliseconds);

    if (timestamp > kMaxTimestamp) {
        throw std::runtime_error("UUIDv7 timestamp exceeds 48-bit range");
    }

    std::array<unsigned char, kRandomByteCount> random_bytes{};

    if (RAND_bytes(random_bytes.data(), static_cast<int>(random_bytes.size())) != 1) {
        throw std::runtime_error("failed to generate UUIDv7 random bytes");
    }

    std::array<unsigned char, kUuidByteCount> bytes{};

    for (std::size_t index = 0; index < 6; ++index) {
        const auto shift = static_cast<unsigned int>((5U - index) * 8U);

        bytes[index] = static_cast<unsigned char>((timestamp >> shift) & 0xFFU);
    }

    bytes[6] = static_cast<unsigned char>(0x70U | (random_bytes[0] & 0x0FU));

    bytes[7] = random_bytes[1];

    bytes[8] = static_cast<unsigned char>(0x80U | (random_bytes[2] & 0x3FU));

    for (std::size_t index = 9; index < bytes.size(); ++index) {
        bytes[index] = random_bytes[index - 6];
    }

    return UuidV7{
        format_uuid(bytes),
    };
}

UuidV7::UuidV7(std::string value) : value_(std::move(value)) { validate(value_); }

const std::string& UuidV7::str() const noexcept { return value_; }

void UuidV7::validate(std::string_view value) {
    if (value.size() != 36) {
        throw std::invalid_argument("UUIDv7 must contain exactly 36 characters");
    }

    constexpr std::array<std::size_t, 4> kHyphenPositions{
        8,
        13,
        18,
        23,
    };

    for (const std::size_t position : kHyphenPositions) {
        if (value[position] != '-') {
            throw std::invalid_argument("UUIDv7 contains invalid separator positions");
        }
    }

    for (std::size_t index = 0; index < value.size(); ++index) {
        const bool is_hyphen_position = index == 8 || index == 13 || index == 18 || index == 23;

        if (is_hyphen_position) {
            continue;
        }

        if (!is_lower_hex(value[index])) {
            throw std::invalid_argument("UUIDv7 must use lowercase hexadecimal characters");
        }
    }

    if (value[14] != '7') {
        throw std::invalid_argument("UUID does not contain version 7 bits");
    }

    const char variant = value[19];

    if (variant != '8' && variant != '9' && variant != 'a' && variant != 'b') {
        throw std::invalid_argument("UUIDv7 does not contain RFC variant bits");
    }
}

}  // namespace aistore::metadata
