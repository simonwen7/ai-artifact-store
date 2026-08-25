#include "aistore/hashing/sha256.hpp"

#include <gtest/gtest.h>

#include <cstddef>
#include <span>
#include <string>
#include <string_view>

namespace {

using aistore::hashing::Sha256;

std::span<const std::byte> as_bytes(std::string_view text) {
    return std::as_bytes(std::span{text.data(), text.size()});
}

std::string to_hex(const Sha256::Digest& digest) {
    constexpr char kHexDigits[] = "0123456789abcdef";

    std::string result;
    result.reserve(digest.size() * 2);

    for (const std::byte byte : digest) {
        const auto value = std::to_integer<unsigned int>(byte);

        result.push_back(kHexDigits[(value >> 4U) & 0x0FU]);
        result.push_back(kHexDigits[value & 0x0FU]);
    }

    return result;
}

TEST(Sha256Test, HashesEmptyInput) {
    Sha256 hasher;

    const auto digest = hasher.finalize();

    EXPECT_EQ(to_hex(digest),
              "e3b0c44298fc1c149afbf4c8996fb924"
              "27ae41e4649b934ca495991b7852b855");
}

TEST(Sha256Test, HashesKnownInput) {
    Sha256 hasher;
    hasher.update(as_bytes("abc"));

    const auto digest = hasher.finalize();

    EXPECT_EQ(to_hex(digest),
              "ba7816bf8f01cfea414140de5dae2223"
              "b00361a396177a9cb410ff61f20015ad");
}

TEST(Sha256Test, SupportsIncrementalUpdates) {
    Sha256 hasher;

    hasher.update(as_bytes("a"));
    hasher.update(as_bytes("b"));
    hasher.update(as_bytes("c"));

    const auto digest = hasher.finalize();

    EXPECT_EQ(to_hex(digest),
              "ba7816bf8f01cfea414140de5dae2223"
              "b00361a396177a9cb410ff61f20015ad");
}

TEST(Sha256Test, RejectsUpdateAfterFinalize) {
    Sha256 hasher;
    hasher.update(as_bytes("abc"));

    static_cast<void>(hasher.finalize());

    EXPECT_THROW(hasher.update(as_bytes("more data")), std::logic_error);
}

TEST(Sha256Test, RejectsRepeatedFinalize) {
    Sha256 hasher;

    static_cast<void>(hasher.finalize());

    EXPECT_THROW(static_cast<void>(hasher.finalize()), std::logic_error);
}

}  // namespace
