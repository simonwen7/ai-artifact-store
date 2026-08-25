#include "aistore/metadata/manifest_model.hpp"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

using aistore::metadata::Manifest;

std::string bytes_to_hex(const std::vector<std::byte>& bytes) {
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

TEST(ManifestModelTest, CanonicalizesKnownManifest) {
    const Manifest::Entries entries{
        {"config", std::string(64, 'b')},
        {"model", std::string(64, 'a')},
    };

    const Manifest manifest{
        entries,
    };

    EXPECT_EQ(bytes_to_hex(manifest.canonical_bytes()),
              "414953544f52455f4d414e4946455354"
              "00000001"
              "0000000000000002"
              "0000000000000006"
              "636f6e666967"
              "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
              "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
              "0000000000000005"
              "6d6f64656c"
              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
              "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

    EXPECT_EQ(manifest.manifest_id(), "12adad980ddcf4a17a88a00d610e9cf823d1ec4f2c72a7362eea4060cd93c163");

    EXPECT_EQ(manifest.entries(), entries);
}

TEST(ManifestModelTest, SameEntriesDifferentInsertionOrderSameIdentity) {
    Manifest::Entries first_order;
    first_order.emplace("model", std::string(64, 'a'));
    first_order.emplace("config", std::string(64, 'b'));

    Manifest::Entries second_order;
    second_order.emplace("config", std::string(64, 'b'));
    second_order.emplace("model", std::string(64, 'a'));

    const Manifest first{
        std::move(first_order),
    };

    const Manifest second{
        std::move(second_order),
    };

    EXPECT_EQ(first.canonical_bytes(), second.canonical_bytes());

    EXPECT_EQ(first.manifest_id(), second.manifest_id());
}

TEST(ManifestModelTest, DifferentVersionReferenceChangesIdentity) {
    const Manifest first{
        Manifest::Entries{
            {"model", std::string(64, 'a')},
        },
    };

    const Manifest second{
        Manifest::Entries{
            {"model", std::string(64, 'b')},
        },
    };

    EXPECT_NE(first.manifest_id(), second.manifest_id());
}

TEST(ManifestModelTest, RejectsEmptyRole) {
    EXPECT_THROW(static_cast<void>(Manifest{
                     Manifest::Entries{
                         {"", std::string(64, 'a')},
                     },
                 }),
                 std::invalid_argument);
}

TEST(ManifestModelTest, RejectsMalformedVersionId) {
    EXPECT_THROW(static_cast<void>(Manifest{
                     Manifest::Entries{
                         {"model", "invalid"},
                     },
                 }),
                 std::invalid_argument);
}

}  // namespace
