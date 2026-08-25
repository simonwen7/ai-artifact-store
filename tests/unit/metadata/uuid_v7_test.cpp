#include "aistore/metadata/uuid_v7.hpp"

#include <gtest/gtest.h>

#include <string>

namespace {

using aistore::metadata::UuidV7;

TEST(UuidV7Test, GeneratesValidUuidV7) {
    const UuidV7 generated = UuidV7::generate();

    EXPECT_EQ(generated.str().size(), 36U);

    EXPECT_NO_THROW(static_cast<void>(UuidV7{generated.str()}));
}

TEST(UuidV7Test, GeneratesDistinctIds) {
    const UuidV7 first = UuidV7::generate();
    const UuidV7 second = UuidV7::generate();

    EXPECT_NE(first, second);
}

TEST(UuidV7Test, RejectsInvalidLength) { EXPECT_THROW(static_cast<void>(UuidV7{"1234"}), std::invalid_argument); }

TEST(UuidV7Test, RejectsUppercaseHexadecimal) {
    EXPECT_THROW(static_cast<void>(UuidV7{"01890F3E-9C8A-7CC2-BC63-7F0C2E67A1D1"}), std::invalid_argument);
}

TEST(UuidV7Test, RejectsWrongVersion) {
    EXPECT_THROW(static_cast<void>(UuidV7{"01890f3e-9c8a-6cc2-bc63-7f0c2e67a1d1"}), std::invalid_argument);
}

TEST(UuidV7Test, RejectsWrongVariant) {
    EXPECT_THROW(static_cast<void>(UuidV7{"01890f3e-9c8a-7cc2-7c63-7f0c2e67a1d1"}), std::invalid_argument);
}

}  // namespace
