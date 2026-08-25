#include "aistore/metadata/object.hpp"

#include <gtest/gtest.h>

#include <string>

namespace {

using aistore::metadata::Object;

const std::string kObjectId(64, 'a');

TEST(ObjectTest, PreservesValidIdentityAndSize) {
    const Object object{
        kObjectId,
        1234,
    };

    EXPECT_EQ(object.object_id(), kObjectId);

    EXPECT_EQ(object.total_size(), 1234U);
}

TEST(ObjectTest, AcceptsZeroSizedObject) {
    const Object object{
        kObjectId,
        0,
    };

    EXPECT_EQ(object.total_size(), 0U);
}

TEST(ObjectTest, RejectsMalformedObjectId) {
    EXPECT_THROW(static_cast<void>(Object{
                     "invalid",
                     4,
                 }),
                 std::invalid_argument);
}

TEST(ObjectTest, RejectsUppercaseObjectId) {
    EXPECT_THROW(static_cast<void>(Object{
                     std::string(64, 'A'),
                     4,
                 }),
                 std::invalid_argument);
}

}  // namespace
