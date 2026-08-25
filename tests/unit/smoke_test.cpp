#include <gtest/gtest.h>

#include <array>
#include <span>

TEST(BuildConfigurationTest, SupportsCpp20) {
    const std::array<int, 3> values{1, 2, 3};
    const std::span<const int> view{values};

    EXPECT_EQ(view.size(), 3U);
    EXPECT_EQ(view.front(), 1);
}
