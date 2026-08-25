#include "aistore/metadata/run_model.hpp"

#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <stdexcept>
#include <string>

namespace {

using aistore::metadata::UuidV7;

// GoogleTest's testing::Test::Run() shadows a bare using-declaration for Run.
using RunModel = aistore::metadata::Run;

const UuidV7 kRunId{
    "01890f3e-9c8a-7cc2-bc63-7f0c2e67a1d1",
};

const std::string kManifestId(64, 'a');

TEST(RunModelTest, PreservesRunRecord) {
    const RunModel run{
        kRunId,
        kManifestId,
        "training-run",
        std::string{"abc123"},
        RunModel::Tags{
            {"environment", "dev"},
            {"owner", "ml-team"},
        },
        RunModel::Timestamp{
            std::chrono::milliseconds{
                1700000000000LL,
            },
        },
        RunModel::Timestamp{
            std::chrono::milliseconds{
                1700000060000LL,
            },
        },
    };

    EXPECT_EQ(run.run_id(), kRunId);

    EXPECT_EQ(run.manifest_id(), kManifestId);

    EXPECT_EQ(run.name(), "training-run");

    ASSERT_TRUE(run.source_commit().has_value());

    EXPECT_EQ(*run.source_commit(), "abc123");

    EXPECT_EQ(run.tags(), (RunModel::Tags{
                              {"environment", "dev"},
                              {"owner", "ml-team"},
                          }));

    EXPECT_EQ(run.started_at(), RunModel::Timestamp{std::chrono::milliseconds{1700000000000LL}});

    ASSERT_TRUE(run.completed_at().has_value());

    EXPECT_EQ(*run.completed_at(), RunModel::Timestamp{std::chrono::milliseconds{1700000060000LL}});
}

TEST(RunModelTest, RejectsMalformedManifestId) {
    EXPECT_THROW(static_cast<void>(RunModel{
                     kRunId,
                     "invalid-manifest-id",
                     "training-run",
                     std::nullopt,
                     RunModel::Tags{},
                     RunModel::Timestamp{std::chrono::milliseconds{1700000000000LL}},
                     std::nullopt,
                 }),
                 std::invalid_argument);
}

TEST(RunModelTest, RejectsEmptyName) {
    EXPECT_THROW(static_cast<void>(RunModel{
                     kRunId,
                     kManifestId,
                     "",
                     std::nullopt,
                     RunModel::Tags{},
                     RunModel::Timestamp{std::chrono::milliseconds{1700000000000LL}},
                     std::nullopt,
                 }),
                 std::invalid_argument);
}

TEST(RunModelTest, RejectsEmptySourceCommitWhenPresent) {
    EXPECT_THROW(static_cast<void>(RunModel{
                     kRunId,
                     kManifestId,
                     "training-run",
                     std::string{""},
                     RunModel::Tags{},
                     RunModel::Timestamp{std::chrono::milliseconds{1700000000000LL}},
                     std::nullopt,
                 }),
                 std::invalid_argument);
}

TEST(RunModelTest, RejectsEmptyTagKey) {
    EXPECT_THROW(static_cast<void>(RunModel{
                     kRunId,
                     kManifestId,
                     "training-run",
                     std::nullopt,
                     RunModel::Tags{
                         {"", "value"},
                     },
                     RunModel::Timestamp{std::chrono::milliseconds{1700000000000LL}},
                     std::nullopt,
                 }),
                 std::invalid_argument);
}

TEST(RunModelTest, RejectsCompletionBeforeStart) {
    EXPECT_THROW(static_cast<void>(RunModel{
                     kRunId,
                     kManifestId,
                     "training-run",
                     std::nullopt,
                     RunModel::Tags{},
                     RunModel::Timestamp{std::chrono::milliseconds{1700000060000LL}},
                     RunModel::Timestamp{std::chrono::milliseconds{1700000000000LL}},
                 }),
                 std::invalid_argument);
}

}  // namespace
