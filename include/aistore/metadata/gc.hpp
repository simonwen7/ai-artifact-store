#ifndef AISTORE_METADATA_GC_HPP
#define AISTORE_METADATA_GC_HPP

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "aistore/metadata/uuid_v7.hpp"

namespace aistore::metadata {

// Transaction-level advisory lock shared by start_gc_run and create_upload_session.
// Fixed signed BIGINT key for pg_advisory_xact_lock.
inline constexpr std::int64_t kGcCoordinationAdvisoryLockKey = 0x415353544F524547LL;  // "ASSTOREG"

enum class GcRunMode : std::uint8_t {
    Apply,
    DryRun,
};

enum class GcRunState : std::uint8_t {
    Open,
    Completed,
};

enum class GcErrorKind : std::uint8_t {
    RunNotFound,
    RunConflict,
    AnotherRunOpen,
    OpenUploadSessionsPresent,
    RunNotOpen,
    GcInProgress,
};

class GcError : public std::runtime_error {
   public:
    GcError(GcErrorKind kind, const std::string& message);

    [[nodiscard]] GcErrorKind kind() const noexcept;

   private:
    GcErrorKind kind_;
};

struct GcPhysicalStats {
    std::uint64_t physical_chunks_scanned = 0;
    std::uint64_t physical_bytes_scanned = 0;
    std::uint64_t collectible_chunks = 0;
    std::uint64_t collectible_bytes = 0;
    std::uint64_t physically_deleted_chunks = 0;
    std::uint64_t physically_deleted_bytes = 0;

    bool operator==(const GcPhysicalStats&) const = default;
};

struct GcMetadataStats {
    std::uint64_t storage_locations_swept = 0;
    std::uint64_t chunk_rows_swept = 0;
    std::uint64_t object_layouts_swept = 0;
    std::uint64_t objects_swept = 0;

    bool operator==(const GcMetadataStats&) const = default;
};

struct GcRun {
    UuidV7 run_id;
    std::string target_node_id;
    GcRunMode mode = GcRunMode::Apply;
    GcRunState state = GcRunState::Open;
    GcPhysicalStats physical_stats{};
    GcMetadataStats metadata_stats{};
};

struct GcChunkDecision {
    std::string chunk_id;
    bool collectible = false;
};

[[nodiscard]] std::string_view gc_run_mode_to_string(GcRunMode mode);

[[nodiscard]] GcRunMode gc_run_mode_from_string(std::string_view mode);

[[nodiscard]] std::string_view gc_run_state_to_string(GcRunState state);

[[nodiscard]] GcRunState gc_run_state_from_string(std::string_view state);

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_GC_HPP
