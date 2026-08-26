#ifndef AISTORE_METADATA_FINALIZE_UPLOAD_HPP
#define AISTORE_METADATA_FINALIZE_UPLOAD_HPP

#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>

#include "aistore/metadata/uuid_v7.hpp"

namespace aistore::metadata {

enum class FinalizeUploadErrorKind : std::uint8_t {
    SessionNotFound,
    SessionNotOpen,
    Conflict,
    ChunkNotAvailableOnTarget,
};

class FinalizeUploadError : public std::runtime_error {
   public:
    FinalizeUploadError(FinalizeUploadErrorKind kind, std::string message,
                        std::optional<std::string> chunk_id = std::nullopt);

    [[nodiscard]] FinalizeUploadErrorKind kind() const noexcept;

    [[nodiscard]] const std::optional<std::string>& chunk_id() const noexcept;

   private:
    FinalizeUploadErrorKind kind_;
    std::optional<std::string> chunk_id_;
};

struct FinalizeUploadResult {
    UuidV7 session_id;
    std::string version_id;
    std::string object_id;
    std::string layout_id;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_FINALIZE_UPLOAD_HPP
