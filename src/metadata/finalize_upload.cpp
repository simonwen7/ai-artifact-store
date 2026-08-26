#include "aistore/metadata/finalize_upload.hpp"

#include <utility>

namespace aistore::metadata {

// NOLINTNEXTLINE(performance-unnecessary-value-param) — API takes ownership for runtime_error base
FinalizeUploadError::FinalizeUploadError(FinalizeUploadErrorKind kind, std::string message,
                                         std::optional<std::string> chunk_id)
    : std::runtime_error{message}, kind_{kind}, chunk_id_{std::move(chunk_id)} {}

FinalizeUploadErrorKind FinalizeUploadError::kind() const noexcept { return kind_; }

const std::optional<std::string>& FinalizeUploadError::chunk_id() const noexcept { return chunk_id_; }

}  // namespace aistore::metadata
