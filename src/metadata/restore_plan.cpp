#include "aistore/metadata/restore_plan.hpp"

#include <utility>

namespace aistore::metadata {

// NOLINTNEXTLINE(performance-unnecessary-value-param) — API takes ownership for runtime_error base
RestorePlanError::RestorePlanError(RestorePlanErrorKind kind, std::string message)
    : std::runtime_error{message}, kind_{kind} {}

RestorePlanErrorKind RestorePlanError::kind() const noexcept { return kind_; }

}  // namespace aistore::metadata
