#include "aistore/metadata/gc.hpp"

namespace aistore::metadata {

GcError::GcError(GcErrorKind kind, const std::string& message) : std::runtime_error(message), kind_(kind) {}

GcErrorKind GcError::kind() const noexcept { return kind_; }

std::string_view gc_run_mode_to_string(GcRunMode mode) {
    switch (mode) {
        case GcRunMode::Apply:
            return "apply";

        case GcRunMode::DryRun:
            return "dry-run";
    }

    throw std::logic_error("unsupported GC run mode");
}

GcRunMode gc_run_mode_from_string(std::string_view mode) {
    if (mode == "apply") {
        return GcRunMode::Apply;
    }

    if (mode == "dry-run") {
        return GcRunMode::DryRun;
    }

    throw std::runtime_error("unsupported GC run mode string");
}

std::string_view gc_run_state_to_string(GcRunState state) {
    switch (state) {
        case GcRunState::Open:
            return "open";

        case GcRunState::Completed:
            return "completed";
    }

    throw std::logic_error("unsupported GC run state");
}

GcRunState gc_run_state_from_string(std::string_view state) {
    if (state == "open") {
        return GcRunState::Open;
    }

    if (state == "completed") {
        return GcRunState::Completed;
    }

    throw std::runtime_error("unsupported GC run state string");
}

}  // namespace aistore::metadata
