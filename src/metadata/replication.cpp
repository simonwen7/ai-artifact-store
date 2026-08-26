#include "aistore/metadata/replication.hpp"

#include <utility>

namespace aistore::metadata {

ReplicationError::ReplicationError(ReplicationErrorKind kind, const std::string& message)
    : std::runtime_error(message), kind_(kind) {}

ReplicationErrorKind ReplicationError::kind() const noexcept { return kind_; }

std::string_view replication_run_state_to_string(ReplicationRunState state) {
    switch (state) {
        case ReplicationRunState::Open:
            return "open";

        case ReplicationRunState::Completed:
            return "completed";
    }

    throw std::logic_error("unsupported replication run state");
}

ReplicationRunState replication_run_state_from_string(std::string_view state) {
    if (state == "open") {
        return ReplicationRunState::Open;
    }

    if (state == "completed") {
        return ReplicationRunState::Completed;
    }

    throw std::runtime_error("unsupported replication run state string");
}

}  // namespace aistore::metadata
