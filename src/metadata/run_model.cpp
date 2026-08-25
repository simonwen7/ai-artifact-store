#include "aistore/metadata/run_model.hpp"

#include <stdexcept>
#include <string>
#include <utility>

namespace aistore::metadata {

Run::Run(UuidV7 run_id, std::string manifest_id, std::string name, std::optional<std::string> source_commit, Tags tags,
         Timestamp started_at, std::optional<Timestamp> completed_at)
    : run_id_(std::move(run_id)),
      manifest_id_(std::move(manifest_id)),
      name_(std::move(name)),
      source_commit_(std::move(source_commit)),
      tags_(std::move(tags)),
      started_at_(started_at),
      completed_at_(completed_at) {
    validate_manifest_id(manifest_id_);

    if (name_.empty()) {
        throw std::invalid_argument("run name must not be empty");
    }

    if (source_commit_.has_value() && source_commit_->empty()) {
        throw std::invalid_argument("run source commit must not be empty when present");
    }

    validate_tags(tags_);

    if (completed_at_.has_value() && *completed_at_ < started_at_) {
        throw std::invalid_argument("run completed_at must not be earlier than started_at");
    }
}

const UuidV7& Run::run_id() const noexcept { return run_id_; }

const std::string& Run::manifest_id() const noexcept { return manifest_id_; }

const std::string& Run::name() const noexcept { return name_; }

const std::optional<std::string>& Run::source_commit() const noexcept { return source_commit_; }

const Run::Tags& Run::tags() const noexcept { return tags_; }

Run::Timestamp Run::started_at() const noexcept { return started_at_; }

const std::optional<Run::Timestamp>& Run::completed_at() const noexcept { return completed_at_; }

void Run::validate_manifest_id(const std::string& manifest_id) {
    if (manifest_id.size() != 64) {
        throw std::invalid_argument("manifest ID must contain exactly 64 hexadecimal characters");
    }

    for (const char character : manifest_id) {
        const bool is_digit = character >= '0' && character <= '9';

        const bool is_lower_hex = character >= 'a' && character <= 'f';

        if (!is_digit && !is_lower_hex) {
            throw std::invalid_argument("manifest ID must use lowercase hexadecimal characters");
        }
    }
}

void Run::validate_tags(const Tags& tags) {
    for (const auto& [key, value] : tags) {
        static_cast<void>(value);

        if (key.empty()) {
            throw std::invalid_argument("run tag key must not be empty");
        }
    }
}

}  // namespace aistore::metadata
