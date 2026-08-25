#ifndef AISTORE_METADATA_RUN_MODEL_HPP
#define AISTORE_METADATA_RUN_MODEL_HPP

#include <chrono>
#include <map>
#include <optional>
#include <string>

#include "aistore/metadata/uuid_v7.hpp"

namespace aistore::metadata {

class Run {
   public:
    using Tags = std::map<std::string, std::string>;

    using Timestamp = std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>;

    Run(UuidV7 run_id, std::string manifest_id, std::string name, std::optional<std::string> source_commit, Tags tags,
        Timestamp started_at, std::optional<Timestamp> completed_at);

    [[nodiscard]]
    const UuidV7& run_id() const noexcept;

    [[nodiscard]]
    const std::string& manifest_id() const noexcept;

    [[nodiscard]]
    const std::string& name() const noexcept;

    [[nodiscard]]
    const std::optional<std::string>& source_commit() const noexcept;

    [[nodiscard]]
    const Tags& tags() const noexcept;

    [[nodiscard]]
    Timestamp started_at() const noexcept;

    [[nodiscard]]
    const std::optional<Timestamp>& completed_at() const noexcept;

   private:
    static void validate_manifest_id(const std::string& manifest_id);

    static void validate_tags(const Tags& tags);

    UuidV7 run_id_;
    std::string manifest_id_;
    std::string name_;
    std::optional<std::string> source_commit_;
    Tags tags_;
    Timestamp started_at_;
    std::optional<Timestamp> completed_at_;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_RUN_MODEL_HPP
