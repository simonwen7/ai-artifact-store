#include "aistore/service/metadata_service_m9_routes.hpp"

#include <array>
#include <boost/json.hpp>
#include <cstdint>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "aistore/metadata/lifecycle.hpp"

namespace aistore::service {

namespace {

namespace beast_http = aistore::http::beast_http;

constexpr std::string_view kApplicationJson = "application/json";
constexpr std::string_view kLifecyclePoliciesCollection = "/v1/lifecycle-policies";
constexpr std::string_view kLifecyclePoliciesPrefix = "/v1/lifecycle-policies/";
constexpr std::string_view kLifecycleRunsCollection = "/v1/lifecycle-runs";
constexpr std::string_view kLifecycleRunsPrefix = "/v1/lifecycle-runs/";
constexpr std::string_view kArtifactVersionsPrefix = "/v1/artifact-versions/";
constexpr std::string_view kPinSuffix = "/pin";
constexpr std::string_view kDecisionsSuffix = "/decisions";
constexpr std::size_t kDefaultLifecycleDecisionPageLimit = 128U;
constexpr std::size_t kMaxLifecycleDecisionPageLimit = 256U;
constexpr std::size_t kExpectedRuleCount = 5U;
constexpr std::uint64_t kPostgresBigintMax = static_cast<std::uint64_t>(std::numeric_limits<long long>::max());

constexpr std::array<aistore::metadata::ArtifactKind, kExpectedRuleCount> kCanonicalArtifactKindOrder = {
    aistore::metadata::ArtifactKind::Generic,          aistore::metadata::ArtifactKind::ModelCheckpoint,
    aistore::metadata::ArtifactKind::DatasetSnapshot,  aistore::metadata::ArtifactKind::EmbeddingIndex,
    aistore::metadata::ArtifactKind::EvaluationOutput,
};

aistore::http::HttpResponse make_json_response(const aistore::http::HttpRequest& request, beast_http::status status,
                                               const boost::json::value& body) {
    aistore::http::HttpResponse response{
        status,
        request.version(),
    };
    response.set(beast_http::field::content_type, "application/json");
    response.body() = boost::json::serialize(body);
    response.prepare_payload();
    return response;
}

aistore::http::HttpResponse make_method_not_allowed(const aistore::http::HttpRequest& request,
                                                    std::string_view allowed) {
    aistore::http::HttpResponse response = make_json_response(request, beast_http::status::method_not_allowed,
                                                              boost::json::object{
                                                                  {"error", "method_not_allowed"},
                                                              });
    response.set(beast_http::field::allow, allowed);
    return response;
}

[[nodiscard]] bool json_object_has_exact_keys(const boost::json::object& object,
                                              std::initializer_list<std::string_view> keys) {
    if (object.size() != keys.size()) {
        return false;
    }

    for (const std::string_view key : keys) {
        if (!object.contains(key)) {
            return false;
        }
    }

    return true;
}

[[nodiscard]] bool is_valid_chunk_id(std::string_view chunk_id) {
    if (chunk_id.size() != 64U) {
        return false;
    }

    for (const char character : chunk_id) {
        const bool is_digit = character >= '0' && character <= '9';
        const bool is_lower_hex = character >= 'a' && character <= 'f';

        if (!is_digit && !is_lower_hex) {
            return false;
        }
    }

    return true;
}

[[nodiscard]] std::optional<std::uint64_t> extract_nonnegative_uint64(const boost::json::value& value) {
    if (value.is_double()) {
        return std::nullopt;
    }

    std::uint64_t number = 0;

    if (value.is_uint64()) {
        number = value.as_uint64();
    } else if (value.is_int64()) {
        const std::int64_t signed_number = value.as_int64();

        if (signed_number < 0) {
            return std::nullopt;
        }

        number = static_cast<std::uint64_t>(signed_number);
    } else {
        return std::nullopt;
    }

    if (number > kPostgresBigintMax) {
        return std::nullopt;
    }

    return number;
}

[[nodiscard]] std::optional<std::size_t> parse_positive_size(std::string_view text) {
    if (text.empty()) {
        return std::nullopt;
    }

    std::size_t value = 0;

    for (const char character : text) {
        if (character < '0' || character > '9') {
            return std::nullopt;
        }

        const auto digit = static_cast<std::size_t>(character - '0');

        if (value > (std::numeric_limits<std::size_t>::max() - digit) / 10U) {
            return std::nullopt;
        }

        value = (value * 10U) + digit;
    }

    return value;
}

[[nodiscard]] aistore::http::HttpResponse map_lifecycle_error(const aistore::http::HttpRequest& request,
                                                              const aistore::metadata::LifecycleError& error) {
    switch (error.kind()) {
        case aistore::metadata::LifecycleErrorKind::InvalidRequest:
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });

        case aistore::metadata::LifecycleErrorKind::PolicyNotFound:
            return make_json_response(request, beast_http::status::not_found,
                                      boost::json::object{
                                          {"error", "lifecycle_policy_not_found"},
                                      });

        case aistore::metadata::LifecycleErrorKind::RunNotFound:
            return make_json_response(request, beast_http::status::not_found,
                                      boost::json::object{
                                          {"error", "lifecycle_run_not_found"},
                                      });

        case aistore::metadata::LifecycleErrorKind::VersionNotFound:
            return make_json_response(request, beast_http::status::not_found,
                                      boost::json::object{
                                          {"error", "artifact_version_not_found"},
                                      });

        case aistore::metadata::LifecycleErrorKind::VersionRetired:
            return make_json_response(request, beast_http::status::gone,
                                      boost::json::object{
                                          {"error", "artifact_version_retired"},
                                      });

        case aistore::metadata::LifecycleErrorKind::RunConflict:
            return make_json_response(request, beast_http::status::conflict,
                                      boost::json::object{
                                          {"error", "lifecycle_run_conflict"},
                                      });

        case aistore::metadata::LifecycleErrorKind::BlockedByOpenUploadSessions:
            return make_json_response(request, beast_http::status::conflict,
                                      boost::json::object{
                                          {"error", "lifecycle_blocked_by_open_upload_sessions"},
                                      });

        case aistore::metadata::LifecycleErrorKind::BlockedByGc:
            return make_json_response(request, beast_http::status::conflict,
                                      boost::json::object{
                                          {"error", "lifecycle_blocked_by_gc"},
                                      });

        case aistore::metadata::LifecycleErrorKind::BlockedByReplication:
            return make_json_response(request, beast_http::status::conflict,
                                      boost::json::object{
                                          {"error", "lifecycle_blocked_by_replication"},
                                      });

        case aistore::metadata::LifecycleErrorKind::InvalidArtifactKind:
            return make_json_response(request, beast_http::status::conflict,
                                      boost::json::object{
                                          {"error", "invalid_artifact_kind"},
                                      });

        case aistore::metadata::LifecycleErrorKind::PolicyConflict:
            return make_json_response(request, beast_http::status::conflict,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
    }

    throw std::logic_error("unsupported lifecycle error kind");
}

[[nodiscard]] boost::json::object lifecycle_rule_to_json(const aistore::metadata::LifecycleRule& rule) {
    boost::json::object object{
        {"artifact_kind", std::string{aistore::metadata::artifact_kind_to_string(rule.artifact_kind)}},
        {"keep_last_n", rule.keep_last_n},
    };

    if (rule.max_age_seconds.has_value()) {
        object["max_age_seconds"] = *rule.max_age_seconds;
    } else {
        object["max_age_seconds"] = nullptr;
    }

    return object;
}

[[nodiscard]] boost::json::object lifecycle_policy_to_json(const aistore::metadata::LifecyclePolicy& policy) {
    boost::json::array rules;
    rules.reserve(kExpectedRuleCount);

    for (const aistore::metadata::ArtifactKind kind : kCanonicalArtifactKindOrder) {
        const auto rule_it = policy.rules.find(kind);

        if (rule_it == policy.rules.end()) {
            throw std::logic_error("lifecycle policy is missing a canonical artifact kind rule");
        }

        rules.push_back(lifecycle_rule_to_json(rule_it->second));
    }

    return boost::json::object{
        {"policy_id", policy.policy_id.str()},
        {"name", policy.name},
        {"rules", std::move(rules)},
    };
}

[[nodiscard]] boost::json::object lifecycle_run_to_json(const aistore::metadata::LifecycleRun& run) {
    return boost::json::object{
        {"run_id", run.run_id.str()},
        {"policy_id", run.policy_id.str()},
        {"mode", std::string{aistore::metadata::lifecycle_run_mode_to_string(run.mode)}},
        {"evaluated_at_unix_ms", run.evaluated_at_unix_ms},
        {"versions_scanned", run.stats.versions_scanned},
        {"versions_protected", run.stats.versions_protected},
        {"versions_retained_by_policy", run.stats.versions_retained_by_policy},
        {"versions_candidates", run.stats.versions_candidates},
        {"versions_retired", run.stats.versions_retired},
        {"logical_bytes_candidates", run.stats.logical_bytes_candidates},
        {"logical_bytes_retired", run.stats.logical_bytes_retired},
    };
}

[[nodiscard]] boost::json::object lifecycle_decision_to_json(const aistore::metadata::LifecycleDecision& decision) {
    return boost::json::object{
        {"version_id", decision.version_id},
        {"artifact_id", decision.artifact_id.str()},
        {"artifact_kind", std::string{aistore::metadata::artifact_kind_to_string(decision.artifact_kind)}},
        {"decision", decision.retire ? "retire" : "retain"},
        {"reason", std::string{aistore::metadata::lifecycle_decision_reason_to_string(decision.reason)}},
        {"logical_size_bytes", decision.logical_size_bytes},
    };
}

struct LifecycleDecisionQuery {
    std::optional<std::string_view> after;
    std::size_t limit{kDefaultLifecycleDecisionPageLimit};
};

[[nodiscard]] std::optional<LifecycleDecisionQuery> parse_lifecycle_decisions_query(std::string_view query) {
    LifecycleDecisionQuery parsed;

    if (query.empty()) {
        return parsed;
    }

    std::size_t start = 0;

    while (start < query.size()) {
        const std::size_t ampersand = query.find('&', start);
        const std::string_view pair =
            ampersand == std::string_view::npos ? query.substr(start) : query.substr(start, ampersand - start);
        const std::size_t equals = pair.find('=');

        if (equals == std::string_view::npos) {
            return std::nullopt;
        }

        const std::string_view key = pair.substr(0, equals);
        const std::string_view value = pair.substr(equals + 1U);

        if (key == "after") {
            if (parsed.after.has_value()) {
                return std::nullopt;
            }

            if (!is_valid_chunk_id(value)) {
                return std::nullopt;
            }

            parsed.after = value;
        } else if (key == "limit") {
            const std::optional<std::size_t> parsed_limit = parse_positive_size(value);

            if (!parsed_limit.has_value() || *parsed_limit < 1U || *parsed_limit > kMaxLifecycleDecisionPageLimit) {
                return std::nullopt;
            }

            parsed.limit = *parsed_limit;
        } else {
            return std::nullopt;
        }

        if (ampersand == std::string_view::npos) {
            break;
        }

        start = ampersand + 1U;
    }

    return parsed;
}

[[nodiscard]] std::optional<aistore::metadata::LifecycleRule> parse_lifecycle_rule_json(
    const boost::json::object& object) {
    if (!json_object_has_exact_keys(object, {"artifact_kind", "keep_last_n", "max_age_seconds"}) ||
        !object.at("artifact_kind").is_string()) {
        return std::nullopt;
    }

    const std::optional<std::uint64_t> keep_last_n = extract_nonnegative_uint64(object.at("keep_last_n"));

    if (!keep_last_n.has_value()) {
        return std::nullopt;
    }

    std::optional<std::uint64_t> max_age_seconds;

    if (object.at("max_age_seconds").is_null()) {
        max_age_seconds = std::nullopt;
    } else {
        max_age_seconds = extract_nonnegative_uint64(object.at("max_age_seconds"));

        if (!max_age_seconds.has_value()) {
            return std::nullopt;
        }
    }

    try {
        return aistore::metadata::LifecycleRule{
            .artifact_kind =
                aistore::metadata::artifact_kind_from_string(std::string{object.at("artifact_kind").as_string()}),
            .keep_last_n = static_cast<std::uint32_t>(*keep_last_n),
            .max_age_seconds = max_age_seconds,
        };
    } catch (const std::runtime_error&) {
        return std::nullopt;
    }
}

[[nodiscard]] std::optional<aistore::metadata::LifecyclePolicy> parse_lifecycle_policy_json(
    const boost::json::object& body) {
    if (!json_object_has_exact_keys(body, {"policy_id", "name", "rules"}) || !body.at("policy_id").is_string() ||
        !body.at("name").is_string() || !body.at("rules").is_array()) {
        return std::nullopt;
    }

    const boost::json::array& rules_json = body.at("rules").as_array();

    if (rules_json.size() != kExpectedRuleCount) {
        return std::nullopt;
    }

    std::map<aistore::metadata::ArtifactKind, aistore::metadata::LifecycleRule> rules;

    for (const boost::json::value& entry : rules_json) {
        if (!entry.is_object()) {
            return std::nullopt;
        }

        const std::optional<aistore::metadata::LifecycleRule> rule = parse_lifecycle_rule_json(entry.as_object());

        if (!rule.has_value()) {
            return std::nullopt;
        }

        if (rules.contains(rule->artifact_kind)) {
            return std::nullopt;
        }

        rules.emplace(rule->artifact_kind, *rule);
    }

    try {
        return aistore::metadata::LifecyclePolicy{
            .policy_id = aistore::metadata::UuidV7{std::string{body.at("policy_id").as_string()}},
            .name = std::string{body.at("name").as_string()},
            .rules = std::move(rules),
        };
    } catch (const std::invalid_argument&) {
        return std::nullopt;
    }
}

[[nodiscard]] aistore::http::HttpResponse handle_register_lifecycle_policy(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex) {
    if (request[beast_http::field::content_type] != kApplicationJson) {
        return make_json_response(request, beast_http::status::unsupported_media_type,
                                  boost::json::object{
                                      {"error", "unsupported_media_type"},
                                  });
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(request.body(), parse_error);

    if (parse_error || !parsed.is_object()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", parse_error ? "invalid_json" : "invalid_request"},
                                  });
    }

    const std::optional<aistore::metadata::LifecyclePolicy> policy = parse_lifecycle_policy_json(parsed.as_object());

    if (!policy.has_value()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    try {
        const std::scoped_lock lock{repository_mutex};
        repository.register_lifecycle_policy(*policy);
        const std::optional<aistore::metadata::LifecyclePolicy> loaded =
            repository.get_lifecycle_policy(policy->policy_id);

        if (!loaded.has_value()) {
            throw std::runtime_error("lifecycle policy could not be reloaded after registration");
        }

        return make_json_response(request, beast_http::status::ok, lifecycle_policy_to_json(*loaded));
    } catch (const aistore::metadata::LifecycleError& error) {
        return map_lifecycle_error(request, error);
    }
}

[[nodiscard]] aistore::http::HttpResponse handle_get_lifecycle_policy(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view policy_id_text) {
    aistore::metadata::UuidV7 policy_id{std::string{policy_id_text}};
    std::optional<aistore::metadata::LifecyclePolicy> policy;

    {
        const std::scoped_lock lock{repository_mutex};
        policy = repository.get_lifecycle_policy(policy_id);
    }

    if (!policy.has_value()) {
        return make_json_response(request, beast_http::status::not_found,
                                  boost::json::object{
                                      {"error", "lifecycle_policy_not_found"},
                                  });
    }

    return make_json_response(request, beast_http::status::ok, lifecycle_policy_to_json(*policy));
}

[[nodiscard]] aistore::http::HttpResponse handle_put_version_pin(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view version_id) {
    if (request[beast_http::field::content_type] != kApplicationJson) {
        return make_json_response(request, beast_http::status::unsupported_media_type,
                                  boost::json::object{
                                      {"error", "unsupported_media_type"},
                                  });
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(request.body(), parse_error);

    if (parse_error || !parsed.is_object()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", parse_error ? "invalid_json" : "invalid_request"},
                                  });
    }

    const boost::json::object& body = parsed.as_object();

    if (!json_object_has_exact_keys(body, {"reason"}) || !body.at("reason").is_string()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const std::string reason{body.at("reason").as_string()};

    try {
        const std::scoped_lock lock{repository_mutex};
        repository.pin_version(version_id, reason);
    } catch (const aistore::metadata::LifecycleError& error) {
        return map_lifecycle_error(request, error);
    } catch (const std::invalid_argument&) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    return make_json_response(request, beast_http::status::ok,
                              boost::json::object{
                                  {"version_id", std::string{version_id}},
                                  {"pinned", true},
                                  {"reason", reason},
                              });
}

[[nodiscard]] aistore::http::HttpResponse handle_delete_version_pin(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view version_id) {
    try {
        const std::scoped_lock lock{repository_mutex};

        if (!repository.get_version(version_id).has_value()) {
            return make_json_response(request, beast_http::status::not_found,
                                      boost::json::object{
                                          {"error", "artifact_version_not_found"},
                                      });
        }

        (void)repository.unpin_version(version_id);
    } catch (const std::invalid_argument&) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    return make_json_response(request, beast_http::status::ok,
                              boost::json::object{
                                  {"version_id", std::string{version_id}},
                                  {"pinned", false},
                              });
}

[[nodiscard]] aistore::http::HttpResponse handle_get_version_pin(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view version_id) {
    std::optional<std::string> reason;

    try {
        const std::scoped_lock lock{repository_mutex};

        if (!repository.get_version(version_id).has_value()) {
            return make_json_response(request, beast_http::status::not_found,
                                      boost::json::object{
                                          {"error", "artifact_version_not_found"},
                                      });
        }

        reason = repository.get_version_pin(version_id);
    } catch (const std::invalid_argument&) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    if (!reason.has_value()) {
        return make_json_response(request, beast_http::status::ok,
                                  boost::json::object{
                                      {"version_id", std::string{version_id}},
                                      {"pinned", false},
                                  });
    }

    return make_json_response(request, beast_http::status::ok,
                              boost::json::object{
                                  {"version_id", std::string{version_id}},
                                  {"pinned", true},
                                  {"reason", *reason},
                              });
}

[[nodiscard]] aistore::http::HttpResponse handle_run_lifecycle(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex) {
    if (request[beast_http::field::content_type] != kApplicationJson) {
        return make_json_response(request, beast_http::status::unsupported_media_type,
                                  boost::json::object{
                                      {"error", "unsupported_media_type"},
                                  });
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(request.body(), parse_error);

    if (parse_error || !parsed.is_object()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", parse_error ? "invalid_json" : "invalid_request"},
                                  });
    }

    const boost::json::object& body = parsed.as_object();

    if (!json_object_has_exact_keys(body, {"run_id", "policy_id", "mode"}) || !body.at("run_id").is_string() ||
        !body.at("policy_id").is_string() || !body.at("mode").is_string()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    aistore::metadata::LifecycleRunMode mode;

    try {
        mode = aistore::metadata::lifecycle_run_mode_from_string(std::string{body.at("mode").as_string()});
    } catch (const std::runtime_error&) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    try {
        const aistore::metadata::UuidV7 run_id{std::string{body.at("run_id").as_string()}};
        const aistore::metadata::UuidV7 policy_id{std::string{body.at("policy_id").as_string()}};
        const std::scoped_lock lock{repository_mutex};
        const aistore::metadata::LifecycleRun run = repository.run_lifecycle(run_id, policy_id, mode);
        return make_json_response(request, beast_http::status::ok, lifecycle_run_to_json(run));
    } catch (const aistore::metadata::LifecycleError& error) {
        return map_lifecycle_error(request, error);
    } catch (const std::invalid_argument&) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }
}

[[nodiscard]] aistore::http::HttpResponse handle_get_lifecycle_run(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view run_id_text) {
    const aistore::metadata::UuidV7 run_id{std::string{run_id_text}};
    std::optional<aistore::metadata::LifecycleRun> run;

    {
        const std::scoped_lock lock{repository_mutex};
        run = repository.get_lifecycle_run(run_id);
    }

    if (!run.has_value()) {
        return make_json_response(request, beast_http::status::not_found,
                                  boost::json::object{
                                      {"error", "lifecycle_run_not_found"},
                                  });
    }

    return make_json_response(request, beast_http::status::ok, lifecycle_run_to_json(*run));
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters) — frozen HTTP route parameter order
[[nodiscard]] aistore::http::HttpResponse handle_list_lifecycle_decisions(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex,
    std::string_view run_id_text,  // NOLINT(bugprone-easily-swappable-parameters)
    std::string_view query) {
    const std::optional<LifecycleDecisionQuery> parsed_query = parse_lifecycle_decisions_query(query);

    if (!parsed_query.has_value()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const aistore::metadata::UuidV7 run_id{std::string{run_id_text}};

    try {
        const std::scoped_lock lock{repository_mutex};
        const std::vector<aistore::metadata::LifecycleDecision> decisions =
            repository.list_lifecycle_decisions(run_id, parsed_query->after, parsed_query->limit);

        boost::json::array decision_array;
        decision_array.reserve(decisions.size());

        for (const aistore::metadata::LifecycleDecision& decision : decisions) {
            decision_array.push_back(lifecycle_decision_to_json(decision));
        }

        boost::json::value next_after = nullptr;

        if (decisions.size() == parsed_query->limit) {
            next_after = decisions.back().version_id;
        }

        return make_json_response(request, beast_http::status::ok,
                                  boost::json::object{
                                      {"decisions", std::move(decision_array)},
                                      {"next_after", std::move(next_after)},
                                  });
    } catch (const aistore::metadata::LifecycleError& error) {
        return map_lifecycle_error(request, error);
    } catch (const std::invalid_argument&) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }
}

}  // namespace

std::optional<aistore::http::HttpResponse> try_handle_m9_routes(const aistore::http::HttpRequest& request,
                                                                metadata::PostgresMetadataRepository& repository,
                                                                std::mutex& repository_mutex) {
    const std::string_view target = request.target();
    const std::size_t query_or_fragment = target.find_first_of("?#");
    const std::string_view path =
        query_or_fragment == std::string_view::npos ? target : target.substr(0, query_or_fragment);
    const std::string_view query =
        query_or_fragment != std::string_view::npos && target[query_or_fragment] == '?'
            ? target.substr(query_or_fragment + 1U, (target.find('#', query_or_fragment + 1U) == std::string_view::npos
                                                         ? target.size()
                                                         : target.find('#', query_or_fragment + 1U)) -
                                                        (query_or_fragment + 1U))
            : std::string_view{};

    if (path == kLifecyclePoliciesCollection) {
        if (query_or_fragment != std::string_view::npos && target[query_or_fragment] != '?') {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        if (request.method() == beast_http::verb::post) {
            return handle_register_lifecycle_policy(request, repository, repository_mutex);
        }

        return make_method_not_allowed(request, "POST");
    }

    if (path.starts_with(kLifecyclePoliciesPrefix)) {
        const std::string_view policy_id = path.substr(kLifecyclePoliciesPrefix.size());

        if (policy_id.empty() || policy_id.find('/') != std::string_view::npos) {
            return std::nullopt;
        }

        if (query_or_fragment != std::string_view::npos && target[query_or_fragment] != '?') {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        if (request.method() == beast_http::verb::get) {
            try {
                [[maybe_unused]] const aistore::metadata::UuidV7 validated_policy_id{std::string{policy_id}};
            } catch (const std::invalid_argument&) {
                return make_json_response(request, beast_http::status::bad_request,
                                          boost::json::object{
                                              {"error", "invalid_request"},
                                          });
            }

            return handle_get_lifecycle_policy(request, repository, repository_mutex, policy_id);
        }

        return make_method_not_allowed(request, "GET");
    }

    if (path == kLifecycleRunsCollection) {
        if (query_or_fragment != std::string_view::npos && target[query_or_fragment] != '?') {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        if (request.method() == beast_http::verb::post) {
            return handle_run_lifecycle(request, repository, repository_mutex);
        }

        return make_method_not_allowed(request, "POST");
    }

    if (path.starts_with(kLifecycleRunsPrefix)) {
        const std::string_view remainder = path.substr(kLifecycleRunsPrefix.size());
        const std::size_t slash = remainder.find('/');

        if (slash == std::string_view::npos) {
            if (query_or_fragment != std::string_view::npos && target[query_or_fragment] != '?') {
                return make_json_response(request, beast_http::status::bad_request,
                                          boost::json::object{
                                              {"error", "invalid_request"},
                                          });
            }

            if (request.method() == beast_http::verb::get) {
                try {
                    [[maybe_unused]] const aistore::metadata::UuidV7 validated_run_id{std::string{remainder}};
                } catch (const std::invalid_argument&) {
                    return make_json_response(request, beast_http::status::bad_request,
                                              boost::json::object{
                                                  {"error", "invalid_request"},
                                              });
                }

                return handle_get_lifecycle_run(request, repository, repository_mutex, remainder);
            }

            return make_method_not_allowed(request, "GET");
        }

        const std::string_view run_id = remainder.substr(0, slash);
        const std::string_view subroute = remainder.substr(slash);

        if (subroute == kDecisionsSuffix) {
            if (query_or_fragment != std::string_view::npos && target[query_or_fragment] != '?') {
                return make_json_response(request, beast_http::status::bad_request,
                                          boost::json::object{
                                              {"error", "invalid_request"},
                                          });
            }

            if (request.method() == beast_http::verb::get) {
                try {
                    [[maybe_unused]] const aistore::metadata::UuidV7 validated_run_id{std::string{run_id}};
                } catch (const std::invalid_argument&) {
                    return make_json_response(request, beast_http::status::bad_request,
                                              boost::json::object{
                                                  {"error", "invalid_request"},
                                              });
                }

                return handle_list_lifecycle_decisions(request, repository, repository_mutex, run_id, query);
            }

            return make_method_not_allowed(request, "GET");
        }

        return std::nullopt;
    }

    if (path.starts_with(kArtifactVersionsPrefix) && path.ends_with(kPinSuffix)) {
        const std::string_view version_id = path.substr(
            kArtifactVersionsPrefix.size(), path.size() - kArtifactVersionsPrefix.size() - kPinSuffix.size());

        if (version_id.empty() || version_id.find('/') != std::string_view::npos) {
            return std::nullopt;
        }

        if (query_or_fragment != std::string_view::npos && target[query_or_fragment] != '?') {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        if (!is_valid_chunk_id(version_id)) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_version_id"},
                                      });
        }

        if (request.method() == beast_http::verb::put) {
            return handle_put_version_pin(request, repository, repository_mutex, version_id);
        }

        if (request.method() == beast_http::verb::delete_) {
            return handle_delete_version_pin(request, repository, repository_mutex, version_id);
        }

        if (request.method() == beast_http::verb::get) {
            return handle_get_version_pin(request, repository, repository_mutex, version_id);
        }

        return make_method_not_allowed(request, "GET, PUT, DELETE");
    }

    return std::nullopt;
}

}  // namespace aistore::service
