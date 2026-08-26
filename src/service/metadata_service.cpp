#include "aistore/service/metadata_service.hpp"

#include <boost/json.hpp>
#include <cstdint>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <pqxx/pqxx>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "aistore/metadata/chunk_metadata.hpp"
#include "aistore/metadata/chunking.hpp"
#include "aistore/metadata/finalize_upload.hpp"
#include "aistore/metadata/gc.hpp"
#include "aistore/metadata/object.hpp"
#include "aistore/metadata/object_layout.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/restore_plan.hpp"
#include "aistore/metadata/storage_location.hpp"
#include "aistore/metadata/upload_session.hpp"
#include "aistore/metadata/uuid_v7.hpp"

namespace aistore::service {

namespace {

namespace beast_http = aistore::http::beast_http;

constexpr std::string_view kChunkRoutePrefix = "/v1/chunks/";
constexpr std::string_view kNegotiatePath = "/v1/chunks/negotiate";
constexpr std::string_view kUploadSessionsCollection = "/v1/upload-sessions";
constexpr std::string_view kUploadSessionsPrefix = "/v1/upload-sessions/";
constexpr std::string_view kAbortSuffix = "/abort";
constexpr std::string_view kFinalizeSuffix = "/finalize";
constexpr std::string_view kLocationsSuffix = "/locations";
constexpr std::string_view kLocationsPrefix = "/locations/";
constexpr std::string_view kArtifactVersionsPrefix = "/v1/artifact-versions/";
constexpr std::string_view kRestorePlanInfix = "/restore-plan/";
constexpr std::string_view kGcRunsCollection = "/v1/gc-runs";
constexpr std::string_view kGcRunsPrefix = "/v1/gc-runs/";
constexpr std::string_view kClassifySuffix = "/classify";
constexpr std::string_view kCompleteSuffix = "/complete";
constexpr std::string_view kApplicationJson = "application/json";
constexpr std::size_t kMaxNegotiationChunks = 256U;
constexpr std::size_t kMaxClassifyChunks = 256U;
constexpr std::uint64_t kPostgresBigintMax = static_cast<std::uint64_t>(std::numeric_limits<long long>::max());

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

aistore::http::HttpResponse make_empty_response(const aistore::http::HttpRequest& request, beast_http::status status) {
    aistore::http::HttpResponse response{
        status,
        request.version(),
    };
    response.body().clear();
    response.prepare_payload();
    return response;
}

aistore::http::HttpResponse make_method_not_allowed(const aistore::http::HttpRequest& request, std::string_view allow) {
    aistore::http::HttpResponse response = make_json_response(request, beast_http::status::method_not_allowed,
                                                              boost::json::object{
                                                                  {"error", "method_not_allowed"},
                                                              });
    response.set(beast_http::field::allow, allow);
    return response;
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

[[nodiscard]] bool is_valid_node_id(std::string_view node_id) {
    if (node_id.empty() || node_id.size() > 128U) {
        return false;
    }

    for (const char character : node_id) {
        const bool is_upper = character >= 'A' && character <= 'Z';
        const bool is_lower = character >= 'a' && character <= 'z';
        const bool is_digit = character >= '0' && character <= '9';
        const bool is_allowed_punct = character == '-' || character == '_' || character == '.';

        if (!is_upper && !is_lower && !is_digit && !is_allowed_punct) {
            return false;
        }
    }

    return true;
}

[[nodiscard]] std::string_view storage_location_state_to_string(aistore::metadata::StorageLocationState state) {
    switch (state) {
        case aistore::metadata::StorageLocationState::Available:
            return "available";

        case aistore::metadata::StorageLocationState::Missing:
            return "missing";

        case aistore::metadata::StorageLocationState::Corrupt:
            return "corrupt";
    }

    throw std::logic_error("unsupported storage location state");
}

[[nodiscard]] std::optional<aistore::metadata::StorageLocationState> storage_location_state_from_string(
    std::string_view state) {
    if (state == "available") {
        return aistore::metadata::StorageLocationState::Available;
    }

    if (state == "missing") {
        return aistore::metadata::StorageLocationState::Missing;
    }

    if (state == "corrupt") {
        return aistore::metadata::StorageLocationState::Corrupt;
    }

    return std::nullopt;
}

[[nodiscard]] std::string_view upload_session_state_to_string(aistore::metadata::UploadSessionState state) {
    switch (state) {
        case aistore::metadata::UploadSessionState::Open:
            return "open";

        case aistore::metadata::UploadSessionState::Committed:
            return "committed";

        case aistore::metadata::UploadSessionState::Aborted:
            return "aborted";
    }

    throw std::logic_error("unsupported upload session state");
}

[[nodiscard]] boost::json::object chunking_parameters_to_json(const aistore::metadata::UploadSession& session) {
    if (session.chunking_strategy() == aistore::metadata::ChunkingStrategy::FixedSize) {
        return boost::json::object{
            {"chunk_size_bytes", session.chunk_size_bytes()},
        };
    }

    const std::optional<aistore::metadata::FastCdcParameters> optional_parameters = session.fastcdc_parameters();

    if (!optional_parameters.has_value()) {
        throw std::logic_error("FastCDC upload session is missing FastCDC parameters");
    }

    const aistore::metadata::FastCdcParameters& parameters = *optional_parameters;

    return boost::json::object{
        {"min_chunk_size_bytes", parameters.min_chunk_size_bytes},
        {"avg_chunk_size_bytes", parameters.avg_chunk_size_bytes},
        {"max_chunk_size_bytes", parameters.max_chunk_size_bytes},
    };
}

[[nodiscard]] boost::json::object chunking_parameters_to_json(
    const aistore::metadata::ObjectLayoutDescriptor& descriptor) {
    if (descriptor.chunking_strategy() == aistore::metadata::ChunkingStrategy::FixedSize) {
        return boost::json::object{};
    }

    const std::optional<aistore::metadata::FastCdcParameters> optional_parameters = descriptor.fastcdc_parameters();

    if (!optional_parameters.has_value()) {
        throw std::logic_error("FastCDC object layout descriptor is missing FastCDC parameters");
    }

    const aistore::metadata::FastCdcParameters& parameters = *optional_parameters;

    return boost::json::object{
        {"min_chunk_size_bytes", parameters.min_chunk_size_bytes},
        {"avg_chunk_size_bytes", parameters.avg_chunk_size_bytes},
        {"max_chunk_size_bytes", parameters.max_chunk_size_bytes},
    };
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

[[nodiscard]] boost::json::object upload_session_to_json(const aistore::metadata::UploadSession& session) {
    boost::json::object metadata_object;

    for (const auto& [key, value] : session.immutable_metadata()) {
        metadata_object[key] = value;
    }

    boost::json::object body{
        {"session_id", session.session_id().str()},
        {"artifact_id", session.artifact_id().str()},
        {"target_node_id", session.target_node_id()},
        {"chunking_strategy", aistore::metadata::chunking_strategy_to_string(session.chunking_strategy())},
        {"chunking_parameters", chunking_parameters_to_json(session)},
        {"immutable_metadata", std::move(metadata_object)},
        {"state", upload_session_state_to_string(session.state())},
    };

    if (session.parent_version_id().has_value()) {
        body["parent_version_id"] = *session.parent_version_id();
    } else {
        body["parent_version_id"] = nullptr;
    }

    if (session.finalized_version_id().has_value()) {
        body["finalized_version_id"] = *session.finalized_version_id();
    } else {
        body["finalized_version_id"] = nullptr;
    }

    return body;
}

[[nodiscard]] std::optional<std::uint64_t> extract_positive_uint64(const boost::json::value& value) {
    if (value.is_double()) {
        return std::nullopt;
    }

    std::uint64_t number = 0;

    if (value.is_uint64()) {
        number = value.as_uint64();
    } else if (value.is_int64()) {
        const std::int64_t signed_number = value.as_int64();

        if (signed_number <= 0) {
            return std::nullopt;
        }

        number = static_cast<std::uint64_t>(signed_number);
    } else {
        return std::nullopt;
    }

    if (number == 0U || number > kPostgresBigintMax) {
        return std::nullopt;
    }

    return number;
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

[[nodiscard]] std::optional<aistore::metadata::FastCdcParameters> parse_fastcdc_parameters_json(
    const boost::json::object& object) {
    if (!json_object_has_exact_keys(object, {"min_chunk_size_bytes", "avg_chunk_size_bytes", "max_chunk_size_bytes"})) {
        return std::nullopt;
    }

    const std::optional<std::uint64_t> min_chunk_size_bytes =
        extract_positive_uint64(object.at("min_chunk_size_bytes"));
    const std::optional<std::uint64_t> avg_chunk_size_bytes =
        extract_positive_uint64(object.at("avg_chunk_size_bytes"));
    const std::optional<std::uint64_t> max_chunk_size_bytes =
        extract_positive_uint64(object.at("max_chunk_size_bytes"));

    if (!min_chunk_size_bytes.has_value() || !avg_chunk_size_bytes.has_value() || !max_chunk_size_bytes.has_value()) {
        return std::nullopt;
    }

    const aistore::metadata::FastCdcParameters parameters{
        .min_chunk_size_bytes = *min_chunk_size_bytes,
        .avg_chunk_size_bytes = *avg_chunk_size_bytes,
        .max_chunk_size_bytes = *max_chunk_size_bytes,
    };

    try {
        aistore::metadata::validate_fastcdc_parameters(parameters);
    } catch (const std::invalid_argument&) {
        return std::nullopt;
    }

    return parameters;
}

[[nodiscard]] bool creation_fields_match(const aistore::metadata::UploadSession& existing,
                                         const aistore::metadata::UploadSession& requested) {
    return existing.session_id() == requested.session_id() && existing.artifact_id() == requested.artifact_id() &&
           existing.target_node_id() == requested.target_node_id() &&
           existing.chunking_strategy() == requested.chunking_strategy() &&
           existing.fixed_chunk_size_bytes() == requested.fixed_chunk_size_bytes() &&
           existing.fastcdc_parameters() == requested.fastcdc_parameters() &&
           existing.parent_version_id() == requested.parent_version_id() &&
           existing.immutable_metadata() == requested.immutable_metadata() &&
           existing.state() == aistore::metadata::UploadSessionState::Open &&
           !existing.finalized_version_id().has_value();
}

enum class LocationRouteKind : std::uint8_t {
    NotLocation,
    Collection,
    Item,
};

struct ParsedLocationRoute {
    LocationRouteKind kind{LocationRouteKind::NotLocation};
    std::string_view chunk_id;
    std::string_view node_id;
    bool invalid_chunk_id{false};
    bool invalid_node_id{false};
};

[[nodiscard]] ParsedLocationRoute parse_location_route(std::string_view path) {
    ParsedLocationRoute parsed;

    if (!path.starts_with(kChunkRoutePrefix)) {
        return parsed;
    }

    const std::string_view after_prefix = path.substr(kChunkRoutePrefix.size());
    const std::size_t slash = after_prefix.find('/');
    const std::string_view chunk_id = slash == std::string_view::npos ? after_prefix : after_prefix.substr(0, slash);

    if (!is_valid_chunk_id(chunk_id)) {
        parsed.invalid_chunk_id = true;
        parsed.chunk_id = chunk_id;
        return parsed;
    }

    parsed.chunk_id = chunk_id;

    if (slash == std::string_view::npos) {
        return parsed;
    }

    const std::string_view remainder = after_prefix.substr(slash);

    if (remainder == kLocationsSuffix) {
        parsed.kind = LocationRouteKind::Collection;
        return parsed;
    }

    if (!remainder.starts_with(kLocationsPrefix)) {
        return parsed;
    }

    const std::string_view node_id = remainder.substr(kLocationsPrefix.size());

    if (node_id.find('/') != std::string_view::npos || !is_valid_node_id(node_id)) {
        parsed.invalid_node_id = true;
        parsed.node_id = node_id;
        return parsed;
    }

    parsed.kind = LocationRouteKind::Item;
    parsed.node_id = node_id;
    return parsed;
}

enum class UploadSessionRouteKind : std::uint8_t {
    NotUploadSession,
    Collection,
    Item,
    Abort,
    Finalize,
    InvalidSessionId,
    UnknownSubroute,
};

struct ParsedUploadSessionRoute {
    UploadSessionRouteKind kind{UploadSessionRouteKind::NotUploadSession};
    std::string_view session_id;
};

[[nodiscard]] ParsedUploadSessionRoute parse_upload_session_route(std::string_view path) {
    ParsedUploadSessionRoute parsed;

    if (path == kUploadSessionsCollection) {
        parsed.kind = UploadSessionRouteKind::Collection;
        return parsed;
    }

    if (!path.starts_with(kUploadSessionsPrefix)) {
        return parsed;
    }

    const std::string_view remainder = path.substr(kUploadSessionsPrefix.size());

    if (remainder.empty() || remainder.starts_with('/')) {
        parsed.kind = UploadSessionRouteKind::UnknownSubroute;
        return parsed;
    }

    const std::size_t slash = remainder.find('/');
    const std::string_view session_id = slash == std::string_view::npos ? remainder : remainder.substr(0, slash);

    try {
        (void)aistore::metadata::UuidV7{std::string{session_id}};
    } catch (const std::invalid_argument&) {
        parsed.kind = UploadSessionRouteKind::InvalidSessionId;
        parsed.session_id = session_id;
        return parsed;
    }

    parsed.session_id = session_id;

    if (slash == std::string_view::npos) {
        parsed.kind = UploadSessionRouteKind::Item;
        return parsed;
    }

    if (remainder.substr(slash) == kAbortSuffix) {
        parsed.kind = UploadSessionRouteKind::Abort;
        return parsed;
    }

    if (remainder.substr(slash) == kFinalizeSuffix) {
        parsed.kind = UploadSessionRouteKind::Finalize;
        return parsed;
    }

    parsed.kind = UploadSessionRouteKind::UnknownSubroute;
    return parsed;
}

enum class RestorePlanRouteKind : std::uint8_t {
    NotRestorePlan,
    Item,
    UnknownSubroute,
};

struct ParsedRestorePlanRoute {
    RestorePlanRouteKind kind{RestorePlanRouteKind::NotRestorePlan};
    std::string_view version_id;
    std::string_view source_node_id;
    bool invalid_version_id{false};
    bool invalid_node_id{false};
};

[[nodiscard]] ParsedRestorePlanRoute parse_restore_plan_route(std::string_view path) {
    ParsedRestorePlanRoute parsed;

    if (!path.starts_with(kArtifactVersionsPrefix)) {
        return parsed;
    }

    const std::string_view after_prefix = path.substr(kArtifactVersionsPrefix.size());
    const std::size_t infix_pos = after_prefix.find(kRestorePlanInfix);

    if (infix_pos == std::string_view::npos) {
        return parsed;
    }

    const std::string_view version_id = after_prefix.substr(0, infix_pos);
    const std::string_view after_infix = after_prefix.substr(infix_pos + kRestorePlanInfix.size());

    if (!is_valid_chunk_id(version_id)) {
        parsed.invalid_version_id = true;
        parsed.version_id = version_id;
        return parsed;
    }

    parsed.version_id = version_id;

    if (after_infix.empty() || after_infix.starts_with('/')) {
        parsed.kind = RestorePlanRouteKind::UnknownSubroute;
        return parsed;
    }

    const std::size_t slash = after_infix.find('/');

    if (slash != std::string_view::npos) {
        parsed.kind = RestorePlanRouteKind::UnknownSubroute;
        return parsed;
    }

    const std::string_view source_node_id = after_infix;

    if (!is_valid_node_id(source_node_id)) {
        parsed.invalid_node_id = true;
        parsed.source_node_id = source_node_id;
        return parsed;
    }

    parsed.kind = RestorePlanRouteKind::Item;
    parsed.source_node_id = source_node_id;
    return parsed;
}

enum class GcRunRouteKind : std::uint8_t {
    NotGcRun,
    Collection,
    Item,
    Classify,
    Complete,
    InvalidGcRunId,
    UnknownSubroute,
};

struct ParsedGcRunRoute {
    GcRunRouteKind kind{GcRunRouteKind::NotGcRun};
    std::string_view gc_run_id;
};

[[nodiscard]] ParsedGcRunRoute parse_gc_run_route(std::string_view path) {
    ParsedGcRunRoute parsed;

    if (path == kGcRunsCollection) {
        parsed.kind = GcRunRouteKind::Collection;
        return parsed;
    }

    if (!path.starts_with(kGcRunsPrefix)) {
        return parsed;
    }

    const std::string_view remainder = path.substr(kGcRunsPrefix.size());

    if (remainder.empty() || remainder.starts_with('/')) {
        parsed.kind = GcRunRouteKind::UnknownSubroute;
        return parsed;
    }

    const std::size_t slash = remainder.find('/');
    const std::string_view gc_run_id = slash == std::string_view::npos ? remainder : remainder.substr(0, slash);

    try {
        (void)aistore::metadata::UuidV7{std::string{gc_run_id}};
    } catch (const std::invalid_argument&) {
        parsed.kind = GcRunRouteKind::InvalidGcRunId;
        parsed.gc_run_id = gc_run_id;
        return parsed;
    }

    parsed.gc_run_id = gc_run_id;

    if (slash == std::string_view::npos) {
        parsed.kind = GcRunRouteKind::Item;
        return parsed;
    }

    if (remainder.substr(slash) == kClassifySuffix) {
        parsed.kind = GcRunRouteKind::Classify;
        return parsed;
    }

    if (remainder.substr(slash) == kCompleteSuffix) {
        parsed.kind = GcRunRouteKind::Complete;
        return parsed;
    }

    parsed.kind = GcRunRouteKind::UnknownSubroute;
    return parsed;
}

[[nodiscard]] boost::json::object gc_run_to_json(const aistore::metadata::GcRun& run) {
    return boost::json::object{
        {"gc_run_id", run.run_id.str()},
        {"target_node_id", run.target_node_id},
        {"dry_run", run.mode == aistore::metadata::GcRunMode::DryRun},
        {"state", aistore::metadata::gc_run_state_to_string(run.state)},
        {"physical_chunks_scanned", run.physical_stats.physical_chunks_scanned},
        {"physical_bytes_scanned", run.physical_stats.physical_bytes_scanned},
        {"collectible_chunks", run.physical_stats.collectible_chunks},
        {"collectible_bytes", run.physical_stats.collectible_bytes},
        {"physically_deleted_chunks", run.physical_stats.physically_deleted_chunks},
        {"physically_deleted_bytes", run.physical_stats.physically_deleted_bytes},
        {"storage_locations_swept", run.metadata_stats.storage_locations_swept},
        {"chunk_rows_swept", run.metadata_stats.chunk_rows_swept},
        {"object_layouts_swept", run.metadata_stats.object_layouts_swept},
        {"objects_swept", run.metadata_stats.objects_swept},
    };
}

[[nodiscard]] aistore::http::HttpResponse make_gc_error_response(const aistore::http::HttpRequest& request,
                                                                 aistore::metadata::GcErrorKind kind) {
    switch (kind) {
        case aistore::metadata::GcErrorKind::RunNotFound:
            return make_json_response(request, beast_http::status::not_found,
                                      boost::json::object{
                                          {"error", "gc_run_not_found"},
                                      });

        case aistore::metadata::GcErrorKind::RunConflict:
            return make_json_response(request, beast_http::status::conflict,
                                      boost::json::object{
                                          {"error", "gc_run_conflict"},
                                      });

        case aistore::metadata::GcErrorKind::AnotherRunOpen:
            return make_json_response(request, beast_http::status::conflict,
                                      boost::json::object{
                                          {"error", "gc_already_in_progress"},
                                      });

        case aistore::metadata::GcErrorKind::OpenUploadSessionsPresent:
            return make_json_response(request, beast_http::status::conflict,
                                      boost::json::object{
                                          {"error", "gc_blocked_by_open_upload_sessions"},
                                      });

        case aistore::metadata::GcErrorKind::RunNotOpen:
            return make_json_response(request, beast_http::status::conflict,
                                      boost::json::object{
                                          {"error", "gc_run_not_open"},
                                      });

        case aistore::metadata::GcErrorKind::GcInProgress:
            return make_json_response(request, beast_http::status::conflict,
                                      boost::json::object{
                                          {"error", "gc_in_progress"},
                                      });
    }

    throw std::logic_error("unsupported GC error kind");
}

[[nodiscard]] std::optional<aistore::metadata::GcPhysicalStats> parse_gc_physical_stats_json(
    const boost::json::object& body) {
    if (!json_object_has_exact_keys(
            body, {"physical_chunks_scanned", "physical_bytes_scanned", "collectible_chunks", "collectible_bytes",
                   "physically_deleted_chunks", "physically_deleted_bytes"})) {
        return std::nullopt;
    }

    const std::optional<std::uint64_t> physical_chunks_scanned =
        extract_nonnegative_uint64(body.at("physical_chunks_scanned"));
    const std::optional<std::uint64_t> physical_bytes_scanned =
        extract_nonnegative_uint64(body.at("physical_bytes_scanned"));
    const std::optional<std::uint64_t> collectible_chunks = extract_nonnegative_uint64(body.at("collectible_chunks"));
    const std::optional<std::uint64_t> collectible_bytes = extract_nonnegative_uint64(body.at("collectible_bytes"));
    const std::optional<std::uint64_t> physically_deleted_chunks =
        extract_nonnegative_uint64(body.at("physically_deleted_chunks"));
    const std::optional<std::uint64_t> physically_deleted_bytes =
        extract_nonnegative_uint64(body.at("physically_deleted_bytes"));

    if (!physical_chunks_scanned.has_value() || !physical_bytes_scanned.has_value() ||
        !collectible_chunks.has_value() || !collectible_bytes.has_value() || !physically_deleted_chunks.has_value() ||
        !physically_deleted_bytes.has_value()) {
        return std::nullopt;
    }

    return aistore::metadata::GcPhysicalStats{
        .physical_chunks_scanned = *physical_chunks_scanned,
        .physical_bytes_scanned = *physical_bytes_scanned,
        .collectible_chunks = *collectible_chunks,
        .collectible_bytes = *collectible_bytes,
        .physically_deleted_chunks = *physically_deleted_chunks,
        .physically_deleted_bytes = *physically_deleted_bytes,
    };
}

[[nodiscard]] aistore::http::HttpResponse handle_start_gc_run(const aistore::http::HttpRequest& request,
                                                              aistore::metadata::PostgresMetadataRepository& repository,
                                                              std::mutex& repository_mutex) {
    if (request[beast_http::field::content_type] != kApplicationJson) {
        return make_json_response(request, beast_http::status::unsupported_media_type,
                                  boost::json::object{
                                      {"error", "unsupported_media_type"},
                                  });
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(request.body(), parse_error);

    if (parse_error) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_json"},
                                  });
    }

    if (!parsed.is_object()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const boost::json::object& body = parsed.as_object();

    if (body.size() != 3U || !body.contains("gc_run_id") || !body.contains("target_node_id") ||
        !body.contains("dry_run") || !body.at("gc_run_id").is_string() || !body.at("target_node_id").is_string() ||
        !body.at("dry_run").is_bool()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const std::string target_node_id{body.at("target_node_id").as_string()};

    if (!is_valid_node_id(target_node_id)) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    try {
        const aistore::metadata::GcRun requested_run{
            .run_id = aistore::metadata::UuidV7{std::string{body.at("gc_run_id").as_string()}},
            .target_node_id = target_node_id,
            .mode = body.at("dry_run").as_bool() ? aistore::metadata::GcRunMode::DryRun
                                                 : aistore::metadata::GcRunMode::Apply,
            .state = aistore::metadata::GcRunState::Open,
        };

        const std::scoped_lock lock{repository_mutex};
        const aistore::metadata::GcRun started = repository.start_gc_run(requested_run);
        return make_json_response(request, beast_http::status::ok, gc_run_to_json(started));
    } catch (const std::invalid_argument&) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    } catch (const aistore::metadata::GcError& error) {
        return make_gc_error_response(request, error.kind());
    }
}

[[nodiscard]] aistore::http::HttpResponse handle_get_gc_run(const aistore::http::HttpRequest& request,
                                                            aistore::metadata::PostgresMetadataRepository& repository,
                                                            std::mutex& repository_mutex,
                                                            std::string_view gc_run_id_text) {
    aistore::metadata::UuidV7 gc_run_id{std::string{gc_run_id_text}};

    const std::scoped_lock lock{repository_mutex};
    const std::optional<aistore::metadata::GcRun> run = repository.get_gc_run(gc_run_id);

    if (!run.has_value()) {
        return make_json_response(request, beast_http::status::not_found,
                                  boost::json::object{
                                      {"error", "gc_run_not_found"},
                                  });
    }

    return make_json_response(request, beast_http::status::ok, gc_run_to_json(*run));
}

[[nodiscard]] aistore::http::HttpResponse handle_classify_gc_chunks(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view gc_run_id_text) {
    if (request[beast_http::field::content_type] != kApplicationJson) {
        return make_json_response(request, beast_http::status::unsupported_media_type,
                                  boost::json::object{
                                      {"error", "unsupported_media_type"},
                                  });
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(request.body(), parse_error);

    if (parse_error) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_json"},
                                  });
    }

    if (!parsed.is_object()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const boost::json::object& body = parsed.as_object();

    if (body.size() != 1U || !body.contains("chunk_ids") || !body.at("chunk_ids").is_array()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const boost::json::array& chunk_id_values = body.at("chunk_ids").as_array();

    if (chunk_id_values.empty() || chunk_id_values.size() > kMaxClassifyChunks) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    std::vector<std::string> chunk_ids;
    chunk_ids.reserve(chunk_id_values.size());
    std::map<std::string, std::size_t> seen_chunk_ids;

    for (const boost::json::value& chunk_id_value : chunk_id_values) {
        if (!chunk_id_value.is_string()) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        const std::string chunk_id{chunk_id_value.as_string()};

        if (!is_valid_chunk_id(chunk_id)) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        if (!seen_chunk_ids.emplace(chunk_id, chunk_ids.size()).second) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        chunk_ids.push_back(chunk_id);
    }

    aistore::metadata::UuidV7 gc_run_id{std::string{gc_run_id_text}};

    try {
        const std::scoped_lock lock{repository_mutex};
        const std::vector<aistore::metadata::GcChunkDecision> decisions =
            repository.classify_gc_chunks(gc_run_id, chunk_ids);

        boost::json::array chunks;
        chunks.reserve(decisions.size());

        for (const aistore::metadata::GcChunkDecision& decision : decisions) {
            chunks.push_back(boost::json::object{
                {"chunk_id", decision.chunk_id},
                {"collectible", decision.collectible},
            });
        }

        return make_json_response(request, beast_http::status::ok,
                                  boost::json::object{
                                      {"gc_run_id", gc_run_id.str()},
                                      {"chunks", std::move(chunks)},
                                  });
    } catch (const aistore::metadata::GcError& error) {
        return make_gc_error_response(request, error.kind());
    } catch (const std::invalid_argument&) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }
}

[[nodiscard]] aistore::http::HttpResponse handle_complete_gc_run(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view gc_run_id_text) {
    if (request[beast_http::field::content_type] != kApplicationJson) {
        return make_json_response(request, beast_http::status::unsupported_media_type,
                                  boost::json::object{
                                      {"error", "unsupported_media_type"},
                                  });
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(request.body(), parse_error);

    if (parse_error) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_json"},
                                  });
    }

    if (!parsed.is_object()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const std::optional<aistore::metadata::GcPhysicalStats> physical_stats =
        parse_gc_physical_stats_json(parsed.as_object());

    if (!physical_stats.has_value()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    aistore::metadata::UuidV7 gc_run_id{std::string{gc_run_id_text}};

    try {
        const std::scoped_lock lock{repository_mutex};
        const aistore::metadata::GcRun completed = repository.complete_gc_run(gc_run_id, *physical_stats);
        return make_json_response(request, beast_http::status::ok, gc_run_to_json(completed));
    } catch (const aistore::metadata::GcError& error) {
        return make_gc_error_response(request, error.kind());
    }
}

[[nodiscard]] boost::json::object restore_plan_to_json(const aistore::metadata::RestorePlan& plan) {
    const aistore::metadata::ObjectLayoutDescriptor& descriptor = plan.layout_descriptor;
    boost::json::array chunks;
    chunks.reserve(descriptor.layout().chunks().size());

    for (const aistore::metadata::ChunkRef& chunk : descriptor.layout().chunks()) {
        chunks.push_back(boost::json::object{
            {"chunk_id", chunk.chunk_id},
            {"offset", chunk.offset},
            {"size_bytes", chunk.size},
        });
    }

    return boost::json::object{
        {"version_id", plan.version_id},
        {"artifact_id", plan.artifact_id.str()},
        {"source_node_id", plan.source_node_id},
        {"object_id", descriptor.object_id()},
        {"total_size_bytes", descriptor.object().total_size()},
        {"layout_id", descriptor.layout_id()},
        {"chunking_strategy", aistore::metadata::chunking_strategy_to_string(descriptor.chunking_strategy())},
        {"chunking_parameters", chunking_parameters_to_json(descriptor)},
        {"chunk_count", chunks.size()},
        {"chunks", std::move(chunks)},
    };
}

[[nodiscard]] aistore::http::HttpResponse handle_get_restore_plan(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view version_id, std::string_view source_node_id) {
    try {
        const std::scoped_lock lock{repository_mutex};
        const aistore::metadata::RestorePlan plan = repository.resolve_restore_plan(version_id, source_node_id);
        return make_json_response(request, beast_http::status::ok, restore_plan_to_json(plan));
    } catch (const aistore::metadata::RestorePlanError& error) {
        switch (error.kind()) {
            case aistore::metadata::RestorePlanErrorKind::VersionNotFound:
                return make_json_response(request, beast_http::status::not_found,
                                          boost::json::object{
                                              {"error", "artifact_version_not_found"},
                                          });

            case aistore::metadata::RestorePlanErrorKind::VersionNotCommitted:
                return make_json_response(request, beast_http::status::conflict,
                                          boost::json::object{
                                              {"error", "artifact_version_not_committed"},
                                          });

            case aistore::metadata::RestorePlanErrorKind::SourceUnavailable:
                return make_json_response(request, beast_http::status::conflict,
                                          boost::json::object{
                                              {"error", "restore_source_unavailable"},
                                          });
        }

        throw std::logic_error("unsupported restore plan error kind");
    }
}

[[nodiscard]] aistore::http::HttpResponse handle_create_upload_session(
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

    if (parse_error) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_json"},
                                  });
    }

    if (!parsed.is_object()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const boost::json::object& body = parsed.as_object();

    if (body.size() != 7U || !body.contains("session_id") || !body.contains("artifact_id") ||
        !body.contains("target_node_id") || !body.contains("chunking_strategy") ||
        !body.contains("chunking_parameters") || !body.contains("parent_version_id") ||
        !body.contains("immutable_metadata")) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    if (!body.at("session_id").is_string() || !body.at("artifact_id").is_string() ||
        !body.at("target_node_id").is_string() || !body.at("chunking_strategy").is_string() ||
        !body.at("chunking_parameters").is_object() || !body.at("immutable_metadata").is_object()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const std::string chunking_strategy{body.at("chunking_strategy").as_string()};
    const boost::json::object& chunking_parameters = body.at("chunking_parameters").as_object();

    std::optional<aistore::metadata::ChunkingStrategy> parsed_strategy;

    if (chunking_strategy == "fixed-size") {
        parsed_strategy = aistore::metadata::ChunkingStrategy::FixedSize;
    } else if (chunking_strategy == "fastcdc") {
        parsed_strategy = aistore::metadata::ChunkingStrategy::FastCdc;
    } else {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    std::optional<std::uint64_t> fixed_chunk_size_bytes;
    std::optional<aistore::metadata::FastCdcParameters> fastcdc_parameters;
    std::uint64_t resolved_fixed_chunk_size_bytes = 0;
    aistore::metadata::FastCdcParameters resolved_fastcdc_parameters{};

    if (*parsed_strategy == aistore::metadata::ChunkingStrategy::FixedSize) {
        if (!json_object_has_exact_keys(chunking_parameters, {"chunk_size_bytes"})) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        fixed_chunk_size_bytes = extract_positive_uint64(chunking_parameters.at("chunk_size_bytes"));

        if (!fixed_chunk_size_bytes.has_value()) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        resolved_fixed_chunk_size_bytes = *fixed_chunk_size_bytes;
    } else {
        fastcdc_parameters = parse_fastcdc_parameters_json(chunking_parameters);

        if (!fastcdc_parameters.has_value()) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        resolved_fastcdc_parameters = *fastcdc_parameters;
    }

    std::optional<std::string> parent_version_id;

    if (body.at("parent_version_id").is_null()) {
        parent_version_id = std::nullopt;
    } else if (body.at("parent_version_id").is_string()) {
        parent_version_id = std::string{body.at("parent_version_id").as_string()};
    } else {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    aistore::metadata::UploadSession::ImmutableMetadata immutable_metadata;

    for (const auto& [key, value] : body.at("immutable_metadata").as_object()) {
        if (!value.is_string()) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        immutable_metadata.emplace(std::string{key}, std::string{value.as_string()});
    }

    std::optional<aistore::metadata::UploadSession> requested;

    try {
        aistore::metadata::UuidV7 session_id{std::string{body.at("session_id").as_string()}};
        aistore::metadata::UuidV7 artifact_id{std::string{body.at("artifact_id").as_string()}};

        if (*parsed_strategy == aistore::metadata::ChunkingStrategy::FixedSize) {
            requested.emplace(std::move(session_id), std::move(artifact_id),
                              std::string{body.at("target_node_id").as_string()},
                              aistore::metadata::ChunkingStrategy::FixedSize, resolved_fixed_chunk_size_bytes,
                              std::move(parent_version_id), std::move(immutable_metadata),
                              aistore::metadata::UploadSessionState::Open, std::nullopt);
        } else {
            requested.emplace(std::move(session_id), std::move(artifact_id),
                              std::string{body.at("target_node_id").as_string()}, resolved_fastcdc_parameters,
                              std::move(parent_version_id), std::move(immutable_metadata),
                              aistore::metadata::UploadSessionState::Open, std::nullopt);
        }
    } catch (const std::invalid_argument&) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    } catch (const std::overflow_error&) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    aistore::metadata::UploadSession response_session = *requested;

    try {
        const std::scoped_lock lock{repository_mutex};
        const std::optional<aistore::metadata::UploadSession> existing =
            repository.get_upload_session(requested->session_id());

        if (existing.has_value()) {
            if (!creation_fields_match(*existing, *requested)) {
                return make_json_response(request, beast_http::status::conflict,
                                          boost::json::object{
                                              {"error", "upload_session_conflict"},
                                          });
            }

            response_session = *existing;
        } else {
            repository.create_upload_session(*requested);
            const std::optional<aistore::metadata::UploadSession> created =
                repository.get_upload_session(requested->session_id());

            if (!created.has_value()) {
                throw std::runtime_error("created upload session could not be reloaded");
            }

            response_session = *created;
        }
    } catch (const pqxx::foreign_key_violation&) {
        return make_json_response(request, beast_http::status::not_found,
                                  boost::json::object{
                                      {"error", "upload_session_prerequisite_not_found"},
                                  });
    } catch (const aistore::metadata::GcError& error) {
        if (error.kind() == aistore::metadata::GcErrorKind::GcInProgress) {
            return make_json_response(request, beast_http::status::conflict,
                                      boost::json::object{
                                          {"error", "gc_in_progress"},
                                      });
        }

        throw;
    }

    return make_json_response(request, beast_http::status::ok, upload_session_to_json(response_session));
}

[[nodiscard]] aistore::http::HttpResponse handle_get_upload_session(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view session_id_text) {
    aistore::metadata::UuidV7 session_id{std::string{session_id_text}};

    std::optional<aistore::metadata::UploadSession> session;

    {
        const std::scoped_lock lock{repository_mutex};
        session = repository.get_upload_session(session_id);
    }

    if (!session.has_value()) {
        return make_json_response(request, beast_http::status::not_found,
                                  boost::json::object{
                                      {"error", "upload_session_not_found"},
                                  });
    }

    return make_json_response(request, beast_http::status::ok, upload_session_to_json(*session));
}

[[nodiscard]] aistore::http::HttpResponse handle_abort_upload_session(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view session_id_text) {
    if (!request.body().empty()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const aistore::metadata::UuidV7 session_id{std::string{session_id_text}};
    std::optional<aistore::metadata::UploadSession> aborted_session;

    {
        const std::scoped_lock lock{repository_mutex};
        const std::optional<aistore::metadata::UploadSession> existing = repository.get_upload_session(session_id);

        if (!existing.has_value()) {
            return make_json_response(request, beast_http::status::not_found,
                                      boost::json::object{
                                          {"error", "upload_session_not_found"},
                                      });
        }

        if (existing->state() == aistore::metadata::UploadSessionState::Committed) {
            return make_json_response(request, beast_http::status::conflict,
                                      boost::json::object{
                                          {"error", "upload_session_committed"},
                                      });
        }

        repository.abort_upload_session(session_id);

        aborted_session = repository.get_upload_session(session_id);

        if (!aborted_session.has_value()) {
            throw std::runtime_error("aborted upload session could not be reloaded");
        }
    }

    return make_json_response(request, beast_http::status::ok, upload_session_to_json(*aborted_session));
}

[[nodiscard]] aistore::http::HttpResponse handle_finalize_upload_session(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view session_id_text) {
    if (request[beast_http::field::content_type] != kApplicationJson) {
        return make_json_response(request, beast_http::status::unsupported_media_type,
                                  boost::json::object{
                                      {"error", "unsupported_media_type"},
                                  });
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(request.body(), parse_error);

    if (parse_error) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_json"},
                                  });
    }

    if (!parsed.is_object()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const boost::json::object& body = parsed.as_object();

    if (body.size() != 5U || !body.contains("object_id") || !body.contains("total_size_bytes") ||
        !body.contains("chunking_strategy") || !body.contains("chunking_parameters") || !body.contains("chunks") ||
        !body.at("object_id").is_string() || !body.at("chunking_strategy").is_string() ||
        !body.at("chunking_parameters").is_object() || !body.at("chunks").is_array()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const std::string object_id{body.at("object_id").as_string()};
    const std::optional<std::uint64_t> total_size_bytes = extract_nonnegative_uint64(body.at("total_size_bytes"));
    const std::string chunking_strategy{body.at("chunking_strategy").as_string()};
    const boost::json::object& chunking_parameters = body.at("chunking_parameters").as_object();

    if (!is_valid_chunk_id(object_id) || !total_size_bytes.has_value()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    std::optional<aistore::metadata::ChunkingStrategy> parsed_strategy;

    if (chunking_strategy == "fixed-size") {
        parsed_strategy = aistore::metadata::ChunkingStrategy::FixedSize;
    } else if (chunking_strategy == "fastcdc") {
        parsed_strategy = aistore::metadata::ChunkingStrategy::FastCdc;
    } else {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    std::optional<aistore::metadata::FastCdcParameters> finalize_fastcdc_parameters;
    aistore::metadata::FastCdcParameters resolved_finalize_fastcdc_parameters{};

    if (*parsed_strategy == aistore::metadata::ChunkingStrategy::FixedSize) {
        if (!chunking_parameters.empty()) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }
    } else {
        finalize_fastcdc_parameters = parse_fastcdc_parameters_json(chunking_parameters);

        if (!finalize_fastcdc_parameters.has_value()) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        resolved_finalize_fastcdc_parameters = *finalize_fastcdc_parameters;
    }

    const boost::json::array& chunks_json = body.at("chunks").as_array();
    std::vector<aistore::metadata::ChunkRef> chunks;
    chunks.reserve(chunks_json.size());

    for (const boost::json::value& entry : chunks_json) {
        if (!entry.is_object()) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        const boost::json::object& chunk_object = entry.as_object();

        if (chunk_object.size() != 3U || !chunk_object.contains("chunk_id") || !chunk_object.contains("offset") ||
            !chunk_object.contains("size_bytes") || !chunk_object.at("chunk_id").is_string()) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        const std::string chunk_id{chunk_object.at("chunk_id").as_string()};
        const std::optional<std::uint64_t> offset = extract_nonnegative_uint64(chunk_object.at("offset"));
        const std::optional<std::uint64_t> size_bytes = extract_positive_uint64(chunk_object.at("size_bytes"));

        if (!is_valid_chunk_id(chunk_id) || !offset.has_value() || !size_bytes.has_value()) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        chunks.push_back(aistore::metadata::ChunkRef{
            .chunk_id = chunk_id,
            .offset = offset.value(),
            .size = size_bytes.value(),
        });
    }

    std::optional<aistore::metadata::ObjectLayoutDescriptor> descriptor;

    try {
        if (*parsed_strategy == aistore::metadata::ChunkingStrategy::FixedSize) {
            descriptor.emplace(aistore::metadata::Object{object_id, total_size_bytes.value()},
                               aistore::metadata::ChunkingStrategy::FixedSize,
                               aistore::metadata::ObjectLayout{std::move(chunks)});
        } else {
            descriptor.emplace(aistore::metadata::Object{object_id, total_size_bytes.value()},
                               resolved_finalize_fastcdc_parameters,
                               aistore::metadata::ObjectLayout{std::move(chunks)});
        }
    } catch (const std::invalid_argument&) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    } catch (const std::overflow_error&) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const aistore::metadata::UuidV7 session_id{std::string{session_id_text}};
    std::optional<aistore::metadata::FinalizeUploadResult> result;

    try {
        const std::scoped_lock lock{repository_mutex};
        result = repository.finalize_upload(session_id, *descriptor);
    } catch (const aistore::metadata::FinalizeUploadError& error) {
        boost::json::object error_body;
        beast_http::status status = beast_http::status::conflict;

        switch (error.kind()) {
            case aistore::metadata::FinalizeUploadErrorKind::SessionNotFound:
                status = beast_http::status::not_found;
                error_body["error"] = "upload_session_not_found";
                break;

            case aistore::metadata::FinalizeUploadErrorKind::SessionNotOpen:
                error_body["error"] = "upload_session_not_open";
                break;

            case aistore::metadata::FinalizeUploadErrorKind::Conflict:
                error_body["error"] = "finalize_conflict";
                break;

            case aistore::metadata::FinalizeUploadErrorKind::ChunkNotAvailableOnTarget:
                error_body["error"] = "chunk_not_available_on_target";

                if (error.chunk_id().has_value()) {
                    error_body["chunk_id"] = *error.chunk_id();
                }
                break;
        }

        return make_json_response(request, status, error_body);
    } catch (const std::invalid_argument&) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    return make_json_response(request, beast_http::status::ok,
                              boost::json::object{
                                  {"session_id", result->session_id.str()},
                                  {"version_id", result->version_id},
                                  {"object_id", result->object_id},
                                  {"layout_id", result->layout_id},
                                  {"state", "committed"},
                              });
}

[[nodiscard]] std::string_view classify_availability(std::string_view target_node_id,
                                                     const std::vector<std::string>& available_node_ids) {
    for (const std::string& node_id : available_node_ids) {
        if (node_id == target_node_id) {
            return "available_on_target";
        }
    }

    if (!available_node_ids.empty()) {
        return "available_elsewhere";
    }

    return "no_available_location";
}

[[nodiscard]] aistore::http::HttpResponse handle_negotiate_chunks(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex) {
    if (request.method() != beast_http::verb::post) {
        return make_method_not_allowed(request, "POST");
    }

    if (request[beast_http::field::content_type] != kApplicationJson) {
        return make_json_response(request, beast_http::status::unsupported_media_type,
                                  boost::json::object{
                                      {"error", "unsupported_media_type"},
                                  });
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(request.body(), parse_error);

    if (parse_error) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_json"},
                                  });
    }

    if (!parsed.is_object()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const boost::json::object& body = parsed.as_object();

    if (body.size() != 2U || !body.contains("session_id") || !body.contains("chunks") ||
        !body.at("session_id").is_string() || !body.at("chunks").is_array()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    std::optional<aistore::metadata::UuidV7> session_id;

    try {
        session_id.emplace(std::string{body.at("session_id").as_string()});
    } catch (const std::invalid_argument&) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_session_id"},
                                  });
    }

    const boost::json::array& chunks_json = body.at("chunks").as_array();

    if (chunks_json.empty() || chunks_json.size() > kMaxNegotiationChunks) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    std::vector<aistore::metadata::ChunkMetadata> chunks;
    chunks.reserve(chunks_json.size());
    std::map<std::string, std::uint64_t> seen_sizes;

    for (const boost::json::value& entry : chunks_json) {
        if (!entry.is_object()) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        const boost::json::object& chunk_object = entry.as_object();

        if (chunk_object.size() != 2U || !chunk_object.contains("chunk_id") || !chunk_object.contains("size_bytes") ||
            !chunk_object.at("chunk_id").is_string()) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        const std::string chunk_id{chunk_object.at("chunk_id").as_string()};

        if (!is_valid_chunk_id(chunk_id)) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        const std::optional<std::uint64_t> size_bytes = extract_positive_uint64(chunk_object.at("size_bytes"));

        if (!size_bytes.has_value()) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        const auto [iterator, inserted] = seen_sizes.emplace(chunk_id, *size_bytes);

        if (!inserted && iterator->second != *size_bytes) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        chunks.push_back(aistore::metadata::ChunkMetadata{
            .chunk_id = chunk_id,
            .size_bytes = *size_bytes,
        });
    }

    std::string target_node_id;
    aistore::metadata::ChunkNegotiationBatch batch;

    {
        const std::scoped_lock lock{repository_mutex};
        const std::optional<aistore::metadata::UploadSession> session = repository.get_upload_session(*session_id);

        if (!session.has_value()) {
            return make_json_response(request, beast_http::status::not_found,
                                      boost::json::object{
                                          {"error", "upload_session_not_found"},
                                      });
        }

        if (session->state() != aistore::metadata::UploadSessionState::Open) {
            return make_json_response(request, beast_http::status::conflict,
                                      boost::json::object{
                                          {"error", "upload_session_not_open"},
                                      });
        }

        target_node_id = session->target_node_id();
        batch = repository.negotiate_chunks(chunks);
    }

    if (batch.conflict.has_value()) {
        return make_json_response(request, beast_http::status::conflict,
                                  boost::json::object{
                                      {"error", "chunk_size_conflict"},
                                      {"chunk_id", batch.conflict->chunk_id},
                                      {"requested_size_bytes", batch.conflict->requested_size_bytes},
                                      {"stored_size_bytes", batch.conflict->stored_size_bytes},
                                  });
    }

    boost::json::array chunk_array;

    for (const aistore::metadata::ChunkNegotiationEntry& entry : batch.chunks) {
        boost::json::array available_nodes;

        for (const std::string& node_id : entry.available_node_ids) {
            available_nodes.push_back(boost::json::value{node_id});
        }

        chunk_array.push_back(boost::json::object{
            {"chunk_id", entry.chunk.chunk_id},
            {"size_bytes", entry.chunk.size_bytes},
            {"metadata_was_known", entry.metadata_was_known},
            {"availability", classify_availability(target_node_id, entry.available_node_ids)},
            {"available_node_ids", std::move(available_nodes)},
        });
    }

    return make_json_response(request, beast_http::status::ok,
                              boost::json::object{
                                  {"session_id", session_id->str()},
                                  {"target_node_id", target_node_id},
                                  {"chunks", std::move(chunk_array)},
                              });
}

[[nodiscard]] aistore::http::HttpResponse handle_location_routes(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view path, bool has_query_or_fragment) {
    const ParsedLocationRoute route = parse_location_route(path);

    if (route.invalid_chunk_id) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_chunk_id"},
                                  });
    }

    if (route.invalid_node_id) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_node_id"},
                                  });
    }

    if (route.kind == LocationRouteKind::NotLocation) {
        return make_json_response(request, beast_http::status::not_found,
                                  boost::json::object{
                                      {"error", "not_found"},
                                  });
    }

    if (has_query_or_fragment) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    if (route.kind == LocationRouteKind::Collection) {
        if (request.method() != beast_http::verb::get) {
            return make_method_not_allowed(request, "GET");
        }

        std::vector<aistore::metadata::StorageLocation> locations;

        {
            const std::scoped_lock lock{repository_mutex};
            locations = repository.get_storage_locations(route.chunk_id);
        }

        boost::json::array location_array;

        for (const aistore::metadata::StorageLocation& location : locations) {
            location_array.push_back(boost::json::object{
                {"node_id", location.node_id},
                {"storage_path", location.storage_path},
                {"state", storage_location_state_to_string(location.state)},
            });
        }

        return make_json_response(request, beast_http::status::ok,
                                  boost::json::object{
                                      {"chunk_id", route.chunk_id},
                                      {"locations", std::move(location_array)},
                                  });
    }

    if (request.method() != beast_http::verb::put) {
        return make_method_not_allowed(request, "PUT");
    }

    if (request[beast_http::field::content_type] != kApplicationJson) {
        return make_json_response(request, beast_http::status::unsupported_media_type,
                                  boost::json::object{
                                      {"error", "unsupported_media_type"},
                                  });
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(request.body(), parse_error);

    if (parse_error) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_json"},
                                  });
    }

    if (!parsed.is_object()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const boost::json::object& body = parsed.as_object();

    if (body.size() != 2U || !body.contains("storage_path") || !body.contains("state") ||
        !body.at("storage_path").is_string() || !body.at("state").is_string()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const std::string storage_path{body.at("storage_path").as_string()};
    const std::string state_string{body.at("state").as_string()};

    if (storage_path.empty()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const std::optional<aistore::metadata::StorageLocationState> state =
        storage_location_state_from_string(state_string);

    if (!state.has_value()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_state"},
                                  });
    }

    const aistore::metadata::StorageLocation location{
        .chunk_id = std::string{route.chunk_id},
        .node_id = std::string{route.node_id},
        .storage_path = storage_path,
        .state = *state,
    };

    try {
        const std::scoped_lock lock{repository_mutex};
        repository.register_storage_location(location);
    } catch (const pqxx::foreign_key_violation&) {
        return make_json_response(request, beast_http::status::not_found,
                                  boost::json::object{
                                      {"error", "chunk_not_found"},
                                  });
    } catch (const pqxx::unique_violation&) {
        return make_json_response(request, beast_http::status::conflict,
                                  boost::json::object{
                                      {"error", "location_conflict"},
                                  });
    }

    return make_empty_response(request, beast_http::status::no_content);
}

}  // namespace

MetadataService::MetadataService(metadata::PostgresMetadataRepository& repository) : repository_(repository) {}

aistore::http::HttpResponse MetadataService::handle_request(const aistore::http::HttpRequest& request) const {
    if (request.target() == "/health") {
        if (request.method() == beast_http::verb::get) {
            return make_json_response(request, beast_http::status::ok,
                                      boost::json::object{
                                          {"status", "ok"},
                                      });
        }

        return make_method_not_allowed(request, "GET");
    }

    const std::string_view target = request.target();
    const std::size_t query_or_fragment = target.find_first_of("?#");
    const std::string_view path =
        query_or_fragment == std::string_view::npos ? target : target.substr(0, query_or_fragment);
    const bool has_query_or_fragment = query_or_fragment != std::string_view::npos;

    if (path == kUploadSessionsCollection || path.starts_with(kUploadSessionsPrefix)) {
        if (has_query_or_fragment) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        const ParsedUploadSessionRoute route = parse_upload_session_route(path);

        switch (route.kind) {
            case UploadSessionRouteKind::Collection:
                if (request.method() != beast_http::verb::post) {
                    return make_method_not_allowed(request, "POST");
                }

                return handle_create_upload_session(request, repository_, repository_mutex_);

            case UploadSessionRouteKind::Item:
                if (request.method() != beast_http::verb::get) {
                    return make_method_not_allowed(request, "GET");
                }

                return handle_get_upload_session(request, repository_, repository_mutex_, route.session_id);

            case UploadSessionRouteKind::Abort:
                if (request.method() != beast_http::verb::post) {
                    return make_method_not_allowed(request, "POST");
                }

                return handle_abort_upload_session(request, repository_, repository_mutex_, route.session_id);

            case UploadSessionRouteKind::Finalize:
                if (request.method() != beast_http::verb::post) {
                    return make_method_not_allowed(request, "POST");
                }

                return handle_finalize_upload_session(request, repository_, repository_mutex_, route.session_id);

            case UploadSessionRouteKind::InvalidSessionId:
                return make_json_response(request, beast_http::status::bad_request,
                                          boost::json::object{
                                              {"error", "invalid_session_id"},
                                          });

            case UploadSessionRouteKind::UnknownSubroute:
                return make_json_response(request, beast_http::status::not_found,
                                          boost::json::object{
                                              {"error", "not_found"},
                                          });

            case UploadSessionRouteKind::NotUploadSession:
                break;
        }
    }

    if (path == kNegotiatePath) {
        if (has_query_or_fragment) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        return handle_negotiate_chunks(request, repository_, repository_mutex_);
    }

    if (path.starts_with(kArtifactVersionsPrefix)) {
        const ParsedRestorePlanRoute route = parse_restore_plan_route(path);

        if (route.invalid_version_id) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_version_id"},
                                      });
        }

        if (route.invalid_node_id) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_node_id"},
                                      });
        }

        if (route.kind == RestorePlanRouteKind::UnknownSubroute) {
            return make_json_response(request, beast_http::status::not_found,
                                      boost::json::object{
                                          {"error", "not_found"},
                                      });
        }

        if (route.kind == RestorePlanRouteKind::Item) {
            if (has_query_or_fragment) {
                return make_json_response(request, beast_http::status::bad_request,
                                          boost::json::object{
                                              {"error", "invalid_request"},
                                          });
            }

            if (request.method() != beast_http::verb::get) {
                return make_method_not_allowed(request, "GET");
            }

            return handle_get_restore_plan(request, repository_, repository_mutex_, route.version_id,
                                           route.source_node_id);
        }

        return make_json_response(request, beast_http::status::not_found,
                                  boost::json::object{
                                      {"error", "not_found"},
                                  });
    }

    if (path.starts_with(kChunkRoutePrefix)) {
        return handle_location_routes(request, repository_, repository_mutex_, path, has_query_or_fragment);
    }

    if (path == kGcRunsCollection || path.starts_with(kGcRunsPrefix)) {
        if (has_query_or_fragment) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        const ParsedGcRunRoute route = parse_gc_run_route(path);

        switch (route.kind) {
            case GcRunRouteKind::Collection:
                if (request.method() != beast_http::verb::post) {
                    return make_method_not_allowed(request, "POST");
                }

                return handle_start_gc_run(request, repository_, repository_mutex_);

            case GcRunRouteKind::Item:
                if (request.method() != beast_http::verb::get) {
                    return make_method_not_allowed(request, "GET");
                }

                return handle_get_gc_run(request, repository_, repository_mutex_, route.gc_run_id);

            case GcRunRouteKind::Classify:
                if (request.method() != beast_http::verb::post) {
                    return make_method_not_allowed(request, "POST");
                }

                return handle_classify_gc_chunks(request, repository_, repository_mutex_, route.gc_run_id);

            case GcRunRouteKind::Complete:
                if (request.method() != beast_http::verb::post) {
                    return make_method_not_allowed(request, "POST");
                }

                return handle_complete_gc_run(request, repository_, repository_mutex_, route.gc_run_id);

            case GcRunRouteKind::InvalidGcRunId:
                return make_json_response(request, beast_http::status::bad_request,
                                          boost::json::object{
                                              {"error", "invalid_gc_run_id"},
                                          });

            case GcRunRouteKind::UnknownSubroute:
                return make_json_response(request, beast_http::status::not_found,
                                          boost::json::object{
                                              {"error", "not_found"},
                                          });

            case GcRunRouteKind::NotGcRun:
                break;
        }
    }

    return make_json_response(request, beast_http::status::not_found,
                              boost::json::object{
                                  {"error", "not_found"},
                              });
}

}  // namespace aistore::service
