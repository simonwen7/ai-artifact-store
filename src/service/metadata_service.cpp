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
#include "aistore/metadata/finalize_upload.hpp"
#include "aistore/metadata/object.hpp"
#include "aistore/metadata/object_layout.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
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
constexpr std::string_view kApplicationJson = "application/json";
constexpr std::size_t kMaxNegotiationChunks = 256U;
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

[[nodiscard]] std::string_view chunking_strategy_to_string(aistore::metadata::ChunkingStrategy strategy) {
    switch (strategy) {
        case aistore::metadata::ChunkingStrategy::FixedSize:
            return "fixed-size";
    }

    throw std::logic_error("unsupported chunking strategy");
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
        {"chunking_strategy", chunking_strategy_to_string(session.chunking_strategy())},
        {"chunk_size_bytes", session.chunk_size_bytes()},
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

[[nodiscard]] bool creation_fields_match(const aistore::metadata::UploadSession& existing,
                                         const aistore::metadata::UploadSession& requested) {
    return existing.session_id() == requested.session_id() && existing.artifact_id() == requested.artifact_id() &&
           existing.target_node_id() == requested.target_node_id() &&
           existing.chunking_strategy() == requested.chunking_strategy() &&
           existing.chunk_size_bytes() == requested.chunk_size_bytes() &&
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
        !body.contains("target_node_id") || !body.contains("chunking_strategy") || !body.contains("chunk_size_bytes") ||
        !body.contains("parent_version_id") || !body.contains("immutable_metadata")) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    if (!body.at("session_id").is_string() || !body.at("artifact_id").is_string() ||
        !body.at("target_node_id").is_string() || !body.at("chunking_strategy").is_string() ||
        !body.at("immutable_metadata").is_object()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const std::string chunking_strategy{body.at("chunking_strategy").as_string()};

    if (chunking_strategy != "fixed-size") {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const std::optional<std::uint64_t> chunk_size_bytes = extract_positive_uint64(body.at("chunk_size_bytes"));

    if (!chunk_size_bytes.has_value()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
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
        requested.emplace(
            std::move(session_id), std::move(artifact_id), std::string{body.at("target_node_id").as_string()},
            aistore::metadata::ChunkingStrategy::FixedSize, *chunk_size_bytes, std::move(parent_version_id),
            std::move(immutable_metadata), aistore::metadata::UploadSessionState::Open, std::nullopt);
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

    if (body.size() != 4U || !body.contains("object_id") || !body.contains("total_size_bytes") ||
        !body.contains("chunking_strategy") || !body.contains("chunks") || !body.at("object_id").is_string() ||
        !body.at("chunking_strategy").is_string() || !body.at("chunks").is_array()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const std::string object_id{body.at("object_id").as_string()};
    const std::optional<std::uint64_t> total_size_bytes = extract_nonnegative_uint64(body.at("total_size_bytes"));

    if (!is_valid_chunk_id(object_id) || !total_size_bytes.has_value() ||
        body.at("chunking_strategy").as_string() != "fixed-size") {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
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
            .offset = *offset,
            .size = *size_bytes,
        });
    }

    std::optional<aistore::metadata::ObjectLayoutDescriptor> descriptor;

    try {
        descriptor.emplace(aistore::metadata::Object{object_id, *total_size_bytes},
                           aistore::metadata::ChunkingStrategy::FixedSize,
                           aistore::metadata::ObjectLayout{std::move(chunks)});
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

    if (path.starts_with(kChunkRoutePrefix)) {
        return handle_location_routes(request, repository_, repository_mutex_, path, has_query_or_fragment);
    }

    return make_json_response(request, beast_http::status::not_found,
                              boost::json::object{
                                  {"error", "not_found"},
                              });
}

}  // namespace aistore::service
