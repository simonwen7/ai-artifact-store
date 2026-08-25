#include "aistore/service/metadata_service.hpp"

#include <boost/json.hpp>
#include <cstdint>
#include <mutex>
#include <optional>
#include <pqxx/pqxx>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "aistore/metadata/storage_location.hpp"

namespace aistore::service {

namespace {

namespace beast_http = aistore::http::beast_http;

constexpr std::string_view kChunkRoutePrefix = "/v1/chunks/";
constexpr std::string_view kLocationsSuffix = "/locations";
constexpr std::string_view kLocationsPrefix = "/locations/";
constexpr std::string_view kApplicationJson = "application/json";

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

        aistore::http::HttpResponse response = make_json_response(request, beast_http::status::method_not_allowed,
                                                                  boost::json::object{
                                                                      {"error", "method_not_allowed"},
                                                                  });
        response.set(beast_http::field::allow, "GET");
        return response;
    }

    const std::string_view target = request.target();

    if (!target.starts_with(kChunkRoutePrefix)) {
        return make_json_response(request, beast_http::status::not_found,
                                  boost::json::object{
                                      {"error", "not_found"},
                                  });
    }

    const std::size_t query_or_fragment = target.find_first_of("?#");
    const std::string_view path =
        query_or_fragment == std::string_view::npos ? target : target.substr(0, query_or_fragment);
    const bool has_query_or_fragment = query_or_fragment != std::string_view::npos;

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
            aistore::http::HttpResponse response = make_json_response(request, beast_http::status::method_not_allowed,
                                                                      boost::json::object{
                                                                          {"error", "method_not_allowed"},
                                                                      });
            response.set(beast_http::field::allow, "GET");
            return response;
        }

        std::vector<aistore::metadata::StorageLocation> locations;

        {
            const std::scoped_lock lock{repository_mutex_};
            locations = repository_.get_storage_locations(route.chunk_id);
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
        aistore::http::HttpResponse response = make_json_response(request, beast_http::status::method_not_allowed,
                                                                  boost::json::object{
                                                                      {"error", "method_not_allowed"},
                                                                  });
        response.set(beast_http::field::allow, "PUT");
        return response;
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
        const std::scoped_lock lock{repository_mutex_};
        repository_.register_storage_location(location);
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

}  // namespace aistore::service
