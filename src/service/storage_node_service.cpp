#include "aistore/service/storage_node_service.hpp"

#include <array>
#include <boost/json.hpp>
#include <charconv>
#include <cstddef>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "aistore/hashing/sha256.hpp"

namespace aistore::service {

namespace {

namespace beast_http = aistore::http::beast_http;

constexpr std::string_view kChunkCollectionPath = "/v1/chunks";
constexpr std::string_view kChunkRoutePrefix = "/v1/chunks/";
constexpr std::string_view kOctetStream = "application/octet-stream";
constexpr std::size_t kDefaultChunkListLimit = 128U;

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

[[nodiscard]] std::string digest_to_hex(const aistore::hashing::Sha256::Digest& digest) {
    constexpr std::array<char, 16> kHexDigits{
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    };

    std::string result;
    result.reserve(digest.size() * 2U);

    for (const std::byte byte : digest) {
        const auto value = std::to_integer<unsigned int>(byte);

        result.push_back(kHexDigits[(value >> 4U) & 0x0FU]);
        result.push_back(kHexDigits[value & 0x0FU]);
    }

    return result;
}

[[nodiscard]] std::string sha256_hex(const std::string& body) {
    aistore::hashing::Sha256 hasher;
    hasher.update(std::as_bytes(std::span{body.data(), body.size()}));

    return digest_to_hex(hasher.finalize());
}

[[nodiscard]] std::pair<std::string_view, std::string_view> split_path_and_query(std::string_view target) {
    const std::size_t query_or_fragment = target.find_first_of("?#");

    if (query_or_fragment == std::string_view::npos) {
        return {target, {}};
    }

    return {target.substr(0, query_or_fragment), target.substr(query_or_fragment + 1U)};
}

[[nodiscard]] std::optional<std::size_t> parse_positive_size(std::string_view value) {
    if (value.empty()) {
        return std::nullopt;
    }

    std::size_t parsed = 0;
    const auto [pointer, error_code] = std::from_chars(value.data(), value.data() + value.size(), parsed);

    if (error_code != std::errc{} || pointer != value.data() + value.size()) {
        return std::nullopt;
    }

    return parsed;
}

struct ChunkListQuery {
    std::optional<std::string_view> after;
    std::size_t limit{kDefaultChunkListLimit};
};

[[nodiscard]] std::optional<ChunkListQuery> parse_chunk_list_query(std::string_view query) {
    ChunkListQuery parsed;

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

            if (!parsed_limit.has_value() || *parsed_limit < 1U || *parsed_limit > 256U) {
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

[[nodiscard]] aistore::http::HttpResponse handle_chunk_collection(const aistore::http::HttpRequest& request,
                                                                  storage::LocalChunkStore& chunk_store,
                                                                  std::string_view query) {
    if (request.method() != beast_http::verb::get) {
        aistore::http::HttpResponse response = make_json_response(request, beast_http::status::method_not_allowed,
                                                                  boost::json::object{
                                                                      {"error", "method_not_allowed"},
                                                                  });
        response.set(beast_http::field::allow, "GET");
        return response;
    }

    const std::optional<ChunkListQuery> parsed_query = parse_chunk_list_query(query);

    if (!parsed_query.has_value()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const storage::StoredChunkPage page = chunk_store.list_chunks(parsed_query->after, parsed_query->limit);

    boost::json::array chunk_array;

    for (const storage::StoredChunkInfo& chunk : page.chunks) {
        chunk_array.push_back(boost::json::object{
            {"chunk_id", chunk.chunk_id},
            {"size_bytes", chunk.size_bytes},
        });
    }

    boost::json::value next_after = boost::json::value(nullptr);

    if (page.next_after.has_value()) {
        next_after = *page.next_after;
    }

    return make_json_response(request, beast_http::status::ok,
                              boost::json::object{
                                  {"chunks", std::move(chunk_array)},
                                  {"next_after", std::move(next_after)},
                              });
}

}  // namespace

StorageNodeService::StorageNodeService(storage::LocalChunkStore& chunk_store, std::string node_id)
    : chunk_store_{chunk_store}, node_id_{std::move(node_id)} {
    if (node_id_.empty() || node_id_.size() > 128U) {
        throw std::invalid_argument("storage node ID is invalid");
    }

    for (const char character : node_id_) {
        const bool is_upper = character >= 'A' && character <= 'Z';
        const bool is_lower = character >= 'a' && character <= 'z';
        const bool is_digit = character >= '0' && character <= '9';
        const bool is_allowed_punct = character == '-' || character == '_' || character == '.';

        if (!is_upper && !is_lower && !is_digit && !is_allowed_punct) {
            throw std::invalid_argument("storage node ID is invalid");
        }
    }
}

aistore::http::HttpResponse StorageNodeService::handle_request(const aistore::http::HttpRequest& request) const {
    if (request.target() == "/health") {
        if (request.method() == beast_http::verb::get) {
            return make_json_response(request, beast_http::status::ok,
                                      boost::json::object{
                                          {"status", "ok"},
                                          {"node_id", node_id_},
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
    const auto [path, query] = split_path_and_query(target);

    if (path == kChunkCollectionPath) {
        return handle_chunk_collection(request, chunk_store_, query);
    }

    if (path.starts_with(kChunkRoutePrefix)) {
        const std::string_view chunk_id = path.substr(kChunkRoutePrefix.size());

        if (!is_valid_chunk_id(chunk_id)) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_chunk_id"},
                                      });
        }

        if (request.method() == beast_http::verb::head) {
            if (chunk_store_.contains(chunk_id)) {
                return make_empty_response(request, beast_http::status::ok);
            }

            return make_empty_response(request, beast_http::status::not_found);
        }

        if (request.method() == beast_http::verb::put) {
            if (request[beast_http::field::content_type] != kOctetStream) {
                return make_json_response(request, beast_http::status::unsupported_media_type,
                                          boost::json::object{
                                              {"error", "unsupported_media_type"},
                                          });
            }

            const std::string calculated_hash = sha256_hex(request.body());

            if (calculated_hash != chunk_id) {
                return make_json_response(request, beast_http::status::unprocessable_entity,
                                          boost::json::object{
                                              {"error", "chunk_hash_mismatch"},
                                          });
            }

            chunk_store_.put(chunk_id, std::as_bytes(std::span{request.body().data(), request.body().size()}));

            return make_empty_response(request, beast_http::status::no_content);
        }

        if (request.method() == beast_http::verb::get) {
            if (!chunk_store_.contains(chunk_id)) {
                return make_json_response(request, beast_http::status::not_found,
                                          boost::json::object{
                                              {"error", "chunk_not_found"},
                                          });
            }

            const std::vector<std::byte> bytes = chunk_store_.get(chunk_id);

            aistore::http::HttpResponse response{
                beast_http::status::ok,
                request.version(),
            };
            response.set(beast_http::field::content_type, "application/octet-stream");
            response.body().assign(reinterpret_cast<const char*>(bytes.data()), bytes.size());
            response.prepare_payload();
            return response;
        }

        if (request.method() == beast_http::verb::delete_) {
            const bool deleted = chunk_store_.remove(chunk_id);

            return make_json_response(request, beast_http::status::ok,
                                      boost::json::object{
                                          {"chunk_id", std::string{chunk_id}},
                                          {"deleted", deleted},
                                      });
        }

        aistore::http::HttpResponse response = make_json_response(request, beast_http::status::method_not_allowed,
                                                                  boost::json::object{
                                                                      {"error", "method_not_allowed"},
                                                                  });
        response.set(beast_http::field::allow, "GET, HEAD, PUT, DELETE");
        return response;
    }

    return make_json_response(request, beast_http::status::not_found,
                              boost::json::object{
                                  {"error", "not_found"},
                              });
}

}  // namespace aistore::service
