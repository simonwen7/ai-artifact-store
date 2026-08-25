#include "aistore/service/storage_node_service.hpp"

#include <array>
#include <boost/json.hpp>
#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "aistore/hashing/sha256.hpp"

namespace aistore::service {

namespace {

namespace beast_http = aistore::http::beast_http;

constexpr std::string_view kChunkRoutePrefix = "/v1/chunks/";
constexpr std::string_view kOctetStream = "application/octet-stream";

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

}  // namespace

StorageNodeService::StorageNodeService(storage::LocalChunkStore& chunk_store) : chunk_store_(chunk_store) {}

aistore::http::HttpResponse StorageNodeService::handle_request(const aistore::http::HttpRequest& request) const {
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

    if (target.starts_with(kChunkRoutePrefix)) {
        const std::string_view chunk_id = target.substr(kChunkRoutePrefix.size());

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

        aistore::http::HttpResponse response = make_json_response(request, beast_http::status::method_not_allowed,
                                                                  boost::json::object{
                                                                      {"error", "method_not_allowed"},
                                                                  });
        response.set(beast_http::field::allow, "GET, HEAD, PUT");
        return response;
    }

    return make_json_response(request, beast_http::status::not_found,
                              boost::json::object{
                                  {"error", "not_found"},
                              });
}

}  // namespace aistore::service
