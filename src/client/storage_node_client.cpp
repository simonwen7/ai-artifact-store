#include "aistore/client/storage_node_client.hpp"

#include <array>
#include <boost/json.hpp>
#include <cstddef>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "aistore/client/client_error.hpp"
#include "aistore/hashing/sha256.hpp"
#include "aistore/storage/local_chunk_store.hpp"

namespace {

namespace beast_http = aistore::http::beast_http;

[[nodiscard]] unsigned int status_code_of(const aistore::http::HttpClientResponse& response) {
    return static_cast<unsigned int>(response.result_int());
}

[[nodiscard]] std::string extract_error_code(const std::string& body) {
    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(body, parse_error);

    if (parse_error || !parsed.is_object() || !parsed.as_object().contains("error") ||
        !parsed.as_object().at("error").is_string()) {
        return "http_error";
    }

    return std::string{parsed.as_object().at("error").as_string()};
}

[[noreturn]] void throw_remote_api_error(const aistore::http::HttpClientResponse& response) {
    throw aistore::client::RemoteApiError{status_code_of(response), extract_error_code(response.body()),
                                          response.body()};
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

[[nodiscard]] std::optional<std::uint64_t> extract_nonnegative_uint64(const boost::json::value& value) {
    if (value.is_double()) {
        return std::nullopt;
    }

    if (value.is_uint64()) {
        return value.as_uint64();
    }

    if (value.is_int64()) {
        const std::int64_t signed_number = value.as_int64();

        if (signed_number < 0) {
            return std::nullopt;
        }

        return static_cast<std::uint64_t>(signed_number);
    }

    return std::nullopt;
}

[[nodiscard]] std::string chunk_target(std::string_view chunk_id) {
    return std::string{"/v1/chunks/"} + std::string{chunk_id};
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

[[nodiscard]] std::string chunk_collection_target(std::optional<std::string_view> after, std::size_t limit) {
    std::string target{"/v1/chunks?limit="};
    target += std::to_string(limit);

    if (after.has_value()) {
        target += "&after=";
        target += std::string{*after};
    }

    return target;
}

[[nodiscard]] aistore::storage::StoredChunkPage parse_chunk_inventory_page(const std::string& body) {
    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(body, parse_error);

    if (parse_error || !parsed.is_object()) {
        throw aistore::client::RemoteProtocolError{"chunk inventory response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (!json_object_has_exact_keys(object, {"chunks", "next_after"})) {
        throw aistore::client::RemoteProtocolError{"chunk inventory response has unexpected fields"};
    }

    if (!object.at("chunks").is_array()) {
        throw aistore::client::RemoteProtocolError{"chunk inventory chunks must be an array"};
    }

    if (!object.at("next_after").is_string() && !object.at("next_after").is_null()) {
        throw aistore::client::RemoteProtocolError{"chunk inventory next_after is invalid"};
    }

    aistore::storage::StoredChunkPage page;

    for (const boost::json::value& entry : object.at("chunks").as_array()) {
        if (!entry.is_object()) {
            throw aistore::client::RemoteProtocolError{"chunk inventory entry is not an object"};
        }

        const boost::json::object& chunk_object = entry.as_object();

        if (!json_object_has_exact_keys(chunk_object, {"chunk_id", "size_bytes"})) {
            throw aistore::client::RemoteProtocolError{"chunk inventory entry has unexpected fields"};
        }

        if (!chunk_object.at("chunk_id").is_string() || !is_valid_chunk_id(chunk_object.at("chunk_id").as_string())) {
            throw aistore::client::RemoteProtocolError{"chunk inventory chunk_id is invalid"};
        }

        const std::optional<std::uint64_t> size_bytes = extract_nonnegative_uint64(chunk_object.at("size_bytes"));

        if (!size_bytes.has_value()) {
            throw aistore::client::RemoteProtocolError{"chunk inventory size_bytes is invalid"};
        }

        page.chunks.push_back(aistore::storage::StoredChunkInfo{
            .chunk_id = std::string{chunk_object.at("chunk_id").as_string()},
            .size_bytes = *size_bytes,
        });
    }

    if (object.at("next_after").is_string()) {
        const std::string_view next_after = object.at("next_after").as_string();

        if (!is_valid_chunk_id(next_after)) {
            throw aistore::client::RemoteProtocolError{"chunk inventory next_after is invalid"};
        }

        page.next_after = std::string{next_after};
    }

    return page;
}

[[nodiscard]] bool parse_delete_chunk_response(std::string_view chunk_id, const std::string& body) {
    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(body, parse_error);

    if (parse_error || !parsed.is_object()) {
        throw aistore::client::RemoteProtocolError{"delete chunk response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (!json_object_has_exact_keys(object, {"chunk_id", "deleted"})) {
        throw aistore::client::RemoteProtocolError{"delete chunk response has unexpected fields"};
    }

    if (!object.at("chunk_id").is_string() || object.at("chunk_id").as_string() != chunk_id) {
        throw aistore::client::RemoteProtocolError{"delete chunk response chunk_id is invalid"};
    }

    if (!object.at("deleted").is_bool()) {
        throw aistore::client::RemoteProtocolError{"delete chunk response deleted flag is invalid"};
    }

    return object.at("deleted").as_bool();
}

}  // namespace

namespace aistore::client {

StorageNodeClient::StorageNodeClient(http::HttpClientConfig config) : http_client_{std::move(config)} {}

bool StorageNodeClient::has_chunk(std::string_view chunk_id) const {
    if (!is_valid_chunk_id(chunk_id)) {
        throw std::invalid_argument("chunk_id must be exactly 64 lowercase hexadecimal characters");
    }

    const aistore::http::HttpClientResponse response =
        http_client_.request(beast_http::verb::head, chunk_target(chunk_id));

    const unsigned int status = status_code_of(response);

    if (status == 200U) {
        return true;
    }

    if (status == 404U) {
        return false;
    }

    throw_remote_api_error(response);
}

void StorageNodeClient::put_chunk(std::string_view chunk_id, std::span<const std::byte> bytes) const {
    if (!is_valid_chunk_id(chunk_id)) {
        throw std::invalid_argument("chunk_id must be exactly 64 lowercase hexadecimal characters");
    }

    std::string body;
    body.resize(bytes.size());

    for (std::size_t index = 0; index < bytes.size(); ++index) {
        body[index] = static_cast<char>(bytes[index]);
    }

    const aistore::http::HttpClientResponse response = http_client_.request(
        beast_http::verb::put, chunk_target(chunk_id), std::move(body), "application/octet-stream");

    if (status_code_of(response) != 204U) {
        throw_remote_api_error(response);
    }
}

std::optional<std::vector<std::byte>> StorageNodeClient::get_chunk(std::string_view chunk_id) const {
    if (!is_valid_chunk_id(chunk_id)) {
        throw std::invalid_argument("chunk_id must be exactly 64 lowercase hexadecimal characters");
    }

    const aistore::http::HttpClientResponse response =
        http_client_.request(beast_http::verb::get, chunk_target(chunk_id));

    const unsigned int status = status_code_of(response);

    if (status == 404U && extract_error_code(response.body()) == "chunk_not_found") {
        return std::nullopt;
    }

    if (status != 200U) {
        throw_remote_api_error(response);
    }

    if (response[beast_http::field::content_type] != "application/octet-stream") {
        throw RemoteProtocolError{"chunk response Content-Type must be application/octet-stream"};
    }

    const std::span<const std::byte> body_bytes{
        reinterpret_cast<const std::byte*>(response.body().data()),
        response.body().size(),
    };

    aistore::hashing::Sha256 hasher;
    hasher.update(body_bytes);

    if (digest_to_hex(hasher.finalize()) != chunk_id) {
        throw RemoteProtocolError{"chunk response digest does not match requested chunk_id"};
    }

    std::vector<std::byte> bytes;
    bytes.resize(response.body().size());

    for (std::size_t index = 0; index < response.body().size(); ++index) {
        bytes[index] = static_cast<std::byte>(static_cast<unsigned char>(response.body()[index]));
    }

    return bytes;
}

storage::StoredChunkPage StorageNodeClient::list_chunks(std::optional<std::string_view> after,
                                                        std::size_t limit) const {
    if (limit < 1U || limit > 256U) {
        throw std::invalid_argument("chunk inventory limit must be between 1 and 256");
    }

    if (after.has_value() && !is_valid_chunk_id(*after)) {
        throw std::invalid_argument("after must be exactly 64 lowercase hexadecimal characters");
    }

    const aistore::http::HttpClientResponse response =
        http_client_.request(beast_http::verb::get, chunk_collection_target(after, limit));

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    if (response[beast_http::field::content_type] != "application/json") {
        throw RemoteProtocolError{"chunk inventory response Content-Type must be application/json"};
    }

    return parse_chunk_inventory_page(response.body());
}

bool StorageNodeClient::delete_chunk(std::string_view chunk_id) const {
    if (!is_valid_chunk_id(chunk_id)) {
        throw std::invalid_argument("chunk_id must be exactly 64 lowercase hexadecimal characters");
    }

    const aistore::http::HttpClientResponse response =
        http_client_.request(beast_http::verb::delete_, chunk_target(chunk_id));

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    if (response[beast_http::field::content_type] != "application/json") {
        throw RemoteProtocolError{"delete chunk response Content-Type must be application/json"};
    }

    return parse_delete_chunk_response(chunk_id, response.body());
}

std::string StorageNodeClient::probe_node_id() const {
    const aistore::http::HttpClientResponse response = http_client_.request(beast_http::verb::get, "/health");

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    if (response[beast_http::field::content_type] != "application/json") {
        throw RemoteProtocolError{"health response Content-Type must be application/json"};
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(response.body(), parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"health response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (!json_object_has_exact_keys(object, {"status", "node_id"})) {
        throw RemoteProtocolError{"health response has unexpected fields"};
    }

    if (!object.at("status").is_string() || object.at("status").as_string() != "ok") {
        throw RemoteProtocolError{"health response status must be ok"};
    }

    if (!object.at("node_id").is_string()) {
        throw RemoteProtocolError{"health response node_id is invalid"};
    }

    const std::string node_id{object.at("node_id").as_string()};

    if (node_id.empty() || node_id.size() > 128U) {
        throw RemoteProtocolError{"health response node_id is invalid"};
    }

    for (const char character : node_id) {
        const bool is_upper = character >= 'A' && character <= 'Z';
        const bool is_lower = character >= 'a' && character <= 'z';
        const bool is_digit = character >= '0' && character <= '9';
        const bool is_allowed_punct = character == '-' || character == '_' || character == '.';

        if (!is_upper && !is_lower && !is_digit && !is_allowed_punct) {
            throw RemoteProtocolError{"health response node_id is invalid"};
        }
    }

    return node_id;
}

}  // namespace aistore::client
