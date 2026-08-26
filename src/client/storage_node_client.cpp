#include "aistore/client/storage_node_client.hpp"

#include <boost/json.hpp>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "aistore/client/client_error.hpp"

namespace aistore::client {

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
    throw RemoteApiError{status_code_of(response), extract_error_code(response.body()), response.body()};
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

[[nodiscard]] std::string chunk_target(std::string_view chunk_id) {
    return std::string{"/v1/chunks/"} + std::string{chunk_id};
}

}  // namespace

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

    std::vector<std::byte> bytes;
    bytes.resize(response.body().size());

    for (std::size_t index = 0; index < response.body().size(); ++index) {
        bytes[index] = static_cast<std::byte>(static_cast<unsigned char>(response.body()[index]));
    }

    return bytes;
}

}  // namespace aistore::client
