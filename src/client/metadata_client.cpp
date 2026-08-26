#include "aistore/client/metadata_client.hpp"

#include <boost/json.hpp>
#include <limits>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include "aistore/client/client_error.hpp"
#include "aistore/metadata/chunking.hpp"
#include "aistore/metadata/object.hpp"
#include "aistore/metadata/object_layout.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/restore_plan.hpp"
#include "aistore/metadata/storage_location.hpp"

namespace aistore::client {

namespace {

namespace beast_http = aistore::http::beast_http;

constexpr std::size_t kMaxNegotiationChunks = 256U;
constexpr std::uint64_t kPostgresBigintMax = static_cast<std::uint64_t>(std::numeric_limits<long long>::max());

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

[[nodiscard]] aistore::metadata::UploadSessionState upload_session_state_from_string(std::string_view state) {
    if (state == "open") {
        return aistore::metadata::UploadSessionState::Open;
    }

    if (state == "committed") {
        return aistore::metadata::UploadSessionState::Committed;
    }

    if (state == "aborted") {
        return aistore::metadata::UploadSessionState::Aborted;
    }

    throw RemoteProtocolError{"upload session state is invalid"};
}

[[nodiscard]] ChunkAvailability chunk_availability_from_string(std::string_view availability) {
    if (availability == "available_on_target") {
        return ChunkAvailability::AvailableOnTarget;
    }

    if (availability == "available_elsewhere") {
        return ChunkAvailability::AvailableElsewhere;
    }

    if (availability == "no_available_location") {
        return ChunkAvailability::NoAvailableLocation;
    }

    throw RemoteProtocolError{"chunk availability is invalid"};
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

[[nodiscard]] aistore::metadata::UploadSession parse_upload_session_json(const std::string& body) {
    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(body, parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"upload session response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (object.size() != 9U || !object.contains("session_id") || !object.contains("artifact_id") ||
        !object.contains("target_node_id") || !object.contains("chunking_strategy") ||
        !object.contains("chunking_parameters") || !object.contains("parent_version_id") ||
        !object.contains("immutable_metadata") || !object.contains("state") ||
        !object.contains("finalized_version_id")) {
        throw RemoteProtocolError{"upload session response has unexpected fields"};
    }

    if (!object.at("session_id").is_string() || !object.at("artifact_id").is_string() ||
        !object.at("target_node_id").is_string() || !object.at("chunking_strategy").is_string() ||
        !object.at("chunking_parameters").is_object() || !object.at("immutable_metadata").is_object() ||
        !object.at("state").is_string()) {
        throw RemoteProtocolError{"upload session response field types are invalid"};
    }

    const std::string chunking_strategy{object.at("chunking_strategy").as_string()};
    const boost::json::object& chunking_parameters = object.at("chunking_parameters").as_object();

    std::optional<std::string> parent_version_id;

    if (object.at("parent_version_id").is_null()) {
        parent_version_id = std::nullopt;
    } else if (object.at("parent_version_id").is_string()) {
        parent_version_id = std::string{object.at("parent_version_id").as_string()};
    } else {
        throw RemoteProtocolError{"upload session parent_version_id is invalid"};
    }

    std::optional<std::string> finalized_version_id;

    if (object.at("finalized_version_id").is_null()) {
        finalized_version_id = std::nullopt;
    } else if (object.at("finalized_version_id").is_string()) {
        finalized_version_id = std::string{object.at("finalized_version_id").as_string()};
    } else {
        throw RemoteProtocolError{"upload session finalized_version_id is invalid"};
    }

    aistore::metadata::UploadSession::ImmutableMetadata immutable_metadata;

    for (const auto& [key, value] : object.at("immutable_metadata").as_object()) {
        if (!value.is_string()) {
            throw RemoteProtocolError{"upload session immutable_metadata values must be strings"};
        }

        immutable_metadata.emplace(std::string{key}, std::string{value.as_string()});
    }

    try {
        aistore::metadata::UuidV7 session_id{std::string{object.at("session_id").as_string()}};
        aistore::metadata::UuidV7 artifact_id{std::string{object.at("artifact_id").as_string()}};
        const aistore::metadata::UploadSessionState state =
            upload_session_state_from_string(std::string{object.at("state").as_string()});

        return [&]() {
            if (chunking_strategy == "fixed-size") {
                if (!json_object_has_exact_keys(chunking_parameters, {"chunk_size_bytes"})) {
                    throw RemoteProtocolError{"upload session chunking_parameters are invalid"};
                }

                const std::optional<std::uint64_t> chunk_size_bytes =
                    extract_positive_uint64(chunking_parameters.at("chunk_size_bytes"));

                if (!chunk_size_bytes.has_value()) {
                    throw RemoteProtocolError{"upload session chunk_size_bytes is invalid"};
                }

                return aistore::metadata::UploadSession{
                    std::move(session_id),
                    std::move(artifact_id),
                    std::string{object.at("target_node_id").as_string()},
                    aistore::metadata::ChunkingStrategy::FixedSize,
                    *chunk_size_bytes,
                    std::move(parent_version_id),
                    std::move(immutable_metadata),
                    state,
                    std::move(finalized_version_id),
                };
            }

            if (chunking_strategy != "fastcdc") {
                throw RemoteProtocolError{"upload session chunking strategy is unsupported"};
            }

            const std::optional<aistore::metadata::FastCdcParameters> fastcdc_parameters =
                parse_fastcdc_parameters_json(chunking_parameters);

            if (!fastcdc_parameters.has_value()) {
                throw RemoteProtocolError{"upload session chunking_parameters are invalid"};
            }

            return aistore::metadata::UploadSession{
                std::move(session_id),
                std::move(artifact_id),
                std::string{object.at("target_node_id").as_string()},
                *fastcdc_parameters,
                std::move(parent_version_id),
                std::move(immutable_metadata),
                state,
                std::move(finalized_version_id),
            };
        }();
    } catch (const RemoteProtocolError&) {
        throw;
    } catch (const std::exception&) {
        throw RemoteProtocolError{"upload session response failed domain validation"};
    }
}

[[nodiscard]] std::string serialize_create_session_request(const aistore::metadata::UploadSession& session) {
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
    };

    if (session.parent_version_id().has_value()) {
        body["parent_version_id"] = *session.parent_version_id();
    } else {
        body["parent_version_id"] = nullptr;
    }

    return boost::json::serialize(body);
}

}  // namespace

MetadataClient::MetadataClient(http::HttpClientConfig config) : http_client_{std::move(config)} {}

aistore::metadata::UploadSession MetadataClient::create_upload_session(
    const aistore::metadata::UploadSession& session) const {
    if (session.state() != aistore::metadata::UploadSessionState::Open || session.finalized_version_id().has_value()) {
        throw std::invalid_argument("create_upload_session requires an open session with null finalized version");
    }

    const aistore::http::HttpClientResponse response = http_client_.request(
        beast_http::verb::post, "/v1/upload-sessions", serialize_create_session_request(session), "application/json");

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    return parse_upload_session_json(response.body());
}

std::optional<aistore::metadata::UploadSession> MetadataClient::get_upload_session(
    const aistore::metadata::UuidV7& session_id) const {
    const std::string target = std::string{"/v1/upload-sessions/"} + session_id.str();
    const aistore::http::HttpClientResponse response = http_client_.request(beast_http::verb::get, target);

    const unsigned int status = status_code_of(response);

    if (status == 200U) {
        return parse_upload_session_json(response.body());
    }

    if (status == 404U && extract_error_code(response.body()) == "upload_session_not_found") {
        return std::nullopt;
    }

    throw_remote_api_error(response);
}

aistore::metadata::UploadSession MetadataClient::abort_upload_session(
    const aistore::metadata::UuidV7& session_id) const {
    const std::string target = std::string{"/v1/upload-sessions/"} + session_id.str() + "/abort";
    const aistore::http::HttpClientResponse response = http_client_.request(beast_http::verb::post, target);

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    return parse_upload_session_json(response.body());
}

aistore::metadata::FinalizeUploadResult MetadataClient::finalize_upload(
    const aistore::metadata::UuidV7& session_id, const aistore::metadata::ObjectLayoutDescriptor& descriptor) const {
    boost::json::array chunks;
    chunks.reserve(descriptor.layout().chunks().size());

    for (const aistore::metadata::ChunkRef& chunk : descriptor.layout().chunks()) {
        chunks.push_back(boost::json::object{
            {"chunk_id", chunk.chunk_id},
            {"offset", chunk.offset},
            {"size_bytes", chunk.size},
        });
    }

    const std::string body = boost::json::serialize(boost::json::object{
        {"object_id", descriptor.object_id()},
        {"total_size_bytes", descriptor.object().total_size()},
        {"chunking_strategy", aistore::metadata::chunking_strategy_to_string(descriptor.chunking_strategy())},
        {"chunking_parameters", chunking_parameters_to_json(descriptor)},
        {"chunks", std::move(chunks)},
    });
    const std::string target = std::string{"/v1/upload-sessions/"} + session_id.str() + "/finalize";
    const aistore::http::HttpClientResponse response =
        http_client_.request(beast_http::verb::post, target, body, "application/json");

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(response.body(), parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"finalize response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (object.size() != 5U || !object.contains("session_id") || !object.contains("version_id") ||
        !object.contains("object_id") || !object.contains("layout_id") || !object.contains("state") ||
        !object.at("session_id").is_string() || !object.at("version_id").is_string() ||
        !object.at("object_id").is_string() || !object.at("layout_id").is_string() || !object.at("state").is_string()) {
        throw RemoteProtocolError{"finalize response has unexpected fields"};
    }

    std::optional<aistore::metadata::UuidV7> response_session_id;

    try {
        response_session_id.emplace(std::string{object.at("session_id").as_string()});
    } catch (const std::invalid_argument&) {
        throw RemoteProtocolError{"finalize response session_id is invalid"};
    }

    if (*response_session_id != session_id) {
        throw RemoteProtocolError{"finalize response session_id does not match request"};
    }

    const std::string version_id{object.at("version_id").as_string()};
    const std::string object_id{object.at("object_id").as_string()};
    const std::string layout_id{object.at("layout_id").as_string()};

    if (!is_valid_chunk_id(version_id) || !is_valid_chunk_id(object_id) || !is_valid_chunk_id(layout_id)) {
        throw RemoteProtocolError{"finalize response contains an invalid content ID"};
    }

    if (object_id != descriptor.object_id()) {
        throw RemoteProtocolError{"finalize response object_id does not match request"};
    }

    if (layout_id != descriptor.layout_id()) {
        throw RemoteProtocolError{"finalize response layout_id does not match request"};
    }

    if (object.at("state").as_string() != "committed") {
        throw RemoteProtocolError{"finalize response state is invalid"};
    }

    return aistore::metadata::FinalizeUploadResult{
        .session_id = *response_session_id,
        .version_id = version_id,
        .object_id = object_id,
        .layout_id = layout_id,
    };
}

aistore::metadata::RestorePlan MetadataClient::get_restore_plan(std::string_view version_id,
                                                                std::string_view source_node_id) const {
    if (!is_valid_chunk_id(version_id)) {
        throw std::invalid_argument("version_id must be 64 lowercase hex characters");
    }

    if (!is_valid_node_id(source_node_id)) {
        throw std::invalid_argument("source_node_id is invalid");
    }

    const std::string target = std::string{"/v1/artifact-versions/"} + std::string{version_id} + "/restore-plan/" +
                               std::string{source_node_id};
    const aistore::http::HttpClientResponse response = http_client_.request(beast_http::verb::get, target);

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(response.body(), parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"restore plan response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (object.size() != 10U || !object.contains("version_id") || !object.contains("artifact_id") ||
        !object.contains("source_node_id") || !object.contains("object_id") || !object.contains("total_size_bytes") ||
        !object.contains("layout_id") || !object.contains("chunking_strategy") ||
        !object.contains("chunking_parameters") || !object.contains("chunk_count") || !object.contains("chunks") ||
        !object.at("version_id").is_string() || !object.at("artifact_id").is_string() ||
        !object.at("source_node_id").is_string() || !object.at("object_id").is_string() ||
        !object.at("layout_id").is_string() || !object.at("chunking_strategy").is_string() ||
        !object.at("chunking_parameters").is_object() || !object.at("chunks").is_array()) {
        throw RemoteProtocolError{"restore plan response has unexpected fields"};
    }

    const std::string response_version_id{object.at("version_id").as_string()};
    const std::string response_source_node_id{object.at("source_node_id").as_string()};

    if (response_version_id != version_id) {
        throw RemoteProtocolError{"restore plan response version_id does not match request"};
    }

    if (response_source_node_id != source_node_id) {
        throw RemoteProtocolError{"restore plan response source_node_id does not match request"};
    }

    const std::string object_id{object.at("object_id").as_string()};
    const std::string layout_id{object.at("layout_id").as_string()};

    if (!is_valid_chunk_id(object_id) || !is_valid_chunk_id(layout_id)) {
        throw RemoteProtocolError{"restore plan response contains an invalid content ID"};
    }

    const std::string chunking_strategy{object.at("chunking_strategy").as_string()};
    const boost::json::object& chunking_parameters = object.at("chunking_parameters").as_object();

    const std::optional<std::uint64_t> total_size_bytes = extract_nonnegative_uint64(object.at("total_size_bytes"));

    if (!total_size_bytes.has_value()) {
        throw RemoteProtocolError{"restore plan total_size_bytes is invalid"};
    }

    const std::optional<std::uint64_t> chunk_count = extract_nonnegative_uint64(object.at("chunk_count"));

    if (!chunk_count.has_value()) {
        throw RemoteProtocolError{"restore plan chunk_count is invalid"};
    }

    const boost::json::array& chunks_array = object.at("chunks").as_array();

    if (*chunk_count != chunks_array.size()) {
        throw RemoteProtocolError{"restore plan chunk_count does not match chunks array size"};
    }

    std::vector<aistore::metadata::ChunkRef> chunk_refs;
    chunk_refs.reserve(chunks_array.size());

    for (const boost::json::value& chunk_value : chunks_array) {
        if (!chunk_value.is_object()) {
            throw RemoteProtocolError{"restore plan chunk entry is not an object"};
        }

        const boost::json::object& chunk_object = chunk_value.as_object();

        if (chunk_object.size() != 3U || !chunk_object.contains("chunk_id") || !chunk_object.contains("offset") ||
            !chunk_object.contains("size_bytes") || !chunk_object.at("chunk_id").is_string()) {
            throw RemoteProtocolError{"restore plan chunk entry has unexpected fields"};
        }

        const std::string chunk_id{chunk_object.at("chunk_id").as_string()};

        if (!is_valid_chunk_id(chunk_id)) {
            throw RemoteProtocolError{"restore plan chunk_id is invalid"};
        }

        const std::optional<std::uint64_t> offset = extract_nonnegative_uint64(chunk_object.at("offset"));

        if (!offset.has_value()) {
            throw RemoteProtocolError{"restore plan chunk offset is invalid"};
        }

        std::optional<std::uint64_t> size_bytes = extract_positive_uint64(chunk_object.at("size_bytes"));

        if (!size_bytes.has_value()) {
            throw RemoteProtocolError{"restore plan chunk size_bytes is invalid"};
        }

        chunk_refs.push_back(aistore::metadata::ChunkRef{
            .chunk_id = chunk_id,
            .offset = *offset,
            .size = *size_bytes,
        });
    }

    aistore::metadata::UuidV7 artifact_id{std::string{object.at("artifact_id").as_string()}};

    try {
        aistore::metadata::Object layout_object{object_id, *total_size_bytes};
        aistore::metadata::ObjectLayout layout{std::move(chunk_refs)};
        aistore::metadata::ObjectLayoutDescriptor descriptor = [&]() {
            if (chunking_strategy == "fixed-size") {
                if (!chunking_parameters.empty()) {
                    throw RemoteProtocolError{"restore plan chunking_parameters are invalid"};
                }

                return aistore::metadata::ObjectLayoutDescriptor{
                    layout_object,
                    aistore::metadata::ChunkingStrategy::FixedSize,
                    layout,
                };
            }

            if (chunking_strategy != "fastcdc") {
                throw RemoteProtocolError{"restore plan chunking strategy is unsupported"};
            }

            const std::optional<aistore::metadata::FastCdcParameters> fastcdc_parameters =
                parse_fastcdc_parameters_json(chunking_parameters);

            if (!fastcdc_parameters.has_value()) {
                throw RemoteProtocolError{"restore plan chunking_parameters are invalid"};
            }

            return aistore::metadata::ObjectLayoutDescriptor{layout_object, *fastcdc_parameters, layout};
        }();

        if (descriptor.object_id() != object_id) {
            throw RemoteProtocolError{"restore plan reconstructed object_id does not match response"};
        }

        if (descriptor.layout_id() != layout_id) {
            throw RemoteProtocolError{"restore plan reconstructed layout_id does not match response"};
        }

        if (descriptor.object().total_size() != *total_size_bytes) {
            throw RemoteProtocolError{"restore plan reconstructed total size does not match response"};
        }

        return aistore::metadata::RestorePlan{
            .artifact_id = artifact_id,
            .version_id = response_version_id,
            .source_node_id = response_source_node_id,
            .layout_descriptor = std::move(descriptor),
        };
    } catch (const RemoteProtocolError&) {
        throw;
    } catch (const std::exception&) {
        throw RemoteProtocolError{"restore plan response failed domain validation"};
    }
}

ChunkNegotiationResult MetadataClient::negotiate_chunks(
    const aistore::metadata::UuidV7& session_id, const std::vector<aistore::metadata::ChunkMetadata>& chunks) const {
    if (chunks.empty() || chunks.size() > kMaxNegotiationChunks) {
        throw std::invalid_argument("negotiate_chunks requires between 1 and 256 chunks");
    }

    std::map<std::string, std::uint64_t> seen_sizes;

    for (const aistore::metadata::ChunkMetadata& chunk : chunks) {
        if (!is_valid_chunk_id(chunk.chunk_id)) {
            throw std::invalid_argument("negotiate_chunks chunk_id must be 64 lowercase hex characters");
        }

        if (chunk.size_bytes == 0U || chunk.size_bytes > kPostgresBigintMax) {
            throw std::invalid_argument("negotiate_chunks size_bytes is out of range");
        }

        const auto [iterator, inserted] = seen_sizes.emplace(chunk.chunk_id, chunk.size_bytes);

        if (!inserted && iterator->second != chunk.size_bytes) {
            throw std::invalid_argument("negotiate_chunks contains conflicting sizes for the same chunk ID");
        }
    }

    boost::json::array chunk_array;

    for (const aistore::metadata::ChunkMetadata& chunk : chunks) {
        chunk_array.push_back(boost::json::object{
            {"chunk_id", chunk.chunk_id},
            {"size_bytes", chunk.size_bytes},
        });
    }

    const std::string body = boost::json::serialize(boost::json::object{
        {"session_id", session_id.str()},
        {"chunks", std::move(chunk_array)},
    });

    const aistore::http::HttpClientResponse response =
        http_client_.request(beast_http::verb::post, "/v1/chunks/negotiate", body, "application/json");

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(response.body(), parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"negotiation response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (object.size() != 3U || !object.contains("session_id") || !object.contains("target_node_id") ||
        !object.contains("chunks") || !object.at("session_id").is_string() ||
        !object.at("target_node_id").is_string() || !object.at("chunks").is_array()) {
        throw RemoteProtocolError{"negotiation response has unexpected fields"};
    }

    std::optional<aistore::metadata::UuidV7> response_session_id;

    try {
        response_session_id.emplace(std::string{object.at("session_id").as_string()});
    } catch (const std::invalid_argument&) {
        throw RemoteProtocolError{"negotiation response session_id is invalid"};
    }

    if (*response_session_id != session_id) {
        throw RemoteProtocolError{"negotiation response session_id does not match request"};
    }

    const std::string target_node_id{object.at("target_node_id").as_string()};

    if (!is_valid_node_id(target_node_id)) {
        throw RemoteProtocolError{"negotiation response target_node_id is invalid"};
    }

    std::vector<NegotiatedChunk> negotiated_chunks;

    for (const boost::json::value& entry_value : object.at("chunks").as_array()) {
        if (!entry_value.is_object()) {
            throw RemoteProtocolError{"negotiation chunk entry is not an object"};
        }

        const boost::json::object& entry = entry_value.as_object();

        if (entry.size() != 5U || !entry.contains("chunk_id") || !entry.contains("size_bytes") ||
            !entry.contains("metadata_was_known") || !entry.contains("availability") ||
            !entry.contains("available_node_ids") || !entry.at("chunk_id").is_string() ||
            !entry.at("metadata_was_known").is_bool() || !entry.at("availability").is_string() ||
            !entry.at("available_node_ids").is_array()) {
            throw RemoteProtocolError{"negotiation chunk entry has unexpected fields"};
        }

        const std::string chunk_id{entry.at("chunk_id").as_string()};

        if (!is_valid_chunk_id(chunk_id)) {
            throw RemoteProtocolError{"negotiation chunk_id is invalid"};
        }

        const std::optional<std::uint64_t> size_bytes = extract_positive_uint64(entry.at("size_bytes"));

        if (!size_bytes.has_value()) {
            throw RemoteProtocolError{"negotiation size_bytes is invalid"};
        }

        std::vector<std::string> available_node_ids;

        for (const boost::json::value& node_value : entry.at("available_node_ids").as_array()) {
            if (!node_value.is_string()) {
                throw RemoteProtocolError{"negotiation available_node_ids entries must be strings"};
            }

            const std::string node_id{node_value.as_string()};

            if (!is_valid_node_id(node_id)) {
                throw RemoteProtocolError{"negotiation available_node_ids contains an invalid node ID"};
            }

            if (!available_node_ids.empty()) {
                if (node_id <= available_node_ids.back()) {
                    throw RemoteProtocolError{"negotiation available_node_ids must be ascending and unique"};
                }
            }

            available_node_ids.push_back(node_id);
        }

        negotiated_chunks.push_back(NegotiatedChunk{
            .chunk =
                aistore::metadata::ChunkMetadata{
                    .chunk_id = chunk_id,
                    .size_bytes = *size_bytes,
                },
            .metadata_was_known = entry.at("metadata_was_known").as_bool(),
            .availability = chunk_availability_from_string(std::string{entry.at("availability").as_string()}),
            .available_node_ids = std::move(available_node_ids),
        });
    }

    return ChunkNegotiationResult{
        .session_id = *response_session_id,
        .target_node_id = target_node_id,
        .chunks = std::move(negotiated_chunks),
    };
}

void MetadataClient::register_storage_location(const aistore::metadata::StorageLocation& location) const {
    if (!is_valid_chunk_id(location.chunk_id)) {
        throw std::invalid_argument("storage location chunk_id must be 64 lowercase hex characters");
    }

    if (!is_valid_node_id(location.node_id)) {
        throw std::invalid_argument("storage location node_id is invalid");
    }

    if (location.storage_path.empty()) {
        throw std::invalid_argument("storage location storage_path must not be empty");
    }

    std::string_view state_string;

    switch (location.state) {
        case aistore::metadata::StorageLocationState::Available:
            state_string = "available";
            break;

        case aistore::metadata::StorageLocationState::Missing:
            state_string = "missing";
            break;

        case aistore::metadata::StorageLocationState::Corrupt:
            state_string = "corrupt";
            break;
    }

    const std::string target = std::string{"/v1/chunks/"} + location.chunk_id + "/locations/" + location.node_id;
    const std::string body = boost::json::serialize(boost::json::object{
        {"storage_path", location.storage_path},
        {"state", state_string},
    });

    const aistore::http::HttpClientResponse response =
        http_client_.request(beast_http::verb::put, target, body, "application/json");

    if (status_code_of(response) != 204U) {
        throw_remote_api_error(response);
    }
}

}  // namespace aistore::client
