#include "aistore/client/metadata_client.hpp"

#include <array>
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
#include "aistore/metadata/gc.hpp"
#include "aistore/metadata/lifecycle.hpp"
#include "aistore/metadata/object.hpp"
#include "aistore/metadata/object_layout.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/replication.hpp"
#include "aistore/metadata/restore_plan.hpp"
#include "aistore/metadata/storage_node.hpp"

namespace aistore::client {

namespace {

namespace beast_http = aistore::http::beast_http;

constexpr std::size_t kMaxNegotiationChunks = 256U;
constexpr std::size_t kExpectedLifecycleRuleCount = 5U;
constexpr std::uint64_t kPostgresBigintMax = static_cast<std::uint64_t>(std::numeric_limits<long long>::max());

constexpr std::array<aistore::metadata::ArtifactKind, kExpectedLifecycleRuleCount> kCanonicalArtifactKindOrder = {
    aistore::metadata::ArtifactKind::Generic,          aistore::metadata::ArtifactKind::ModelCheckpoint,
    aistore::metadata::ArtifactKind::DatasetSnapshot,  aistore::metadata::ArtifactKind::EmbeddingIndex,
    aistore::metadata::ArtifactKind::EvaluationOutput,
};

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

    if (object.size() != 11U || !object.contains("session_id") || !object.contains("artifact_id") ||
        !object.contains("target_node_id") || !object.contains("replication_factor") ||
        !object.contains("placement_node_ids") || !object.contains("chunking_strategy") ||
        !object.contains("chunking_parameters") || !object.contains("parent_version_id") ||
        !object.contains("immutable_metadata") || !object.contains("state") ||
        !object.contains("finalized_version_id")) {
        throw RemoteProtocolError{"upload session response has unexpected fields"};
    }

    if (!object.at("session_id").is_string() || !object.at("artifact_id").is_string() ||
        !object.at("target_node_id").is_string() || !object.at("placement_node_ids").is_array() ||
        !object.at("chunking_strategy").is_string() || !object.at("chunking_parameters").is_object() ||
        !object.at("immutable_metadata").is_object() || !object.at("state").is_string()) {
        throw RemoteProtocolError{"upload session response field types are invalid"};
    }

    const std::optional<std::uint64_t> optional_replication_factor =
        extract_positive_uint64(object.at("replication_factor"));

    if (!optional_replication_factor.has_value() || *optional_replication_factor > 8U) {
        throw RemoteProtocolError{"upload session replication_factor is invalid"};
    }

    const auto replication_factor = static_cast<std::uint8_t>(*optional_replication_factor);

    std::vector<std::string> placement_node_ids;
    placement_node_ids.reserve(object.at("placement_node_ids").as_array().size());

    for (const boost::json::value& node_value : object.at("placement_node_ids").as_array()) {
        if (!node_value.is_string()) {
            throw RemoteProtocolError{"upload session placement_node_ids must be strings"};
        }

        placement_node_ids.emplace_back(node_value.as_string());
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
                    replication_factor,
                    std::move(placement_node_ids),
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
                replication_factor,
                std::move(placement_node_ids),
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

    boost::json::array placement_array;
    placement_array.reserve(session.placement_node_ids().size());

    for (const std::string& node_id : session.placement_node_ids()) {
        placement_array.push_back(boost::json::value(node_id));
    }

    boost::json::object body{
        {"session_id", session.session_id().str()},
        {"artifact_id", session.artifact_id().str()},
        {"target_node_id", session.target_node_id()},
        {"replication_factor", static_cast<std::uint64_t>(session.replication_factor())},
        {"placement_node_ids", std::move(placement_array)},
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

[[nodiscard]] aistore::metadata::GcRunState gc_run_state_from_string_strict(std::string_view state) {
    if (state == "open") {
        return aistore::metadata::GcRunState::Open;
    }

    if (state == "completed") {
        return aistore::metadata::GcRunState::Completed;
    }

    throw RemoteProtocolError{"GC run state is invalid"};
}

[[nodiscard]] aistore::metadata::GcRun parse_gc_run_json(const std::string& body) {
    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(body, parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"GC run response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (!json_object_has_exact_keys(
            object,
            {"gc_run_id", "target_node_id", "dry_run", "state", "physical_chunks_scanned", "physical_bytes_scanned",
             "collectible_chunks", "collectible_bytes", "physically_deleted_chunks", "physically_deleted_bytes",
             "storage_locations_swept", "chunk_rows_swept", "object_layouts_swept", "objects_swept"})) {
        throw RemoteProtocolError{"GC run response has unexpected fields"};
    }

    if (!object.at("gc_run_id").is_string() || !object.at("target_node_id").is_string() ||
        !object.at("dry_run").is_bool() || !object.at("state").is_string()) {
        throw RemoteProtocolError{"GC run response field types are invalid"};
    }

    const std::string target_node_id{object.at("target_node_id").as_string()};

    if (!is_valid_node_id(target_node_id)) {
        throw RemoteProtocolError{"GC run target_node_id is invalid"};
    }

    const std::optional<std::uint64_t> physical_chunks_scanned =
        extract_nonnegative_uint64(object.at("physical_chunks_scanned"));
    const std::optional<std::uint64_t> physical_bytes_scanned =
        extract_nonnegative_uint64(object.at("physical_bytes_scanned"));
    const std::optional<std::uint64_t> collectible_chunks = extract_nonnegative_uint64(object.at("collectible_chunks"));
    const std::optional<std::uint64_t> collectible_bytes = extract_nonnegative_uint64(object.at("collectible_bytes"));
    const std::optional<std::uint64_t> physically_deleted_chunks =
        extract_nonnegative_uint64(object.at("physically_deleted_chunks"));
    const std::optional<std::uint64_t> physically_deleted_bytes =
        extract_nonnegative_uint64(object.at("physically_deleted_bytes"));
    const std::optional<std::uint64_t> storage_locations_swept =
        extract_nonnegative_uint64(object.at("storage_locations_swept"));
    const std::optional<std::uint64_t> chunk_rows_swept = extract_nonnegative_uint64(object.at("chunk_rows_swept"));
    const std::optional<std::uint64_t> object_layouts_swept =
        extract_nonnegative_uint64(object.at("object_layouts_swept"));
    const std::optional<std::uint64_t> objects_swept = extract_nonnegative_uint64(object.at("objects_swept"));

    if (!physical_chunks_scanned.has_value() || !physical_bytes_scanned.has_value() ||
        !collectible_chunks.has_value() || !collectible_bytes.has_value() || !physically_deleted_chunks.has_value() ||
        !physically_deleted_bytes.has_value() || !storage_locations_swept.has_value() ||
        !chunk_rows_swept.has_value() || !object_layouts_swept.has_value() || !objects_swept.has_value()) {
        throw RemoteProtocolError{"GC run stats are invalid"};
    }

    try {
        return aistore::metadata::GcRun{
            .run_id = aistore::metadata::UuidV7{std::string{object.at("gc_run_id").as_string()}},
            .target_node_id = target_node_id,
            .mode = object.at("dry_run").as_bool() ? aistore::metadata::GcRunMode::DryRun
                                                   : aistore::metadata::GcRunMode::Apply,
            .state = gc_run_state_from_string_strict(std::string{object.at("state").as_string()}),
            .physical_stats =
                aistore::metadata::GcPhysicalStats{
                    .physical_chunks_scanned = *physical_chunks_scanned,
                    .physical_bytes_scanned = *physical_bytes_scanned,
                    .collectible_chunks = *collectible_chunks,
                    .collectible_bytes = *collectible_bytes,
                    .physically_deleted_chunks = *physically_deleted_chunks,
                    .physically_deleted_bytes = *physically_deleted_bytes,
                },
            .metadata_stats =
                aistore::metadata::GcMetadataStats{
                    .storage_locations_swept = *storage_locations_swept,
                    .chunk_rows_swept = *chunk_rows_swept,
                    .object_layouts_swept = *object_layouts_swept,
                    .objects_swept = *objects_swept,
                },
        };
    } catch (const RemoteProtocolError&) {
        throw;
    } catch (const std::exception&) {
        throw RemoteProtocolError{"GC run response failed domain validation"};
    }
}

void validate_gc_physical_stats(const aistore::metadata::GcPhysicalStats& physical_stats) {
    const auto validate = [](std::uint64_t value, std::string_view field_name) {
        if (value > kPostgresBigintMax) {
            throw std::invalid_argument(std::string{field_name} + " is out of range");
        }
    };

    validate(physical_stats.physical_chunks_scanned, "physical_chunks_scanned");
    validate(physical_stats.physical_bytes_scanned, "physical_bytes_scanned");
    validate(physical_stats.collectible_chunks, "collectible_chunks");
    validate(physical_stats.collectible_bytes, "collectible_bytes");
    validate(physical_stats.physically_deleted_chunks, "physically_deleted_chunks");
    validate(physical_stats.physically_deleted_bytes, "physically_deleted_bytes");
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

aistore::metadata::GcRun MetadataClient::start_gc_run(const aistore::metadata::UuidV7& gc_run_id,
                                                      std::string_view target_node_id, bool dry_run) const {
    if (!is_valid_node_id(target_node_id)) {
        throw std::invalid_argument("start_gc_run target_node_id is invalid");
    }

    const std::string body = boost::json::serialize(boost::json::object{
        {"gc_run_id", gc_run_id.str()},
        {"target_node_id", std::string{target_node_id}},
        {"dry_run", dry_run},
    });

    const aistore::http::HttpClientResponse response =
        http_client_.request(beast_http::verb::post, "/v1/gc-runs", body, "application/json");

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    aistore::metadata::GcRun run = parse_gc_run_json(response.body());

    if (run.run_id != gc_run_id) {
        throw RemoteProtocolError{"GC run response gc_run_id does not match request"};
    }

    if (run.target_node_id != target_node_id) {
        throw RemoteProtocolError{"GC run response target_node_id does not match request"};
    }

    const aistore::metadata::GcRunMode expected_mode =
        dry_run ? aistore::metadata::GcRunMode::DryRun : aistore::metadata::GcRunMode::Apply;

    if (run.mode != expected_mode) {
        throw RemoteProtocolError{"GC run response dry_run does not match request"};
    }

    return run;
}

std::optional<aistore::metadata::GcRun> MetadataClient::get_gc_run(const aistore::metadata::UuidV7& gc_run_id) const {
    const std::string target = std::string{"/v1/gc-runs/"} + gc_run_id.str();
    const aistore::http::HttpClientResponse response = http_client_.request(beast_http::verb::get, target);

    const unsigned int status = status_code_of(response);

    if (status == 200U) {
        aistore::metadata::GcRun run = parse_gc_run_json(response.body());

        if (run.run_id != gc_run_id) {
            throw RemoteProtocolError{"GC run response gc_run_id does not match request"};
        }

        return run;
    }

    if (status == 404U && extract_error_code(response.body()) == "gc_run_not_found") {
        return std::nullopt;
    }

    throw_remote_api_error(response);
}

std::vector<aistore::metadata::GcChunkDecision> MetadataClient::classify_gc_chunks(
    const aistore::metadata::UuidV7& gc_run_id, const std::vector<std::string>& chunk_ids) const {
    if (chunk_ids.empty() || chunk_ids.size() > kMaxNegotiationChunks) {
        throw std::invalid_argument("classify_gc_chunks requires between 1 and 256 chunk IDs");
    }

    std::map<std::string, std::size_t> seen_chunk_ids;

    for (const std::string& chunk_id : chunk_ids) {
        if (!is_valid_chunk_id(chunk_id)) {
            throw std::invalid_argument("classify_gc_chunks chunk_id must be 64 lowercase hex characters");
        }

        if (!seen_chunk_ids.emplace(chunk_id, seen_chunk_ids.size()).second) {
            throw std::invalid_argument("classify_gc_chunks requires unique chunk IDs");
        }
    }

    boost::json::array chunk_array;

    for (const std::string& chunk_id : chunk_ids) {
        chunk_array.emplace_back(chunk_id);
    }

    const std::string body = boost::json::serialize(boost::json::object{
        {"chunk_ids", std::move(chunk_array)},
    });
    const std::string target = std::string{"/v1/gc-runs/"} + gc_run_id.str() + "/classify";
    const aistore::http::HttpClientResponse response =
        http_client_.request(beast_http::verb::post, target, body, "application/json");

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(response.body(), parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"GC classification response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (object.size() != 2U || !object.contains("gc_run_id") || !object.contains("chunks") ||
        !object.at("gc_run_id").is_string() || !object.at("chunks").is_array()) {
        throw RemoteProtocolError{"GC classification response has unexpected fields"};
    }

    std::optional<aistore::metadata::UuidV7> response_gc_run_id;

    try {
        response_gc_run_id.emplace(std::string{object.at("gc_run_id").as_string()});
    } catch (const std::invalid_argument&) {
        throw RemoteProtocolError{"GC classification response gc_run_id is invalid"};
    }

    if (*response_gc_run_id != gc_run_id) {
        throw RemoteProtocolError{"GC classification response gc_run_id does not match request"};
    }

    const boost::json::array& chunks = object.at("chunks").as_array();

    if (chunks.size() != chunk_ids.size()) {
        throw RemoteProtocolError{"GC classification response chunk count does not match request"};
    }

    std::vector<aistore::metadata::GcChunkDecision> decisions;
    decisions.reserve(chunks.size());

    for (std::size_t index = 0; index < chunks.size(); ++index) {
        const boost::json::value& entry_value = chunks.at(index);

        if (!entry_value.is_object()) {
            throw RemoteProtocolError{"GC classification chunk entry is not an object"};
        }

        const boost::json::object& entry = entry_value.as_object();

        if (entry.size() != 2U || !entry.contains("chunk_id") || !entry.contains("collectible") ||
            !entry.at("chunk_id").is_string() || !entry.at("collectible").is_bool()) {
            throw RemoteProtocolError{"GC classification chunk entry has unexpected fields"};
        }

        const std::string chunk_id{entry.at("chunk_id").as_string()};

        if (chunk_id != chunk_ids.at(index)) {
            throw RemoteProtocolError{"GC classification response chunk order does not match request"};
        }

        if (!is_valid_chunk_id(chunk_id)) {
            throw RemoteProtocolError{"GC classification chunk_id is invalid"};
        }

        decisions.push_back(aistore::metadata::GcChunkDecision{
            .chunk_id = chunk_id,
            .collectible = entry.at("collectible").as_bool(),
        });
    }

    return decisions;
}

aistore::metadata::GcRun MetadataClient::complete_gc_run(
    const aistore::metadata::UuidV7& gc_run_id, const aistore::metadata::GcPhysicalStats& physical_stats) const {
    validate_gc_physical_stats(physical_stats);

    const std::string body = boost::json::serialize(boost::json::object{
        {"physical_chunks_scanned", physical_stats.physical_chunks_scanned},
        {"physical_bytes_scanned", physical_stats.physical_bytes_scanned},
        {"collectible_chunks", physical_stats.collectible_chunks},
        {"collectible_bytes", physical_stats.collectible_bytes},
        {"physically_deleted_chunks", physical_stats.physically_deleted_chunks},
        {"physically_deleted_bytes", physical_stats.physically_deleted_bytes},
    });
    const std::string target = std::string{"/v1/gc-runs/"} + gc_run_id.str() + "/complete";
    const aistore::http::HttpClientResponse response =
        http_client_.request(beast_http::verb::post, target, body, "application/json");

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    aistore::metadata::GcRun run = parse_gc_run_json(response.body());

    if (run.run_id != gc_run_id) {
        throw RemoteProtocolError{"GC run response gc_run_id does not match request"};
    }

    if (run.state != aistore::metadata::GcRunState::Completed) {
        throw RemoteProtocolError{"GC run response state is not completed"};
    }

    return run;
}

[[nodiscard]] aistore::metadata::StorageNode parse_storage_node_json(const boost::json::object& object) {
    if (!json_object_has_exact_keys(object, {"node_id", "address", "port", "state"})) {
        throw RemoteProtocolError{"storage node response has unexpected fields"};
    }

    if (!object.at("node_id").is_string() || !object.at("address").is_string() || !object.at("state").is_string()) {
        throw RemoteProtocolError{"storage node response field types are invalid"};
    }

    const std::optional<std::uint64_t> port = extract_positive_uint64(object.at("port"));

    if (!port.has_value() || *port > 65535U) {
        throw RemoteProtocolError{"storage node port is invalid"};
    }

    try {
        return aistore::metadata::StorageNode{
            .node_id = std::string{object.at("node_id").as_string()},
            .address = std::string{object.at("address").as_string()},
            .port = static_cast<std::uint16_t>(*port),
            .state = aistore::metadata::storage_node_state_from_string(std::string{object.at("state").as_string()}),
        };
    } catch (const std::exception&) {
        throw RemoteProtocolError{"storage node response failed domain validation"};
    }
}

[[nodiscard]] aistore::metadata::ReplicationRunState replication_run_state_from_string_strict(std::string_view state) {
    if (state == "open") {
        return aistore::metadata::ReplicationRunState::Open;
    }

    if (state == "completed") {
        return aistore::metadata::ReplicationRunState::Completed;
    }

    throw RemoteProtocolError{"replication run state is invalid"};
}

[[nodiscard]] aistore::metadata::ReplicationRun parse_replication_run_json(const std::string& body) {
    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(body, parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"replication run response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (!json_object_has_exact_keys(
            object, {"replication_run_id", "version_id", "layout_id", "replication_factor", "placement_node_ids",
                     "state", "chunks_scanned", "chunks_under_replicated", "replicas_verified", "replicas_written",
                     "bytes_copied", "source_failovers"})) {
        throw RemoteProtocolError{"replication run response has unexpected fields"};
    }

    if (!object.at("replication_run_id").is_string() || !object.at("version_id").is_string() ||
        !object.at("layout_id").is_string() || !object.at("placement_node_ids").is_array() ||
        !object.at("state").is_string()) {
        throw RemoteProtocolError{"replication run response field types are invalid"};
    }

    const std::optional<std::uint64_t> optional_replication_factor =
        extract_positive_uint64(object.at("replication_factor"));

    if (!optional_replication_factor.has_value() || *optional_replication_factor > 8U) {
        throw RemoteProtocolError{"replication run replication_factor is invalid"};
    }

    const auto replication_factor = static_cast<std::uint8_t>(*optional_replication_factor);

    std::vector<std::string> placement_node_ids;
    for (const boost::json::value& node_value : object.at("placement_node_ids").as_array()) {
        if (!node_value.is_string()) {
            throw RemoteProtocolError{"replication run placement_node_ids must be strings"};
        }

        placement_node_ids.emplace_back(node_value.as_string());
    }

    const auto extract_stat = [&](std::string_view key) -> std::uint64_t {
        const std::optional<std::uint64_t> value = extract_nonnegative_uint64(object.at(key));

        if (!value.has_value()) {
            throw RemoteProtocolError{"replication run stats are invalid"};
        }

        return *value;
    };

    return aistore::metadata::ReplicationRun{
        .run_id = aistore::metadata::UuidV7{std::string{object.at("replication_run_id").as_string()}},
        .version_id = std::string{object.at("version_id").as_string()},
        .layout_id = std::string{object.at("layout_id").as_string()},
        .replication_factor = replication_factor,
        .placement_node_ids = std::move(placement_node_ids),
        .state = replication_run_state_from_string_strict(std::string{object.at("state").as_string()}),
        .stats =
            aistore::metadata::ReplicationStats{
                .chunks_scanned = extract_stat("chunks_scanned"),
                .chunks_under_replicated = extract_stat("chunks_under_replicated"),
                .replicas_verified = extract_stat("replicas_verified"),
                .replicas_written = extract_stat("replicas_written"),
                .bytes_copied = extract_stat("bytes_copied"),
                .source_failovers = extract_stat("source_failovers"),
            },
    };
}

[[nodiscard]] aistore::metadata::ReplicationNodeEndpoint parse_replication_node_endpoint(
    const boost::json::object& object) {
    if (!json_object_has_exact_keys(object, {"node_id", "address", "port"})) {
        throw RemoteProtocolError{"replication node endpoint has unexpected fields"};
    }

    if (!object.at("node_id").is_string() || !object.at("address").is_string()) {
        throw RemoteProtocolError{"replication node endpoint field types are invalid"};
    }

    const std::optional<std::uint64_t> port = extract_positive_uint64(object.at("port"));

    if (!port.has_value() || *port > 65535U) {
        throw RemoteProtocolError{"replication node endpoint port is invalid"};
    }

    return aistore::metadata::ReplicationNodeEndpoint{
        .node_id = std::string{object.at("node_id").as_string()},
        .address = std::string{object.at("address").as_string()},
        .port = static_cast<std::uint16_t>(*port),
    };
}

[[nodiscard]] aistore::metadata::RestoreNodeEndpoint parse_restore_node_endpoint(const boost::json::object& object) {
    if (!json_object_has_exact_keys(object, {"node_id", "address", "port"})) {
        throw RemoteProtocolError{"restore source endpoint has unexpected fields"};
    }

    if (!object.at("node_id").is_string() || !object.at("address").is_string()) {
        throw RemoteProtocolError{"restore source endpoint field types are invalid"};
    }

    const std::optional<std::uint64_t> port = extract_positive_uint64(object.at("port"));

    if (!port.has_value() || *port > 65535U) {
        throw RemoteProtocolError{"restore source endpoint port is invalid"};
    }

    return aistore::metadata::RestoreNodeEndpoint{
        .node_id = std::string{object.at("node_id").as_string()},
        .address = std::string{object.at("address").as_string()},
        .port = static_cast<std::uint16_t>(*port),
    };
}

aistore::metadata::MultiNodeRestorePlan MetadataClient::get_multi_node_restore_plan(std::string_view version_id) const {
    if (!is_valid_chunk_id(version_id)) {
        throw std::invalid_argument("version_id must be 64 lowercase hex characters");
    }

    const std::string target = std::string{"/v1/artifact-versions/"} + std::string{version_id} + "/restore-plan";
    const aistore::http::HttpClientResponse response = http_client_.request(beast_http::verb::get, target);

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(response.body(), parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"multi-node restore plan response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (object.size() != 9U || !object.contains("version_id") || !object.contains("artifact_id") ||
        !object.contains("object_id") || !object.contains("total_size_bytes") || !object.contains("layout_id") ||
        !object.contains("chunking_strategy") || !object.contains("chunking_parameters") ||
        !object.contains("chunk_count") || !object.contains("chunks")) {
        throw RemoteProtocolError{"multi-node restore plan response has unexpected fields"};
    }

    const std::string response_version_id{object.at("version_id").as_string()};

    if (response_version_id != version_id) {
        throw RemoteProtocolError{"multi-node restore plan version_id does not match request"};
    }

    const std::string object_id{object.at("object_id").as_string()};
    const std::string layout_id{object.at("layout_id").as_string()};

    if (!is_valid_chunk_id(object_id) || !is_valid_chunk_id(layout_id)) {
        throw RemoteProtocolError{"multi-node restore plan contains an invalid content ID"};
    }

    const std::string chunking_strategy{object.at("chunking_strategy").as_string()};
    const boost::json::object& chunking_parameters = object.at("chunking_parameters").as_object();
    const std::optional<std::uint64_t> total_size_bytes = extract_nonnegative_uint64(object.at("total_size_bytes"));
    const std::optional<std::uint64_t> chunk_count = extract_nonnegative_uint64(object.at("chunk_count"));

    if (!total_size_bytes.has_value() || !chunk_count.has_value()) {
        throw RemoteProtocolError{"multi-node restore plan size fields are invalid"};
    }

    const boost::json::array& chunks_array = object.at("chunks").as_array();

    if (*chunk_count != chunks_array.size()) {
        throw RemoteProtocolError{"multi-node restore plan chunk_count mismatch"};
    }

    std::vector<aistore::metadata::ChunkRef> chunk_refs;
    std::vector<aistore::metadata::RestoreChunkSources> chunk_sources;
    chunk_refs.reserve(chunks_array.size());
    chunk_sources.reserve(chunks_array.size());

    for (const boost::json::value& chunk_value : chunks_array) {
        if (!chunk_value.is_object()) {
            throw RemoteProtocolError{"multi-node restore plan chunk entry is not an object"};
        }

        const boost::json::object& chunk_object = chunk_value.as_object();

        if (!json_object_has_exact_keys(chunk_object, {"chunk_id", "offset", "size_bytes", "sources"}) ||
            !chunk_object.at("chunk_id").is_string() || !chunk_object.at("sources").is_array()) {
            throw RemoteProtocolError{"multi-node restore plan chunk entry has unexpected fields"};
        }

        const std::string chunk_id{chunk_object.at("chunk_id").as_string()};

        if (!is_valid_chunk_id(chunk_id)) {
            throw RemoteProtocolError{"multi-node restore plan chunk_id is invalid"};
        }

        const std::optional<std::uint64_t> offset = extract_nonnegative_uint64(chunk_object.at("offset"));
        const std::optional<std::uint64_t> size_bytes = extract_positive_uint64(chunk_object.at("size_bytes"));

        if (!offset.has_value() || !size_bytes.has_value()) {
            throw RemoteProtocolError{"multi-node restore plan chunk size fields are invalid"};
        }

        aistore::metadata::RestoreChunkSources sources{
            .chunk_id = chunk_id,
            .offset = *offset,
            .size_bytes = *size_bytes,
        };

        for (const boost::json::value& source_value : chunk_object.at("sources").as_array()) {
            if (!source_value.is_object()) {
                throw RemoteProtocolError{"multi-node restore plan source entry is not an object"};
            }

            sources.sources.push_back(parse_restore_node_endpoint(source_value.as_object()));
        }

        chunk_refs.push_back(aistore::metadata::ChunkRef{
            .chunk_id = chunk_id,
            .offset = *offset,
            .size = *size_bytes,
        });
        chunk_sources.push_back(std::move(sources));
    }

    aistore::metadata::UuidV7 artifact_id{std::string{object.at("artifact_id").as_string()}};

    try {
        aistore::metadata::Object layout_object{object_id, *total_size_bytes};
        aistore::metadata::ObjectLayout layout{std::move(chunk_refs)};
        aistore::metadata::ObjectLayoutDescriptor descriptor = [&]() {
            if (chunking_strategy == "fixed-size") {
                if (!chunking_parameters.empty()) {
                    throw RemoteProtocolError{"multi-node restore plan chunking_parameters are invalid"};
                }

                return aistore::metadata::ObjectLayoutDescriptor{
                    std::move(layout_object), aistore::metadata::ChunkingStrategy::FixedSize, std::move(layout)};
            }

            if (chunking_strategy != "fastcdc") {
                throw RemoteProtocolError{"multi-node restore plan chunking strategy is unsupported"};
            }

            const std::optional<aistore::metadata::FastCdcParameters> fastcdc_parameters =
                parse_fastcdc_parameters_json(chunking_parameters);

            if (!fastcdc_parameters.has_value()) {
                throw RemoteProtocolError{"multi-node restore plan chunking_parameters are invalid"};
            }

            return aistore::metadata::ObjectLayoutDescriptor{std::move(layout_object), *fastcdc_parameters,
                                                             std::move(layout)};
        }();

        return aistore::metadata::MultiNodeRestorePlan{
            .artifact_id = artifact_id,
            .version_id = response_version_id,
            .object_id = object_id,
            .layout_id = layout_id,
            .layout_descriptor = std::move(descriptor),
            .chunks = std::move(chunk_sources),
        };
    } catch (const RemoteProtocolError&) {
        throw;
    } catch (const std::exception&) {
        throw RemoteProtocolError{"multi-node restore plan failed domain validation"};
    }
}

void MetadataClient::register_storage_node(const aistore::metadata::StorageNode& node) const {
    aistore::metadata::validate_storage_node(node);

    const std::string body = boost::json::serialize(boost::json::object{
        {"address", node.address},
        {"port", node.port},
        {"state", aistore::metadata::storage_node_state_to_string(node.state)},
    });
    const std::string target = std::string{"/v1/storage-nodes/"} + node.node_id;
    const aistore::http::HttpClientResponse response =
        http_client_.request(beast_http::verb::put, target, body, "application/json");

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(response.body(), parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"storage node response is not a JSON object"};
    }

    const aistore::metadata::StorageNode response_node = parse_storage_node_json(parsed.as_object());

    if (response_node != node) {
        throw RemoteProtocolError{"storage node response does not match request"};
    }
}

std::optional<aistore::metadata::StorageNode> MetadataClient::get_storage_node(std::string_view node_id) const {
    if (!is_valid_node_id(node_id)) {
        throw std::invalid_argument("node_id is invalid");
    }

    const std::string target = std::string{"/v1/storage-nodes/"} + std::string{node_id};
    const aistore::http::HttpClientResponse response = http_client_.request(beast_http::verb::get, target);

    const unsigned int status = status_code_of(response);

    if (status == 200U) {
        boost::system::error_code parse_error;
        const boost::json::value parsed = boost::json::parse(response.body(), parse_error);

        if (parse_error || !parsed.is_object()) {
            throw RemoteProtocolError{"storage node response is not a JSON object"};
        }

        return parse_storage_node_json(parsed.as_object());
    }

    if (status == 404U && extract_error_code(response.body()) == "storage_node_not_found") {
        return std::nullopt;
    }

    throw_remote_api_error(response);
}

std::vector<aistore::metadata::StorageNode> MetadataClient::list_storage_nodes() const {
    const aistore::http::HttpClientResponse response = http_client_.request(beast_http::verb::get, "/v1/storage-nodes");

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(response.body(), parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"storage node list response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (!json_object_has_exact_keys(object, {"nodes"}) || !object.at("nodes").is_array()) {
        throw RemoteProtocolError{"storage node list response has unexpected fields"};
    }

    std::vector<aistore::metadata::StorageNode> nodes;

    for (const boost::json::value& node_value : object.at("nodes").as_array()) {
        if (!node_value.is_object()) {
            throw RemoteProtocolError{"storage node list entry is not an object"};
        }

        nodes.push_back(parse_storage_node_json(node_value.as_object()));
    }

    return nodes;
}

aistore::metadata::ReplicationRun MetadataClient::start_replication_run(const aistore::metadata::UuidV7& run_id,
                                                                        std::string_view version_id,
                                                                        std::uint8_t replication_factor) const {
    if (!is_valid_chunk_id(version_id)) {
        throw std::invalid_argument("version_id must be 64 lowercase hex characters");
    }

    if (replication_factor < 1U || replication_factor > 8U) {
        throw std::invalid_argument("replication_factor must be between 1 and 8");
    }

    const std::string body = boost::json::serialize(boost::json::object{
        {"replication_run_id", run_id.str()},
        {"version_id", std::string{version_id}},
        {"replication_factor", static_cast<std::uint64_t>(replication_factor)},
    });
    const aistore::http::HttpClientResponse response =
        http_client_.request(beast_http::verb::post, "/v1/replication-runs", body, "application/json");

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    aistore::metadata::ReplicationRun run = parse_replication_run_json(response.body());

    if (run.run_id != run_id) {
        throw RemoteProtocolError{"replication run response run_id does not match request"};
    }

    return run;
}

std::optional<aistore::metadata::ReplicationRun> MetadataClient::get_replication_run(
    const aistore::metadata::UuidV7& run_id) const {
    const std::string target = std::string{"/v1/replication-runs/"} + run_id.str();
    const aistore::http::HttpClientResponse response = http_client_.request(beast_http::verb::get, target);

    const unsigned int status = status_code_of(response);

    if (status == 200U) {
        aistore::metadata::ReplicationRun run = parse_replication_run_json(response.body());

        if (run.run_id != run_id) {
            throw RemoteProtocolError{"replication run response run_id does not match request"};
        }

        return run;
    }

    if (status == 404U && extract_error_code(response.body()) == "replication_run_not_found") {
        return std::nullopt;
    }

    throw_remote_api_error(response);
}

aistore::metadata::ReplicationPlan MetadataClient::get_replication_plan(const aistore::metadata::UuidV7& run_id) const {
    const std::string target = std::string{"/v1/replication-runs/"} + run_id.str() + "/plan";
    const aistore::http::HttpClientResponse response = http_client_.request(beast_http::verb::get, target);

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(response.body(), parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"replication plan response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (!object.contains("replication_run_id") || !object.contains("version_id") || !object.contains("layout_id") ||
        !object.contains("replication_factor") || !object.contains("placement_node_ids") ||
        !object.contains("chunks")) {
        throw RemoteProtocolError{"replication plan response has unexpected fields"};
    }

    const std::optional<std::uint64_t> optional_replication_factor =
        extract_positive_uint64(object.at("replication_factor"));

    if (!optional_replication_factor.has_value() || *optional_replication_factor > 8U) {
        throw RemoteProtocolError{"replication plan replication_factor is invalid"};
    }

    const auto replication_factor = static_cast<std::uint8_t>(*optional_replication_factor);
    std::vector<std::string> placement_node_ids;

    for (const boost::json::value& node_value : object.at("placement_node_ids").as_array()) {
        placement_node_ids.emplace_back(node_value.as_string());
    }

    std::vector<aistore::metadata::ReplicationChunkPlan> chunks;

    for (const boost::json::value& chunk_value : object.at("chunks").as_array()) {
        const boost::json::object& chunk_object = chunk_value.as_object();
        aistore::metadata::ReplicationChunkPlan chunk_plan{
            .chunk_id = std::string{chunk_object.at("chunk_id").as_string()},
            .offset = extract_nonnegative_uint64(chunk_object.at("offset")).value_or(0),
            .size_bytes = extract_positive_uint64(chunk_object.at("size_bytes")).value_or(0),
        };

        for (const boost::json::value& source_value : chunk_object.at("source_nodes").as_array()) {
            chunk_plan.source_nodes.push_back(parse_replication_node_endpoint(source_value.as_object()));
        }

        for (const boost::json::value& node_value : chunk_object.at("desired_node_ids").as_array()) {
            chunk_plan.desired_node_ids.emplace_back(node_value.as_string());
        }

        for (const boost::json::value& target_value : chunk_object.at("target_nodes").as_array()) {
            chunk_plan.target_nodes.push_back(parse_replication_node_endpoint(target_value.as_object()));
        }

        chunks.push_back(std::move(chunk_plan));
    }

    return aistore::metadata::ReplicationPlan{
        .run_id = aistore::metadata::UuidV7{std::string{object.at("replication_run_id").as_string()}},
        .version_id = std::string{object.at("version_id").as_string()},
        .layout_id = std::string{object.at("layout_id").as_string()},
        .replication_factor = replication_factor,
        .placement_node_ids = std::move(placement_node_ids),
        .chunks = std::move(chunks),
    };
}

aistore::metadata::ReplicationRun MetadataClient::complete_replication_run(
    const aistore::metadata::UuidV7& run_id, const aistore::metadata::ReplicationStats& stats) const {
    const std::string body = boost::json::serialize(boost::json::object{
        {"chunks_scanned", stats.chunks_scanned},
        {"chunks_under_replicated", stats.chunks_under_replicated},
        {"replicas_verified", stats.replicas_verified},
        {"replicas_written", stats.replicas_written},
        {"bytes_copied", stats.bytes_copied},
        {"source_failovers", stats.source_failovers},
    });
    const std::string target = std::string{"/v1/replication-runs/"} + run_id.str() + "/complete";
    const aistore::http::HttpClientResponse response =
        http_client_.request(beast_http::verb::post, target, body, "application/json");

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    aistore::metadata::ReplicationRun run = parse_replication_run_json(response.body());

    if (run.run_id != run_id) {
        throw RemoteProtocolError{"replication run response run_id does not match request"};
    }

    if (run.state != aistore::metadata::ReplicationRunState::Completed) {
        throw RemoteProtocolError{"replication run response state is not completed"};
    }

    return run;
}

[[nodiscard]] std::optional<std::int64_t> extract_signed_int64(const boost::json::value& value) {
    if (value.is_double()) {
        return std::nullopt;
    }

    if (value.is_int64()) {
        return value.as_int64();
    }

    if (value.is_uint64()) {
        const std::uint64_t unsigned_value = value.as_uint64();

        if (unsigned_value > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
            return std::nullopt;
        }

        return static_cast<std::int64_t>(unsigned_value);
    }

    return std::nullopt;
}

[[nodiscard]] aistore::metadata::LifecycleRule parse_lifecycle_rule_json(const boost::json::object& object) {
    if (!json_object_has_exact_keys(object, {"artifact_kind", "keep_last_n", "max_age_seconds"}) ||
        !object.at("artifact_kind").is_string()) {
        throw RemoteProtocolError{"lifecycle policy rule has unexpected fields"};
    }

    const std::optional<std::uint64_t> keep_last_n = extract_nonnegative_uint64(object.at("keep_last_n"));

    if (!keep_last_n.has_value()) {
        throw RemoteProtocolError{"lifecycle policy keep_last_n is invalid"};
    }

    std::optional<std::uint64_t> max_age_seconds;

    if (object.at("max_age_seconds").is_null()) {
        max_age_seconds = std::nullopt;
    } else {
        max_age_seconds = extract_nonnegative_uint64(object.at("max_age_seconds"));

        if (!max_age_seconds.has_value()) {
            throw RemoteProtocolError{"lifecycle policy max_age_seconds is invalid"};
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
        throw RemoteProtocolError{"lifecycle policy artifact_kind is invalid"};
    }
}

[[nodiscard]] aistore::metadata::LifecyclePolicy parse_lifecycle_policy_json(const boost::json::object& object) {
    if (!json_object_has_exact_keys(object, {"policy_id", "name", "rules"}) || !object.at("policy_id").is_string() ||
        !object.at("name").is_string() || !object.at("rules").is_array()) {
        throw RemoteProtocolError{"lifecycle policy response has unexpected fields"};
    }

    const boost::json::array& rules_json = object.at("rules").as_array();

    if (rules_json.size() != kExpectedLifecycleRuleCount) {
        throw RemoteProtocolError{"lifecycle policy rules count is invalid"};
    }

    std::map<aistore::metadata::ArtifactKind, aistore::metadata::LifecycleRule> rules;

    for (const boost::json::value& entry : rules_json) {
        if (!entry.is_object()) {
            throw RemoteProtocolError{"lifecycle policy rule is not an object"};
        }

        const aistore::metadata::LifecycleRule rule = parse_lifecycle_rule_json(entry.as_object());

        if (rules.contains(rule.artifact_kind)) {
            throw RemoteProtocolError{"lifecycle policy rules contain duplicate artifact kinds"};
        }

        rules.emplace(rule.artifact_kind, rule);
    }

    for (const aistore::metadata::ArtifactKind kind : kCanonicalArtifactKindOrder) {
        if (!rules.contains(kind)) {
            throw RemoteProtocolError{"lifecycle policy rules are missing a canonical artifact kind"};
        }
    }

    try {
        return aistore::metadata::LifecyclePolicy{
            .policy_id = aistore::metadata::UuidV7{std::string{object.at("policy_id").as_string()}},
            .name = std::string{object.at("name").as_string()},
            .rules = std::move(rules),
        };
    } catch (const std::invalid_argument&) {
        throw RemoteProtocolError{"lifecycle policy policy_id is invalid"};
    }
}

[[nodiscard]] aistore::metadata::LifecyclePolicy parse_lifecycle_policy_body(const std::string& body) {
    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(body, parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"lifecycle policy response is not a JSON object"};
    }

    return parse_lifecycle_policy_json(parsed.as_object());
}

[[nodiscard]] aistore::metadata::LifecycleRun parse_lifecycle_run_json(const boost::json::object& object) {
    if (!json_object_has_exact_keys(
            object, {"run_id", "policy_id", "mode", "evaluated_at_unix_ms", "versions_scanned", "versions_protected",
                     "versions_retained_by_policy", "versions_candidates", "versions_retired",
                     "logical_bytes_candidates", "logical_bytes_retired"}) ||
        !object.at("run_id").is_string() || !object.at("policy_id").is_string() || !object.at("mode").is_string()) {
        throw RemoteProtocolError{"lifecycle run response has unexpected fields"};
    }

    const std::optional<std::int64_t> evaluated_at_unix_ms = extract_signed_int64(object.at("evaluated_at_unix_ms"));

    if (!evaluated_at_unix_ms.has_value()) {
        throw RemoteProtocolError{"lifecycle run evaluated_at_unix_ms is invalid"};
    }

    const std::optional<std::uint64_t> versions_scanned = extract_nonnegative_uint64(object.at("versions_scanned"));
    const std::optional<std::uint64_t> versions_protected = extract_nonnegative_uint64(object.at("versions_protected"));
    const std::optional<std::uint64_t> versions_retained_by_policy =
        extract_nonnegative_uint64(object.at("versions_retained_by_policy"));
    const std::optional<std::uint64_t> versions_candidates =
        extract_nonnegative_uint64(object.at("versions_candidates"));
    const std::optional<std::uint64_t> versions_retired = extract_nonnegative_uint64(object.at("versions_retired"));
    const std::optional<std::uint64_t> logical_bytes_candidates =
        extract_nonnegative_uint64(object.at("logical_bytes_candidates"));
    const std::optional<std::uint64_t> logical_bytes_retired =
        extract_nonnegative_uint64(object.at("logical_bytes_retired"));

    if (!versions_scanned.has_value() || !versions_protected.has_value() || !versions_retained_by_policy.has_value() ||
        !versions_candidates.has_value() || !versions_retired.has_value() || !logical_bytes_candidates.has_value() ||
        !logical_bytes_retired.has_value()) {
        throw RemoteProtocolError{"lifecycle run stats are invalid"};
    }

    try {
        return aistore::metadata::LifecycleRun{
            .run_id = aistore::metadata::UuidV7{std::string{object.at("run_id").as_string()}},
            .policy_id = aistore::metadata::UuidV7{std::string{object.at("policy_id").as_string()}},
            .mode = aistore::metadata::lifecycle_run_mode_from_string(std::string{object.at("mode").as_string()}),
            .evaluated_at_unix_ms = *evaluated_at_unix_ms,
            .stats =
                aistore::metadata::LifecycleStats{
                    .versions_scanned = *versions_scanned,
                    .versions_protected = *versions_protected,
                    .versions_retained_by_policy = *versions_retained_by_policy,
                    .versions_candidates = *versions_candidates,
                    .versions_retired = *versions_retired,
                    .logical_bytes_candidates = *logical_bytes_candidates,
                    .logical_bytes_retired = *logical_bytes_retired,
                },
        };
    } catch (const std::invalid_argument&) {
        throw RemoteProtocolError{"lifecycle run identifiers are invalid"};
    } catch (const std::runtime_error&) {
        throw RemoteProtocolError{"lifecycle run mode is invalid"};
    }
}

[[nodiscard]] aistore::metadata::LifecycleRun parse_lifecycle_run_body(const std::string& body) {
    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(body, parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"lifecycle run response is not a JSON object"};
    }

    return parse_lifecycle_run_json(parsed.as_object());
}

[[nodiscard]] aistore::metadata::LifecycleDecision parse_lifecycle_decision_json(const boost::json::object& object) {
    if (!json_object_has_exact_keys(
            object, {"version_id", "artifact_id", "artifact_kind", "decision", "reason", "logical_size_bytes"}) ||
        !object.at("version_id").is_string() || !object.at("artifact_id").is_string() ||
        !object.at("artifact_kind").is_string() || !object.at("decision").is_string() ||
        !object.at("reason").is_string()) {
        throw RemoteProtocolError{"lifecycle decision has unexpected fields"};
    }

    const std::string decision{object.at("decision").as_string()};

    if (decision != "retain" && decision != "retire") {
        throw RemoteProtocolError{"lifecycle decision value is invalid"};
    }

    const std::optional<std::uint64_t> logical_size_bytes = extract_nonnegative_uint64(object.at("logical_size_bytes"));

    if (!logical_size_bytes.has_value()) {
        throw RemoteProtocolError{"lifecycle decision logical_size_bytes is invalid"};
    }

    try {
        return aistore::metadata::LifecycleDecision{
            .version_id = std::string{object.at("version_id").as_string()},
            .artifact_id = aistore::metadata::UuidV7{std::string{object.at("artifact_id").as_string()}},
            .artifact_kind =
                aistore::metadata::artifact_kind_from_string(std::string{object.at("artifact_kind").as_string()}),
            .retire = decision == "retire",
            .reason =
                aistore::metadata::lifecycle_decision_reason_from_string(std::string{object.at("reason").as_string()}),
            .logical_size_bytes = *logical_size_bytes,
        };
    } catch (const std::invalid_argument&) {
        throw RemoteProtocolError{"lifecycle decision artifact_id is invalid"};
    } catch (const std::runtime_error&) {
        throw RemoteProtocolError{"lifecycle decision kind or reason is invalid"};
    }
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

aistore::metadata::LifecyclePolicy MetadataClient::register_lifecycle_policy(
    const aistore::metadata::LifecyclePolicy& policy) const {
    aistore::metadata::validate_lifecycle_policy(policy);

    boost::json::array rules;
    rules.reserve(kExpectedLifecycleRuleCount);

    for (const aistore::metadata::ArtifactKind kind : kCanonicalArtifactKindOrder) {
        const auto rule_it = policy.rules.find(kind);

        if (rule_it == policy.rules.end()) {
            throw std::invalid_argument("lifecycle policy is missing a canonical artifact kind rule");
        }

        rules.push_back(lifecycle_rule_to_json(rule_it->second));
    }

    const std::string body = boost::json::serialize(boost::json::object{
        {"policy_id", policy.policy_id.str()},
        {"name", policy.name},
        {"rules", std::move(rules)},
    });
    const aistore::http::HttpClientResponse response =
        http_client_.request(beast_http::verb::post, "/v1/lifecycle-policies", body, "application/json");

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    aistore::metadata::LifecyclePolicy loaded = parse_lifecycle_policy_body(response.body());

    if (loaded.policy_id != policy.policy_id || loaded.name != policy.name || loaded.rules != policy.rules) {
        throw RemoteProtocolError{"lifecycle policy response does not match request"};
    }

    return loaded;
}

std::optional<aistore::metadata::LifecyclePolicy> MetadataClient::get_lifecycle_policy(
    const aistore::metadata::UuidV7& policy_id) const {
    const std::string target = std::string{"/v1/lifecycle-policies/"} + policy_id.str();
    const aistore::http::HttpClientResponse response = http_client_.request(beast_http::verb::get, target);

    const unsigned int status = status_code_of(response);

    if (status == 200U) {
        aistore::metadata::LifecyclePolicy policy = parse_lifecycle_policy_body(response.body());

        if (policy.policy_id != policy_id) {
            throw RemoteProtocolError{"lifecycle policy response policy_id does not match request"};
        }

        return policy;
    }

    if (status == 404U && extract_error_code(response.body()) == "lifecycle_policy_not_found") {
        return std::nullopt;
    }

    throw_remote_api_error(response);
}

void MetadataClient::pin_version(std::string_view version_id, std::string_view reason) const {
    if (!is_valid_chunk_id(version_id)) {
        throw std::invalid_argument("version_id must be 64 lowercase hex characters");
    }

    const std::string body = boost::json::serialize(boost::json::object{
        {"reason", std::string{reason}},
    });
    const std::string target = std::string{"/v1/artifact-versions/"} + std::string{version_id} + "/pin";
    const aistore::http::HttpClientResponse response =
        http_client_.request(beast_http::verb::put, target, body, "application/json");

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(response.body(), parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"pin response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (!json_object_has_exact_keys(object, {"version_id", "pinned", "reason"}) ||
        !object.at("version_id").is_string() || !object.at("pinned").is_bool() || !object.at("reason").is_string() ||
        !object.at("pinned").as_bool() || std::string{object.at("version_id").as_string()} != version_id ||
        std::string{object.at("reason").as_string()} != reason) {
        throw RemoteProtocolError{"pin response does not match request"};
    }
}

void MetadataClient::unpin_version(std::string_view version_id) const {
    if (!is_valid_chunk_id(version_id)) {
        throw std::invalid_argument("version_id must be 64 lowercase hex characters");
    }

    const std::string target = std::string{"/v1/artifact-versions/"} + std::string{version_id} + "/pin";
    const aistore::http::HttpClientResponse response = http_client_.request(beast_http::verb::delete_, target);

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(response.body(), parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"unpin response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (!json_object_has_exact_keys(object, {"version_id", "pinned"}) || !object.at("version_id").is_string() ||
        !object.at("pinned").is_bool() || object.at("pinned").as_bool() ||
        std::string{object.at("version_id").as_string()} != version_id) {
        throw RemoteProtocolError{"unpin response does not match request"};
    }
}

VersionPinStatus MetadataClient::get_version_pin(std::string_view version_id) const {
    if (!is_valid_chunk_id(version_id)) {
        throw std::invalid_argument("version_id must be 64 lowercase hex characters");
    }

    const std::string target = std::string{"/v1/artifact-versions/"} + std::string{version_id} + "/pin";
    const aistore::http::HttpClientResponse response = http_client_.request(beast_http::verb::get, target);

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(response.body(), parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"pin status response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (!object.at("version_id").is_string() || !object.at("pinned").is_bool() ||
        std::string{object.at("version_id").as_string()} != version_id) {
        throw RemoteProtocolError{"pin status response version_id is invalid"};
    }

    if (!object.at("pinned").as_bool()) {
        if (!json_object_has_exact_keys(object, {"version_id", "pinned"})) {
            throw RemoteProtocolError{"unpinned pin status response has unexpected fields"};
        }

        return VersionPinStatus{.pinned = false};
    }

    if (!json_object_has_exact_keys(object, {"version_id", "pinned", "reason"}) || !object.at("reason").is_string()) {
        throw RemoteProtocolError{"pinned pin status response has unexpected fields"};
    }

    return VersionPinStatus{
        .pinned = true,
        .reason = std::string{object.at("reason").as_string()},
    };
}

aistore::metadata::LifecycleRun MetadataClient::run_lifecycle(const aistore::metadata::UuidV7& run_id,
                                                              const aistore::metadata::UuidV7& policy_id,
                                                              aistore::metadata::LifecycleRunMode mode) const {
    const std::string body = boost::json::serialize(boost::json::object{
        {"run_id", run_id.str()},
        {"policy_id", policy_id.str()},
        {"mode", std::string{aistore::metadata::lifecycle_run_mode_to_string(mode)}},
    });
    const aistore::http::HttpClientResponse response =
        http_client_.request(beast_http::verb::post, "/v1/lifecycle-runs", body, "application/json");

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    aistore::metadata::LifecycleRun run = parse_lifecycle_run_body(response.body());

    if (run.run_id != run_id || run.policy_id != policy_id || run.mode != mode) {
        throw RemoteProtocolError{"lifecycle run response does not match request"};
    }

    return run;
}

std::optional<aistore::metadata::LifecycleRun> MetadataClient::get_lifecycle_run(
    const aistore::metadata::UuidV7& run_id) const {
    const std::string target = std::string{"/v1/lifecycle-runs/"} + run_id.str();
    const aistore::http::HttpClientResponse response = http_client_.request(beast_http::verb::get, target);

    const unsigned int status = status_code_of(response);

    if (status == 200U) {
        aistore::metadata::LifecycleRun run = parse_lifecycle_run_body(response.body());

        if (run.run_id != run_id) {
            throw RemoteProtocolError{"lifecycle run response run_id does not match request"};
        }

        return run;
    }

    if (status == 404U && extract_error_code(response.body()) == "lifecycle_run_not_found") {
        return std::nullopt;
    }

    throw_remote_api_error(response);
}

LifecycleDecisionPage MetadataClient::list_lifecycle_decisions(const aistore::metadata::UuidV7& run_id,
                                                               std::optional<std::string_view> after_version_id,
                                                               std::size_t limit) const {
    if (limit < 1U || limit > 256U) {
        throw std::invalid_argument("lifecycle decision page limit must be between 1 and 256");
    }

    if (after_version_id.has_value() && !is_valid_chunk_id(*after_version_id)) {
        throw std::invalid_argument("after_version_id must be 64 lowercase hex characters");
    }

    std::string target =
        std::string{"/v1/lifecycle-runs/"} + run_id.str() + "/decisions?limit=" + std::to_string(limit);

    if (after_version_id.has_value()) {
        target += "&after=";
        target += *after_version_id;
    }

    const aistore::http::HttpClientResponse response = http_client_.request(beast_http::verb::get, target);

    if (status_code_of(response) != 200U) {
        throw_remote_api_error(response);
    }

    boost::system::error_code parse_error;
    const boost::json::value parsed = boost::json::parse(response.body(), parse_error);

    if (parse_error || !parsed.is_object()) {
        throw RemoteProtocolError{"lifecycle decisions response is not a JSON object"};
    }

    const boost::json::object& object = parsed.as_object();

    if (!json_object_has_exact_keys(object, {"decisions", "next_after"}) || !object.at("decisions").is_array()) {
        throw RemoteProtocolError{"lifecycle decisions response has unexpected fields"};
    }

    std::vector<aistore::metadata::LifecycleDecision> decisions;

    for (const boost::json::value& entry : object.at("decisions").as_array()) {
        if (!entry.is_object()) {
            throw RemoteProtocolError{"lifecycle decision entry is not an object"};
        }

        decisions.push_back(parse_lifecycle_decision_json(entry.as_object()));
    }

    std::optional<std::string> next_after;

    if (object.at("next_after").is_null()) {
        next_after = std::nullopt;
    } else if (object.at("next_after").is_string()) {
        next_after = std::string{object.at("next_after").as_string()};
    } else {
        throw RemoteProtocolError{"lifecycle decisions next_after is invalid"};
    }

    return LifecycleDecisionPage{
        .decisions = std::move(decisions),
        .next_after = std::move(next_after),
    };
}

}  // namespace aistore::client
