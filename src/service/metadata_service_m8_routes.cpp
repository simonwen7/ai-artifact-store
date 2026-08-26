#include "aistore/service/metadata_service_m8_routes.hpp"

#include <boost/json.hpp>
#include <cstdint>
#include <limits>
#include <mutex>
#include <optional>
#include <pqxx/pqxx>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "aistore/metadata/chunking.hpp"
#include "aistore/metadata/finalize_upload.hpp"
#include "aistore/metadata/gc.hpp"
#include "aistore/metadata/object.hpp"
#include "aistore/metadata/object_layout.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/replication.hpp"
#include "aistore/metadata/restore_plan.hpp"
#include "aistore/metadata/storage_node.hpp"

namespace aistore::service {

namespace {

namespace beast_http = aistore::http::beast_http;

constexpr std::string_view kApplicationJson = "application/json";
constexpr std::string_view kStorageNodesCollection = "/v1/storage-nodes";
constexpr std::string_view kStorageNodesPrefix = "/v1/storage-nodes/";
constexpr std::string_view kReplicationRunsCollection = "/v1/replication-runs";
constexpr std::string_view kReplicationRunsPrefix = "/v1/replication-runs/";
constexpr std::string_view kArtifactVersionsPrefix = "/v1/artifact-versions/";
constexpr std::string_view kRestorePlanSuffix = "/restore-plan";
constexpr std::string_view kRestorePlanInfix = "/restore-plan/";
constexpr std::string_view kPlanSuffix = "/plan";
constexpr std::string_view kCompleteSuffix = "/complete";
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

[[nodiscard]] boost::json::object storage_node_to_json(const aistore::metadata::StorageNode& node) {
    return boost::json::object{
        {"node_id", node.node_id},
        {"address", node.address},
        {"port", node.port},
        {"state", aistore::metadata::storage_node_state_to_string(node.state)},
    };
}

[[nodiscard]] boost::json::object replication_run_to_json(const aistore::metadata::ReplicationRun& run) {
    boost::json::array placement_array;
    placement_array.reserve(run.placement_node_ids.size());

    for (const std::string& node_id : run.placement_node_ids) {
        placement_array.push_back(boost::json::value(node_id));
    }

    return boost::json::object{
        {"replication_run_id", run.run_id.str()},
        {"version_id", run.version_id},
        {"layout_id", run.layout_id},
        {"replication_factor", static_cast<std::uint64_t>(run.replication_factor)},
        {"placement_node_ids", std::move(placement_array)},
        {"state", aistore::metadata::replication_run_state_to_string(run.state)},
        {"chunks_scanned", run.stats.chunks_scanned},
        {"chunks_under_replicated", run.stats.chunks_under_replicated},
        {"replicas_verified", run.stats.replicas_verified},
        {"replicas_written", run.stats.replicas_written},
        {"bytes_copied", run.stats.bytes_copied},
        {"source_failovers", run.stats.source_failovers},
    };
}

[[nodiscard]] boost::json::object multi_node_restore_plan_to_json(const aistore::metadata::MultiNodeRestorePlan& plan) {
    const aistore::metadata::ObjectLayoutDescriptor& descriptor = plan.layout_descriptor;
    boost::json::array chunks;
    chunks.reserve(plan.chunks.size());

    for (const aistore::metadata::RestoreChunkSources& chunk : plan.chunks) {
        boost::json::array sources;

        for (const aistore::metadata::RestoreNodeEndpoint& source : chunk.sources) {
            sources.push_back(boost::json::object{
                {"node_id", source.node_id},
                {"address", source.address},
                {"port", source.port},
            });
        }

        chunks.push_back(boost::json::object{
            {"chunk_id", chunk.chunk_id},
            {"offset", chunk.offset},
            {"size_bytes", chunk.size_bytes},
            {"sources", std::move(sources)},
        });
    }

    return boost::json::object{
        {"version_id", plan.version_id},
        {"artifact_id", plan.artifact_id.str()},
        {"object_id", plan.object_id},
        {"total_size_bytes", descriptor.object().total_size()},
        {"layout_id", plan.layout_id},
        {"chunking_strategy", aistore::metadata::chunking_strategy_to_string(descriptor.chunking_strategy())},
        {"chunking_parameters", chunking_parameters_to_json(descriptor)},
        {"chunk_count", chunks.size()},
        {"chunks", std::move(chunks)},
    };
}

[[nodiscard]] std::optional<std::string_view> parse_query_param(std::string_view query, const std::string& key) {
    if (query.empty()) {
        return std::nullopt;
    }

    std::size_t start = 0;

    while (start < query.size()) {
        const std::size_t ampersand = query.find('&', start);
        const std::string_view pair =
            ampersand == std::string_view::npos ? query.substr(start) : query.substr(start, ampersand - start);
        const std::size_t equals = pair.find('=');

        if (equals != std::string_view::npos) {
            const std::string_view pair_key = pair.substr(0, equals);

            if (pair_key == key) {
                return pair.substr(equals + 1U);
            }
        }

        if (ampersand == std::string_view::npos) {
            break;
        }

        start = ampersand + 1U;
    }

    return std::nullopt;
}

[[nodiscard]] aistore::http::HttpResponse handle_list_storage_nodes(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex) {
    std::vector<aistore::metadata::StorageNode> nodes;

    {
        const std::scoped_lock lock{repository_mutex};
        nodes = repository.list_storage_nodes();
    }

    boost::json::array node_array;
    node_array.reserve(nodes.size());

    for (const aistore::metadata::StorageNode& node : nodes) {
        node_array.push_back(storage_node_to_json(node));
    }

    return make_json_response(request, beast_http::status::ok,
                              boost::json::object{
                                  {"nodes", std::move(node_array)},
                              });
}

[[nodiscard]] aistore::http::HttpResponse handle_get_storage_node(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view node_id) {
    std::optional<aistore::metadata::StorageNode> node;

    {
        const std::scoped_lock lock{repository_mutex};
        node = repository.get_storage_node(node_id);
    }

    if (!node.has_value()) {
        return make_json_response(request, beast_http::status::not_found,
                                  boost::json::object{
                                      {"error", "storage_node_not_found"},
                                  });
    }

    return make_json_response(request, beast_http::status::ok, storage_node_to_json(*node));
}

[[nodiscard]] aistore::http::HttpResponse handle_put_storage_node(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view node_id) {
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
                                      {"error", "invalid_json"},
                                  });
    }

    const boost::json::object& body = parsed.as_object();

    if (!json_object_has_exact_keys(body, {"address", "port", "state"}) || !body.at("address").is_string() ||
        !body.at("state").is_string()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const std::optional<std::uint64_t> port = extract_positive_uint64(body.at("port"));

    if (!port.has_value() || *port > 65535U) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    try {
        const aistore::metadata::StorageNode node{
            .node_id = std::string{node_id},
            .address = std::string{body.at("address").as_string()},
            .port = static_cast<std::uint16_t>(*port),
            .state = aistore::metadata::storage_node_state_from_string(std::string{body.at("state").as_string()}),
        };

        {
            const std::scoped_lock lock{repository_mutex};
            repository.register_storage_node(node);
        }

        return make_json_response(request, beast_http::status::ok, storage_node_to_json(node));
    } catch (const std::invalid_argument&) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }
}

[[nodiscard]] aistore::http::HttpResponse handle_get_multi_node_restore_plan(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view version_id) {
    try {
        const std::scoped_lock lock{repository_mutex};
        const aistore::metadata::MultiNodeRestorePlan plan = repository.resolve_multi_node_restore_plan(version_id);
        return make_json_response(request, beast_http::status::ok, multi_node_restore_plan_to_json(plan));
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

[[nodiscard]] aistore::http::HttpResponse handle_start_replication_run(
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
                                      {"error", "invalid_json"},
                                  });
    }

    const boost::json::object& body = parsed.as_object();

    if (!json_object_has_exact_keys(body, {"replication_run_id", "version_id", "replication_factor"}) ||
        !body.at("replication_run_id").is_string() || !body.at("version_id").is_string()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const std::optional<std::uint64_t> optional_replication_factor =
        extract_positive_uint64(body.at("replication_factor"));

    if (!optional_replication_factor.has_value() || *optional_replication_factor > 8U) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const auto replication_factor = static_cast<std::uint8_t>(*optional_replication_factor);

    try {
        const aistore::metadata::UuidV7 run_id{std::string{body.at("replication_run_id").as_string()}};
        const std::scoped_lock lock{repository_mutex};
        const aistore::metadata::ReplicationRun run = repository.start_replication_run(
            run_id, std::string{body.at("version_id").as_string()}, replication_factor);
        return make_json_response(request, beast_http::status::ok, replication_run_to_json(run));
    } catch (const aistore::metadata::ReplicationError& error) {
        switch (error.kind()) {
            case aistore::metadata::ReplicationErrorKind::RunConflict:
                return make_json_response(request, beast_http::status::conflict,
                                          boost::json::object{
                                              {"error", "replication_run_conflict"},
                                          });

            case aistore::metadata::ReplicationErrorKind::AnotherRunOpen:
                return make_json_response(request, beast_http::status::conflict,
                                          boost::json::object{
                                              {"error", "replication_already_in_progress"},
                                          });

            case aistore::metadata::ReplicationErrorKind::GcInProgress:
                return make_json_response(request, beast_http::status::conflict,
                                          boost::json::object{
                                              {"error", "gc_in_progress"},
                                          });

            case aistore::metadata::ReplicationErrorKind::VersionNotFound:
                return make_json_response(request, beast_http::status::not_found,
                                          boost::json::object{
                                              {"error", "artifact_version_not_found"},
                                          });

            case aistore::metadata::ReplicationErrorKind::VersionNotCommitted:
                return make_json_response(request, beast_http::status::conflict,
                                          boost::json::object{
                                              {"error", "artifact_version_not_committed"},
                                          });

            case aistore::metadata::ReplicationErrorKind::SourceUnavailable:
                return make_json_response(request, beast_http::status::conflict,
                                          boost::json::object{
                                              {"error", "restore_source_unavailable"},
                                          });

            case aistore::metadata::ReplicationErrorKind::UnderReplicated:
                return make_json_response(request, beast_http::status::conflict,
                                          boost::json::object{
                                              {"error", "chunk_under_replicated"},
                                          });

            case aistore::metadata::ReplicationErrorKind::TargetDisabled:
                return make_json_response(request, beast_http::status::conflict,
                                          boost::json::object{
                                              {"error", "replication_target_disabled"},
                                          });

            default:
                return make_json_response(request, beast_http::status::conflict,
                                          boost::json::object{
                                              {"error", "replication_error"},
                                          });
        }
    } catch (const std::invalid_argument&) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }
}

[[nodiscard]] aistore::http::HttpResponse handle_get_replication_run(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view run_id_text) {
    const aistore::metadata::UuidV7 run_id{std::string{run_id_text}};
    std::optional<aistore::metadata::ReplicationRun> run;

    {
        const std::scoped_lock lock{repository_mutex};
        run = repository.get_replication_run(run_id);
    }

    if (!run.has_value()) {
        return make_json_response(request, beast_http::status::not_found,
                                  boost::json::object{
                                      {"error", "replication_run_not_found"},
                                  });
    }

    return make_json_response(request, beast_http::status::ok, replication_run_to_json(*run));
}

[[nodiscard]] aistore::http::HttpResponse handle_get_replication_plan(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view run_id_text) {
    const aistore::metadata::UuidV7 run_id{std::string{run_id_text}};

    try {
        const std::scoped_lock lock{repository_mutex};
        const aistore::metadata::ReplicationPlan plan = repository.get_replication_plan(run_id);

        boost::json::array placement_array;
        for (const std::string& node_id : plan.placement_node_ids) {
            placement_array.push_back(boost::json::value(node_id));
        }

        boost::json::array chunks;
        for (const aistore::metadata::ReplicationChunkPlan& chunk : plan.chunks) {
            boost::json::array desired;
            for (const std::string& node_id : chunk.desired_node_ids) {
                desired.push_back(boost::json::value(node_id));
            }

            boost::json::array source_nodes;
            for (const aistore::metadata::ReplicationNodeEndpoint& endpoint : chunk.source_nodes) {
                source_nodes.push_back(boost::json::object{
                    {"node_id", endpoint.node_id},
                    {"address", endpoint.address},
                    {"port", endpoint.port},
                });
            }

            boost::json::array target_nodes;
            for (const aistore::metadata::ReplicationNodeEndpoint& endpoint : chunk.target_nodes) {
                target_nodes.push_back(boost::json::object{
                    {"node_id", endpoint.node_id},
                    {"address", endpoint.address},
                    {"port", endpoint.port},
                });
            }

            chunks.push_back(boost::json::object{
                {"chunk_id", chunk.chunk_id},
                {"offset", chunk.offset},
                {"size_bytes", chunk.size_bytes},
                {"desired_node_ids", std::move(desired)},
                {"source_nodes", std::move(source_nodes)},
                {"target_nodes", std::move(target_nodes)},
            });
        }

        return make_json_response(request, beast_http::status::ok,
                                  boost::json::object{
                                      {"replication_run_id", plan.run_id.str()},
                                      {"version_id", plan.version_id},
                                      {"layout_id", plan.layout_id},
                                      {"replication_factor", static_cast<std::uint64_t>(plan.replication_factor)},
                                      {"placement_node_ids", std::move(placement_array)},
                                      {"chunks", std::move(chunks)},
                                  });
    } catch (const aistore::metadata::ReplicationError& error) {
        if (error.kind() == aistore::metadata::ReplicationErrorKind::RunNotFound) {
            return make_json_response(request, beast_http::status::not_found,
                                      boost::json::object{
                                          {"error", "replication_run_not_found"},
                                      });
        }

        if (error.kind() == aistore::metadata::ReplicationErrorKind::TargetDisabled) {
            return make_json_response(request, beast_http::status::conflict,
                                      boost::json::object{
                                          {"error", "replication_target_disabled"},
                                      });
        }

        return make_json_response(request, beast_http::status::conflict,
                                  boost::json::object{
                                      {"error", "replication_error"},
                                  });
    }
}

[[nodiscard]] aistore::http::HttpResponse handle_complete_replication_run(
    const aistore::http::HttpRequest& request, aistore::metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex, std::string_view run_id_text) {
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
                                      {"error", "invalid_json"},
                                  });
    }

    const boost::json::object& body = parsed.as_object();

    if (!json_object_has_exact_keys(body, {"chunks_scanned", "chunks_under_replicated", "replicas_verified",
                                           "replicas_written", "bytes_copied", "source_failovers"})) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const auto extract_stat = [&](std::string_view key) -> std::optional<std::uint64_t> {
        return extract_nonnegative_uint64(body.at(key));
    };

    const std::optional<std::uint64_t> chunks_scanned = extract_stat("chunks_scanned");
    const std::optional<std::uint64_t> chunks_under_replicated = extract_stat("chunks_under_replicated");
    const std::optional<std::uint64_t> replicas_verified = extract_stat("replicas_verified");
    const std::optional<std::uint64_t> replicas_written = extract_stat("replicas_written");
    const std::optional<std::uint64_t> bytes_copied = extract_stat("bytes_copied");
    const std::optional<std::uint64_t> source_failovers = extract_stat("source_failovers");

    if (!chunks_scanned.has_value() || !chunks_under_replicated.has_value() || !replicas_verified.has_value() ||
        !replicas_written.has_value() || !bytes_copied.has_value() || !source_failovers.has_value()) {
        return make_json_response(request, beast_http::status::bad_request,
                                  boost::json::object{
                                      {"error", "invalid_request"},
                                  });
    }

    const aistore::metadata::UuidV7 run_id{std::string{run_id_text}};

    try {
        const std::scoped_lock lock{repository_mutex};
        const aistore::metadata::ReplicationRun completed =
            repository.complete_replication_run(run_id, aistore::metadata::ReplicationStats{
                                                            .chunks_scanned = *chunks_scanned,
                                                            .chunks_under_replicated = *chunks_under_replicated,
                                                            .replicas_verified = *replicas_verified,
                                                            .replicas_written = *replicas_written,
                                                            .bytes_copied = *bytes_copied,
                                                            .source_failovers = *source_failovers,
                                                        });
        return make_json_response(request, beast_http::status::ok, replication_run_to_json(completed));
    } catch (const aistore::metadata::ReplicationError& error) {
        switch (error.kind()) {
            case aistore::metadata::ReplicationErrorKind::RunNotFound:
                return make_json_response(request, beast_http::status::not_found,
                                          boost::json::object{
                                              {"error", "replication_run_not_found"},
                                          });

            case aistore::metadata::ReplicationErrorKind::RunNotOpen:
                return make_json_response(request, beast_http::status::conflict,
                                          boost::json::object{
                                              {"error", "replication_run_not_open"},
                                          });

            case aistore::metadata::ReplicationErrorKind::GcInProgress:
                return make_json_response(request, beast_http::status::conflict,
                                          boost::json::object{
                                              {"error", "gc_in_progress"},
                                          });

            case aistore::metadata::ReplicationErrorKind::UnderReplicated:
                return make_json_response(request, beast_http::status::conflict,
                                          boost::json::object{
                                              {"error", "chunk_under_replicated"},
                                          });

            default:
                return make_json_response(request, beast_http::status::conflict,
                                          boost::json::object{
                                              {"error", "replication_error"},
                                          });
        }
    }
}

}  // namespace

std::optional<aistore::http::HttpResponse> try_handle_m8_routes(const aistore::http::HttpRequest& request,
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

    if (path == kStorageNodesCollection) {
        if (request.method() == beast_http::verb::get) {
            return handle_list_storage_nodes(request, repository, repository_mutex);
        }

        return make_method_not_allowed(request, "GET");
    }

    if (path.starts_with(kStorageNodesPrefix)) {
        const std::string_view node_id = path.substr(kStorageNodesPrefix.size());

        if (node_id.empty() || node_id.find('/') != std::string_view::npos || !is_valid_node_id(node_id)) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_node_id"},
                                      });
        }

        if (query_or_fragment != std::string_view::npos && target[query_or_fragment] != '?') {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_request"},
                                      });
        }

        if (request.method() == beast_http::verb::get) {
            return handle_get_storage_node(request, repository, repository_mutex, node_id);
        }

        if (request.method() == beast_http::verb::put) {
            return handle_put_storage_node(request, repository, repository_mutex, node_id);
        }

        return make_method_not_allowed(request, "GET, PUT");
    }

    if (path == kReplicationRunsCollection) {
        if (request.method() == beast_http::verb::post) {
            return handle_start_replication_run(request, repository, repository_mutex);
        }

        return make_method_not_allowed(request, "POST");
    }

    if (path.starts_with(kReplicationRunsPrefix)) {
        const std::string_view remainder = path.substr(kReplicationRunsPrefix.size());
        const std::size_t slash = remainder.find('/');

        if (slash == std::string_view::npos) {
            if (request.method() == beast_http::verb::get) {
                return handle_get_replication_run(request, repository, repository_mutex, remainder);
            }

            return make_method_not_allowed(request, "GET");
        }

        const std::string_view run_id = remainder.substr(0, slash);
        const std::string_view subroute = remainder.substr(slash);

        if (subroute == kPlanSuffix) {
            if (request.method() == beast_http::verb::get) {
                return handle_get_replication_plan(request, repository, repository_mutex, run_id);
            }

            return make_method_not_allowed(request, "GET");
        }

        if (subroute == kCompleteSuffix) {
            if (request.method() == beast_http::verb::post) {
                return handle_complete_replication_run(request, repository, repository_mutex, run_id);
            }

            return make_method_not_allowed(request, "POST");
        }
    }

    if (path.starts_with(kArtifactVersionsPrefix)) {
        const std::string_view after_prefix = path.substr(kArtifactVersionsPrefix.size());
        const std::size_t restore_pos = after_prefix.find(kRestorePlanSuffix);

        if (restore_pos == std::string_view::npos) {
            return std::nullopt;
        }

        const std::string_view version_id = after_prefix.substr(0, restore_pos);
        const std::string_view after_restore = after_prefix.substr(restore_pos + kRestorePlanSuffix.size());

        if (!is_valid_chunk_id(version_id)) {
            return make_json_response(request, beast_http::status::bad_request,
                                      boost::json::object{
                                          {"error", "invalid_version_id"},
                                      });
        }

        if (after_restore.empty()) {
            if (request.method() != beast_http::verb::get) {
                return make_method_not_allowed(request, "GET");
            }

            const std::optional<std::string_view> source_node_id = parse_query_param(query, "source_node_id");

            if (source_node_id.has_value()) {
                if (!is_valid_node_id(*source_node_id)) {
                    return make_json_response(request, beast_http::status::bad_request,
                                              boost::json::object{
                                                  {"error", "invalid_node_id"},
                                              });
                }

                try {
                    const std::scoped_lock lock{repository_mutex};
                    const aistore::metadata::RestorePlan plan =
                        repository.resolve_restore_plan(version_id, *source_node_id);

                    const aistore::metadata::ObjectLayoutDescriptor& descriptor = plan.layout_descriptor;
                    boost::json::array chunks;

                    for (const aistore::metadata::ChunkRef& chunk : descriptor.layout().chunks()) {
                        chunks.push_back(boost::json::object{
                            {"chunk_id", chunk.chunk_id},
                            {"offset", chunk.offset},
                            {"size_bytes", chunk.size},
                        });
                    }

                    return make_json_response(request, beast_http::status::ok,
                                              boost::json::object{
                                                  {"version_id", plan.version_id},
                                                  {"artifact_id", plan.artifact_id.str()},
                                                  {"source_node_id", plan.source_node_id},
                                                  {"object_id", descriptor.object_id()},
                                                  {"total_size_bytes", descriptor.object().total_size()},
                                                  {"layout_id", descriptor.layout_id()},
                                                  {"chunking_strategy", aistore::metadata::chunking_strategy_to_string(
                                                                            descriptor.chunking_strategy())},
                                                  {"chunking_parameters", chunking_parameters_to_json(descriptor)},
                                                  {"chunk_count", chunks.size()},
                                                  {"chunks", std::move(chunks)},
                                              });
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

            return handle_get_multi_node_restore_plan(request, repository, repository_mutex, version_id);
        }

        if (after_restore.starts_with('/')) {
            const std::string_view source_node_id = after_restore.substr(1U);

            if (source_node_id.find('/') != std::string_view::npos) {
                return make_json_response(request, beast_http::status::not_found,
                                          boost::json::object{
                                              {"error", "not_found"},
                                          });
            }

            return std::nullopt;
        }
    }

    return std::nullopt;
}

}  // namespace aistore::service
