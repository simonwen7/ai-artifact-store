#include <gtest/gtest.h>

#include <array>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <pqxx/pqxx>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "aistore/hashing/sha256.hpp"
#include "aistore/http/http_server.hpp"
#include "aistore/metadata/artifact_model.hpp"
#include "aistore/metadata/chunking.hpp"
#include "aistore/metadata/lifecycle.hpp"
#include "aistore/metadata/object.hpp"
#include "aistore/metadata/object_layout.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/postgres_metadata_repository.hpp"
#include "aistore/metadata/storage_location.hpp"
#include "aistore/metadata/storage_node.hpp"
#include "aistore/metadata/upload_session.hpp"
#include "aistore/metadata/uuid_v7.hpp"
#include "aistore/service/metadata_service.hpp"

namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace beast_http = beast::http;

using tcp = asio::ip::tcp;

using aistore::hashing::Sha256;
using aistore::http::HttpRequest;
using aistore::http::HttpResponse;
using aistore::http::HttpServer;
using aistore::http::HttpServerConfig;
using aistore::metadata::Artifact;
using aistore::metadata::artifact_kind_to_string;
using aistore::metadata::ArtifactKind;
using aistore::metadata::ArtifactVersion;
using aistore::metadata::ChunkingStrategy;
using aistore::metadata::ChunkMetadata;
using aistore::metadata::ChunkRef;
using aistore::metadata::Object;
using aistore::metadata::ObjectLayout;
using aistore::metadata::ObjectLayoutDescriptor;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::StorageLocation;
using aistore::metadata::StorageLocationState;
using aistore::metadata::StorageNode;
using aistore::metadata::UploadSession;
using aistore::metadata::UploadSessionState;
using aistore::metadata::UuidV7;
using aistore::metadata::VersionState;
using aistore::service::MetadataService;

constexpr std::uint64_t kMaxRequestBodyBytes = 8ULL * 1024ULL * 1024ULL;
constexpr std::uint64_t kChunkSizeBytes = 4194304ULL;

std::string test_database_connection_string() {
    const char* configured = std::getenv("AISTORE_TEST_DB_URL");

    if (configured != nullptr && configured[0] != '\0') {
        return configured;
    }

    return "dbname=ai_artifact_store_test";
}

void ensure_lifecycle_migration_applied() {
    pqxx::connection connection{test_database_connection_string()};

    auto apply_migration_if_missing = [&](int version, std::string_view name, std::string_view filename) {
        {
            pqxx::nontransaction check{connection};

            const bool migration_applied = check.query_value<bool>(
                "SELECT EXISTS ("
                "    SELECT 1 "
                "    FROM schema_migrations "
                "    WHERE version = $1 "
                "      AND name = $2"
                ")",
                pqxx::params{
                    version,
                    std::string{name},
                });

            if (migration_applied) {
                return;
            }
        }

        const std::filesystem::path migration_path =
            std::filesystem::path{__FILE__}.parent_path().parent_path().parent_path() / "migrations" / filename;

        std::ifstream migration_file{migration_path};

        if (!migration_file.is_open()) {
            throw std::runtime_error(std::string{"failed to open migration "} + std::string{filename});
        }

        std::string migration_sql{
            std::istreambuf_iterator<char>{migration_file},
            std::istreambuf_iterator<char>{},
        };

        pqxx::nontransaction apply{connection};
        apply.exec(migration_sql);
    };

    apply_migration_if_missing(10, "ai_aware_lifecycle", "010_ai_aware_lifecycle.sql");
    apply_migration_if_missing(11, "retired_finalization_reclamation", "011_retired_finalization_reclamation.sql");
}

[[nodiscard]] std::uint64_t json_uint64(const boost::json::value& value) {
    if (value.is_uint64()) {
        return value.as_uint64();
    }

    if (value.is_int64()) {
        const std::int64_t signed_value = value.as_int64();
        EXPECT_GE(signed_value, 0);
        return static_cast<std::uint64_t>(signed_value);
    }

    ADD_FAILURE() << "expected JSON integer";
    return 0;
}

std::string sha256_hex(std::string_view text) {
    Sha256 hasher;
    hasher.update(std::as_bytes(std::span{text.data(), text.size()}));
    const Sha256::Digest digest = hasher.finalize();

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

HttpResponse http_exchange(std::uint16_t port, beast_http::verb method, std::string_view target,
                           std::optional<std::string> body = std::nullopt,
                           std::optional<std::string_view> content_type = std::nullopt) {
    asio::io_context ioc;

    tcp::resolver resolver{ioc};
    beast::tcp_stream stream{ioc};

    const auto endpoints = resolver.resolve("127.0.0.1", std::to_string(port));
    stream.connect(endpoints);

    HttpRequest request{
        method,
        target,
        11,
    };
    request.set(beast_http::field::host, "127.0.0.1");
    request.set(beast_http::field::user_agent, "aistore-metadata-service-m9-api-test");

    if (content_type.has_value()) {
        request.set(beast_http::field::content_type, *content_type);
    }

    if (body.has_value()) {
        request.body() = std::move(*body);
    }

    request.prepare_payload();

    beast_http::write(stream, request);

    beast::flat_buffer buffer;
    HttpResponse response;
    beast_http::read(stream, buffer, response);

    beast::error_code ignored;
    stream.socket().shutdown(tcp::socket::shutdown_both, ignored);

    return response;
}

class RunningHttpServer {
   public:
    explicit RunningHttpServer(MetadataService& service)
        : server_(
              HttpServerConfig{
                  .bind_address = "127.0.0.1",
                  .port = 0,
                  .worker_threads = 2,
                  .max_request_body_bytes = kMaxRequestBodyBytes,
              },
              [&service](const HttpRequest& request) { return service.handle_request(request); }),
          worker_([this] { server_.run(); }) {}

    ~RunningHttpServer() {
        server_.stop();

        if (worker_.joinable()) {
            worker_.join();
        }
    }

    RunningHttpServer(const RunningHttpServer&) = delete;

    RunningHttpServer& operator=(const RunningHttpServer&) = delete;

    [[nodiscard]] std::uint16_t port() const noexcept { return server_.port(); }

   private:
    HttpServer server_;
    std::thread worker_;
};

ObjectLayoutDescriptor make_m9_layout(std::string_view object_seed, std::string_view chunk_seed) {
    const std::string chunk_id = sha256_hex(chunk_seed);
    const std::string object_id = sha256_hex(std::string{object_seed} + chunk_id);
    return ObjectLayoutDescriptor{
        Object{object_id, 4},
        ChunkingStrategy::FixedSize,
        ObjectLayout{{
            ChunkRef{.chunk_id = chunk_id, .offset = 0, .size = 4},
        }},
    };
}

void register_storage_node(PostgresMetadataRepository& repository, std::string node_id) {
    repository.register_storage_node(StorageNode{
        .node_id = std::move(node_id),
        .address = "127.0.0.1",
        .port = 9101,
        .state = aistore::metadata::StorageNodeState::Active,
    });
}

void register_chunk_on_node(PostgresMetadataRepository& repository, const ChunkRef& chunk, std::string_view node_id) {
    repository.register_chunks({ChunkMetadata{.chunk_id = chunk.chunk_id, .size_bytes = chunk.size}});
    repository.register_storage_location(StorageLocation{
        .chunk_id = chunk.chunk_id,
        .node_id = std::string{node_id},
        .storage_path = std::string{"/m9-http/"} + chunk.chunk_id,
        .state = StorageLocationState::Available,
    });
}

ArtifactVersion register_committed_version(PostgresMetadataRepository& repository, const UuidV7& artifact_id,
                                           const ObjectLayoutDescriptor& descriptor, std::string marker) {
    repository.register_object(descriptor.object());
    repository.register_object_layout(descriptor);

    ArtifactVersion version{
        artifact_id,
        descriptor.object_id(),
        std::nullopt,
        ArtifactVersion::ImmutableMetadata{{"marker", std::move(marker)}},
        VersionState::Committed,
    };
    repository.create_version(version);

    return version;
}

std::string make_lifecycle_policy_json(const UuidV7& policy_id, std::string_view name, std::uint32_t keep_last_n) {
    boost::json::array rules;

    for (const ArtifactKind kind : {ArtifactKind::Generic, ArtifactKind::ModelCheckpoint, ArtifactKind::DatasetSnapshot,
                                    ArtifactKind::EmbeddingIndex, ArtifactKind::EvaluationOutput}) {
        rules.push_back(boost::json::object{
            {"artifact_kind", std::string{artifact_kind_to_string(kind)}},
            {"keep_last_n", keep_last_n},
            {"max_age_seconds", nullptr},
        });
    }

    return boost::json::serialize(boost::json::object{
        {"policy_id", policy_id.str()},
        {"name", std::string{name}},
        {"rules", std::move(rules)},
    });
}

void expect_lifecycle_rule_shape(const boost::json::object& rule) {
    EXPECT_EQ(rule.size(), 3U);
    EXPECT_TRUE(rule.at("artifact_kind").is_string());
    EXPECT_TRUE(rule.at("keep_last_n").is_int64() || rule.at("keep_last_n").is_uint64());
    EXPECT_TRUE(rule.at("max_age_seconds").is_null() || rule.at("max_age_seconds").is_int64() ||
                rule.at("max_age_seconds").is_uint64());
}

void expect_lifecycle_policy_shape(const boost::json::object& body, const UuidV7& policy_id, std::string_view name) {
    EXPECT_EQ(body.size(), 3U);
    EXPECT_EQ(body.at("policy_id").as_string(), policy_id.str());
    EXPECT_EQ(body.at("name").as_string(), name);
    const boost::json::array& rules = body.at("rules").as_array();
    ASSERT_EQ(rules.size(), 5U);
    for (const boost::json::value& entry : rules) {
        ASSERT_TRUE(entry.is_object());
        expect_lifecycle_rule_shape(entry.as_object());
    }
}

void expect_lifecycle_run_shape(const boost::json::object& body, const UuidV7& run_id, const UuidV7& policy_id,
                                std::string_view mode) {
    EXPECT_EQ(body.size(), 11U);
    EXPECT_EQ(body.at("run_id").as_string(), run_id.str());
    EXPECT_EQ(body.at("policy_id").as_string(), policy_id.str());
    EXPECT_EQ(body.at("mode").as_string(), mode);
    EXPECT_TRUE(body.at("evaluated_at_unix_ms").is_int64() || body.at("evaluated_at_unix_ms").is_uint64());
    EXPECT_GT(json_uint64(body.at("evaluated_at_unix_ms")), 0U);
    (void)json_uint64(body.at("versions_scanned"));
    (void)json_uint64(body.at("versions_protected"));
    (void)json_uint64(body.at("versions_retained_by_policy"));
    (void)json_uint64(body.at("versions_candidates"));
    (void)json_uint64(body.at("versions_retired"));
    (void)json_uint64(body.at("logical_bytes_candidates"));
    (void)json_uint64(body.at("logical_bytes_retired"));
}

void expect_lifecycle_decision_shape(const boost::json::object& decision) {
    EXPECT_EQ(decision.size(), 6U);
    EXPECT_TRUE(decision.at("version_id").is_string());
    EXPECT_TRUE(decision.at("artifact_id").is_string());
    EXPECT_TRUE(decision.at("artifact_kind").is_string());
    EXPECT_TRUE(decision.at("decision").is_string());
    EXPECT_TRUE(decision.at("reason").is_string());
    EXPECT_TRUE(decision.at("logical_size_bytes").is_int64() || decision.at("logical_size_bytes").is_uint64());
}

class MetadataServiceM9ApiTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ensure_lifecycle_migration_applied();
        repository_.emplace(test_database_connection_string());
        {
            pqxx::connection connection{test_database_connection_string()};
            pqxx::work transaction{connection};
            transaction
                .exec(
                    "DELETE FROM upload_session_finalizations WHERE session_id IN "
                    "(SELECT session_id FROM upload_sessions WHERE state = 'open')")
                .no_rows();
            transaction
                .exec(
                    "DELETE FROM upload_session_metadata WHERE session_id IN "
                    "(SELECT session_id FROM upload_sessions WHERE state = 'open')")
                .no_rows();
            transaction.exec("DELETE FROM upload_sessions WHERE state = 'open'").no_rows();
            transaction.exec("DELETE FROM replication_runs").no_rows();
            transaction.exec("DELETE FROM gc_runs").no_rows();
            transaction.exec("DELETE FROM lifecycle_run_decisions").no_rows();
            transaction.exec("DELETE FROM artifact_version_retirements").no_rows();
            transaction.exec("DELETE FROM artifact_version_pins").no_rows();
            transaction.exec("DELETE FROM lifecycle_runs").no_rows();
            transaction.exec("DELETE FROM lifecycle_policy_rules").no_rows();
            transaction.exec("DELETE FROM lifecycle_policies").no_rows();
            transaction.exec("DELETE FROM storage_nodes").no_rows();
            transaction.commit();
        }
        service_.emplace(*repository_);
        server_.emplace(*service_);
    }

    void TearDown() override {
        server_.reset();
        service_.reset();

        if (repository_.has_value()) {
            cleanup_owned_rows();
            repository_.reset();
        }
    }

    void cleanup_owned_rows() {
        pqxx::connection connection{test_database_connection_string()};
        pqxx::work transaction{connection};

        for (const UuidV7& run_id : owned_lifecycle_run_ids_) {
            transaction.exec("DELETE FROM lifecycle_run_decisions WHERE run_id = $1::uuid", pqxx::params{run_id.str()})
                .no_rows();
            transaction
                .exec("DELETE FROM artifact_version_retirements WHERE lifecycle_run_id = $1::uuid",
                      pqxx::params{run_id.str()})
                .no_rows();
            transaction.exec("DELETE FROM lifecycle_runs WHERE run_id = $1::uuid", pqxx::params{run_id.str()})
                .no_rows();
        }

        for (const UuidV7& policy_id : owned_lifecycle_policy_ids_) {
            transaction
                .exec("DELETE FROM lifecycle_policy_rules WHERE policy_id = $1::uuid", pqxx::params{policy_id.str()})
                .no_rows();
            transaction.exec("DELETE FROM lifecycle_policies WHERE policy_id = $1::uuid", pqxx::params{policy_id.str()})
                .no_rows();
        }

        for (const UuidV7& replication_run_id : owned_replication_run_ids_) {
            transaction
                .exec("DELETE FROM replication_runs WHERE run_id = $1::uuid", pqxx::params{replication_run_id.str()})
                .no_rows();
        }

        for (const UuidV7& gc_run_id : owned_gc_run_ids_) {
            transaction.exec("DELETE FROM gc_runs WHERE run_id = $1::uuid", pqxx::params{gc_run_id.str()}).no_rows();
        }

        for (const UuidV7& session_id : owned_session_ids_) {
            transaction.exec("DELETE FROM upload_sessions WHERE session_id = $1::uuid", pqxx::params{session_id.str()})
                .no_rows();
        }

        for (const UuidV7& artifact_id : owned_artifact_ids_) {
            transaction
                .exec("DELETE FROM artifact_versions WHERE artifact_id = $1::uuid", pqxx::params{artifact_id.str()})
                .no_rows();
            transaction.exec("DELETE FROM artifacts WHERE artifact_id = $1::uuid", pqxx::params{artifact_id.str()})
                .no_rows();
        }

        for (const std::string& object_id : owned_object_ids_) {
            transaction
                .exec(
                    "DELETE FROM object_layout_chunks WHERE layout_id IN "
                    "(SELECT layout_id FROM object_layouts WHERE object_id = $1)",
                    pqxx::params{object_id})
                .no_rows();
            transaction.exec("DELETE FROM object_layouts WHERE object_id = $1", pqxx::params{object_id}).no_rows();
            transaction.exec("DELETE FROM objects WHERE object_id = $1", pqxx::params{object_id}).no_rows();
        }

        transaction.exec("DELETE FROM storage_locations WHERE node_id LIKE 'm9-http-%'").no_rows();
        transaction.exec("DELETE FROM storage_nodes").no_rows();
        transaction.commit();
    }

    void track_lifecycle_policy(UuidV7 policy_id) { owned_lifecycle_policy_ids_.push_back(std::move(policy_id)); }

    void track_lifecycle_run(UuidV7 run_id) { owned_lifecycle_run_ids_.push_back(std::move(run_id)); }

    void track_replication_run(UuidV7 run_id) { owned_replication_run_ids_.push_back(std::move(run_id)); }

    void track_gc_run(UuidV7 gc_run_id) { owned_gc_run_ids_.push_back(std::move(gc_run_id)); }

    void track_session(UuidV7 session_id) { owned_session_ids_.push_back(std::move(session_id)); }

    void track_artifact(UuidV7 artifact_id) { owned_artifact_ids_.push_back(std::move(artifact_id)); }

    void track_object(std::string object_id) { owned_object_ids_.push_back(std::move(object_id)); }

    std::vector<UuidV7> owned_lifecycle_policy_ids_;
    std::vector<UuidV7> owned_lifecycle_run_ids_;
    std::vector<UuidV7> owned_replication_run_ids_;
    std::vector<UuidV7> owned_gc_run_ids_;
    std::vector<UuidV7> owned_session_ids_;
    std::vector<UuidV7> owned_artifact_ids_;
    std::vector<std::string> owned_object_ids_;
    std::optional<PostgresMetadataRepository> repository_;
    std::optional<MetadataService> service_;
    std::optional<RunningHttpServer> server_;
};

TEST_F(MetadataServiceM9ApiTest, LifecyclePolicyHttpContract) {
    const UuidV7 policy_id = UuidV7::generate();
    track_lifecycle_policy(policy_id);

    const std::string register_payload = make_lifecycle_policy_json(policy_id, "m9-http-policy", 2U);
    const HttpResponse registered = http_exchange(server_->port(), beast_http::verb::post, "/v1/lifecycle-policies",
                                                  register_payload, "application/json");

    ASSERT_EQ(registered.result(), beast_http::status::ok);
    const boost::json::object registered_body = boost::json::parse(registered.body()).as_object();
    expect_lifecycle_policy_shape(registered_body, policy_id, "m9-http-policy");
    EXPECT_EQ(json_uint64(registered_body.at("rules").as_array().at(0).as_object().at("keep_last_n")), 2U);
    EXPECT_TRUE(registered_body.at("rules").as_array().at(0).as_object().at("max_age_seconds").is_null());

    const HttpResponse loaded =
        http_exchange(server_->port(), beast_http::verb::get, std::string{"/v1/lifecycle-policies/"} + policy_id.str());
    ASSERT_EQ(loaded.result(), beast_http::status::ok);
    expect_lifecycle_policy_shape(boost::json::parse(loaded.body()).as_object(), policy_id, "m9-http-policy");

    const HttpResponse missing =
        http_exchange(server_->port(), beast_http::verb::get, "/v1/lifecycle-policies/" + UuidV7::generate().str());
    EXPECT_EQ(missing.result(), beast_http::status::not_found);
    EXPECT_EQ(boost::json::parse(missing.body()).at("error").as_string(), "lifecycle_policy_not_found");
}

TEST_F(MetadataServiceM9ApiTest, LifecyclePinHttpContract) {
    const UuidV7 artifact_id = UuidV7::generate();
    track_artifact(artifact_id);

    register_storage_node(*repository_, "m9-http-node-a");
    repository_->create_artifact(Artifact{artifact_id, "m9-http-pin-" + artifact_id.str(), "m9-http-project"});

    const auto descriptor = make_m9_layout("m9-http-pin-object", "m9-http-pin-chunk");
    track_object(descriptor.object_id());
    register_chunk_on_node(*repository_, descriptor.layout().chunks().front(), "m9-http-node-a");

    const ArtifactVersion version =
        register_committed_version(*repository_, artifact_id, descriptor, artifact_id.str());

    const std::string pin_payload = boost::json::serialize(boost::json::object{
        {"reason", "m9-http-retention"},
    });
    const HttpResponse pinned = http_exchange(server_->port(), beast_http::verb::put,
                                              std::string{"/v1/artifact-versions/"} + version.version_id() + "/pin",
                                              pin_payload, "application/json");

    ASSERT_EQ(pinned.result(), beast_http::status::ok);
    const boost::json::object pinned_body = boost::json::parse(pinned.body()).as_object();
    EXPECT_EQ(pinned_body.size(), 3U);
    EXPECT_EQ(pinned_body.at("version_id").as_string(), version.version_id());
    EXPECT_TRUE(pinned_body.at("pinned").as_bool());
    EXPECT_EQ(pinned_body.at("reason").as_string(), "m9-http-retention");

    const HttpResponse loaded = http_exchange(server_->port(), beast_http::verb::get,
                                              std::string{"/v1/artifact-versions/"} + version.version_id() + "/pin");
    ASSERT_EQ(loaded.result(), beast_http::status::ok);
    const boost::json::object loaded_body = boost::json::parse(loaded.body()).as_object();
    EXPECT_EQ(loaded_body.size(), 3U);
    EXPECT_TRUE(loaded_body.at("pinned").as_bool());
    EXPECT_EQ(loaded_body.at("reason").as_string(), "m9-http-retention");

    const HttpResponse unpinned = http_exchange(server_->port(), beast_http::verb::delete_,
                                                std::string{"/v1/artifact-versions/"} + version.version_id() + "/pin");
    ASSERT_EQ(unpinned.result(), beast_http::status::ok);
    const boost::json::object unpinned_body = boost::json::parse(unpinned.body()).as_object();
    EXPECT_EQ(unpinned_body.size(), 2U);
    EXPECT_EQ(unpinned_body.at("version_id").as_string(), version.version_id());
    EXPECT_FALSE(unpinned_body.at("pinned").as_bool());

    const HttpResponse cleared = http_exchange(server_->port(), beast_http::verb::get,
                                               std::string{"/v1/artifact-versions/"} + version.version_id() + "/pin");
    ASSERT_EQ(cleared.result(), beast_http::status::ok);
    const boost::json::object cleared_body = boost::json::parse(cleared.body()).as_object();
    EXPECT_EQ(cleared_body.size(), 2U);
    EXPECT_FALSE(cleared_body.at("pinned").as_bool());
}

TEST_F(MetadataServiceM9ApiTest, LifecycleRunHttpContract) {
    const UuidV7 policy_id = UuidV7::generate();
    const UuidV7 run_id = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    track_lifecycle_policy(policy_id);
    track_lifecycle_run(run_id);
    track_artifact(artifact_id);

    register_storage_node(*repository_, "m9-http-node-b");
    repository_->create_artifact(Artifact{artifact_id, "m9-http-run-" + artifact_id.str(), "m9-http-project"});

    const auto descriptor = make_m9_layout("m9-http-run-object", "m9-http-run-chunk");
    track_object(descriptor.object_id());
    register_chunk_on_node(*repository_, descriptor.layout().chunks().front(), "m9-http-node-b");
    (void)register_committed_version(*repository_, artifact_id, descriptor, artifact_id.str());

    const HttpResponse registered =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/lifecycle-policies",
                      make_lifecycle_policy_json(policy_id, "m9-http-run-policy", 1U), "application/json");
    ASSERT_EQ(registered.result(), beast_http::status::ok);

    const std::string run_payload = boost::json::serialize(boost::json::object{
        {"run_id", run_id.str()},
        {"policy_id", policy_id.str()},
        {"mode", "dry-run"},
    });
    const HttpResponse started =
        http_exchange(server_->port(), beast_http::verb::post, "/v1/lifecycle-runs", run_payload, "application/json");

    ASSERT_EQ(started.result(), beast_http::status::ok);
    const boost::json::object started_body = boost::json::parse(started.body()).as_object();
    expect_lifecycle_run_shape(started_body, run_id, policy_id, "dry-run");
    EXPECT_GE(json_uint64(started_body.at("versions_scanned")), 1U);

    const HttpResponse loaded =
        http_exchange(server_->port(), beast_http::verb::get, std::string{"/v1/lifecycle-runs/"} + run_id.str());
    ASSERT_EQ(loaded.result(), beast_http::status::ok);
    expect_lifecycle_run_shape(boost::json::parse(loaded.body()).as_object(), run_id, policy_id, "dry-run");
}

TEST_F(MetadataServiceM9ApiTest, LifecycleDecisionPaginationHttpContract) {
    const UuidV7 policy_id = UuidV7::generate();
    const UuidV7 run_id = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    track_lifecycle_policy(policy_id);
    track_lifecycle_run(run_id);
    track_artifact(artifact_id);

    register_storage_node(*repository_, "m9-http-node-c");
    repository_->create_artifact(Artifact{artifact_id, "m9-http-decisions-" + artifact_id.str(), "m9-http-project"});

    const auto first_descriptor = make_m9_layout("m9-http-decision-object-a", "m9-http-decision-chunk-a");
    const auto second_descriptor = make_m9_layout("m9-http-decision-object-b", "m9-http-decision-chunk-b");
    track_object(first_descriptor.object_id());
    track_object(second_descriptor.object_id());

    for (const ObjectLayoutDescriptor& descriptor : {first_descriptor, second_descriptor}) {
        register_chunk_on_node(*repository_, descriptor.layout().chunks().front(), "m9-http-node-c");
        (void)register_committed_version(*repository_, artifact_id, descriptor, descriptor.object_id());
    }

    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/lifecycle-policies",
                            make_lifecycle_policy_json(policy_id, "m9-http-decision-policy", 1U), "application/json")
                  .result(),
              beast_http::status::ok);

    const std::string run_payload = boost::json::serialize(boost::json::object{
        {"run_id", run_id.str()},
        {"policy_id", policy_id.str()},
        {"mode", "dry-run"},
    });
    ASSERT_EQ(
        http_exchange(server_->port(), beast_http::verb::post, "/v1/lifecycle-runs", run_payload, "application/json")
            .result(),
        beast_http::status::ok);

    const HttpResponse first_page =
        http_exchange(server_->port(), beast_http::verb::get,
                      std::string{"/v1/lifecycle-runs/"} + run_id.str() + "/decisions?limit=1");
    ASSERT_EQ(first_page.result(), beast_http::status::ok);
    const boost::json::object first_body = boost::json::parse(first_page.body()).as_object();
    EXPECT_EQ(first_body.size(), 2U);
    const boost::json::array& first_decisions = first_body.at("decisions").as_array();
    ASSERT_EQ(first_decisions.size(), 1U);
    expect_lifecycle_decision_shape(first_decisions.at(0).as_object());
    ASSERT_TRUE(first_body.at("next_after").is_string());
    const std::string after = std::string{first_body.at("next_after").as_string()};

    const HttpResponse second_page =
        http_exchange(server_->port(), beast_http::verb::get,
                      std::string{"/v1/lifecycle-runs/"} + run_id.str() + "/decisions?limit=1&after=" + after);
    ASSERT_EQ(second_page.result(), beast_http::status::ok);
    const boost::json::object second_body = boost::json::parse(second_page.body()).as_object();
    EXPECT_EQ(second_body.size(), 2U);
    const boost::json::array& second_decisions = second_body.at("decisions").as_array();
    ASSERT_GE(second_decisions.size(), 1U);
    expect_lifecycle_decision_shape(second_decisions.at(0).as_object());
    EXPECT_NE(second_decisions.at(0).at("version_id").as_string(), first_decisions.at(0).at("version_id").as_string());
}

TEST_F(MetadataServiceM9ApiTest, RetiredVersionHttpErrorsAreStrict) {
    const UuidV7 policy_id = UuidV7::generate();
    const UuidV7 retire_run_id = UuidV7::generate();
    const UuidV7 conflict_run_id = UuidV7::generate();
    const UuidV7 replication_run_id = UuidV7::generate();
    const UuidV7 artifact_id = UuidV7::generate();
    const UuidV7 session_id = UuidV7::generate();
    track_lifecycle_policy(policy_id);
    track_lifecycle_run(retire_run_id);
    track_lifecycle_run(conflict_run_id);
    track_replication_run(replication_run_id);
    track_artifact(artifact_id);
    track_session(session_id);

    register_storage_node(*repository_, "m9-http-node-d");
    repository_->create_artifact(Artifact{artifact_id, "m9-http-retired-" + artifact_id.str(), "m9-http-project"});

    const auto descriptor = make_m9_layout("m9-http-retired-object", "m9-http-retired-chunk");
    track_object(descriptor.object_id());
    register_chunk_on_node(*repository_, descriptor.layout().chunks().front(), "m9-http-node-d");

    const ArtifactVersion version =
        register_committed_version(*repository_, artifact_id, descriptor, artifact_id.str());

    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/lifecycle-policies",
                            make_lifecycle_policy_json(policy_id, "m9-http-retire-policy", 0U), "application/json")
                  .result(),
              beast_http::status::ok);

    const std::string retire_payload = boost::json::serialize(boost::json::object{
        {"run_id", retire_run_id.str()},
        {"policy_id", policy_id.str()},
        {"mode", "apply"},
    });
    ASSERT_EQ(
        http_exchange(server_->port(), beast_http::verb::post, "/v1/lifecycle-runs", retire_payload, "application/json")
            .result(),
        beast_http::status::ok);

    const std::string pin_payload = boost::json::serialize(boost::json::object{
        {"reason", "blocked-on-retired"},
    });
    const HttpResponse pin_retired = http_exchange(
        server_->port(), beast_http::verb::put, std::string{"/v1/artifact-versions/"} + version.version_id() + "/pin",
        pin_payload, "application/json");
    EXPECT_EQ(pin_retired.result(), beast_http::status::gone);
    EXPECT_EQ(boost::json::parse(pin_retired.body()).at("error").as_string(), "artifact_version_retired");

    const HttpResponse restore_retired =
        http_exchange(server_->port(), beast_http::verb::get,
                      std::string{"/v1/artifact-versions/"} + version.version_id() + "/restore-plan");
    EXPECT_EQ(restore_retired.result(), beast_http::status::gone);
    EXPECT_EQ(boost::json::parse(restore_retired.body()).at("error").as_string(), "artifact_version_retired");

    const std::string replication_payload = boost::json::serialize(boost::json::object{
        {"replication_run_id", replication_run_id.str()},
        {"version_id", version.version_id()},
        {"replication_factor", 1},
    });
    const HttpResponse replication_retired = http_exchange(
        server_->port(), beast_http::verb::post, "/v1/replication-runs", replication_payload, "application/json");
    EXPECT_EQ(replication_retired.result(), beast_http::status::gone);
    EXPECT_EQ(boost::json::parse(replication_retired.body()).at("error").as_string(), "artifact_version_retired");

    const HttpResponse missing_run = http_exchange(server_->port(), beast_http::verb::get,
                                                   "/v1/lifecycle-runs/" + UuidV7::generate().str() + "/decisions");
    EXPECT_EQ(missing_run.result(), beast_http::status::not_found);
    EXPECT_EQ(boost::json::parse(missing_run.body()).at("error").as_string(), "lifecycle_run_not_found");

    const std::string missing_version = sha256_hex("m9-http-missing-version");
    const HttpResponse missing_version_pin = http_exchange(
        server_->port(), beast_http::verb::get, std::string{"/v1/artifact-versions/"} + missing_version + "/pin");
    EXPECT_EQ(missing_version_pin.result(), beast_http::status::not_found);
    EXPECT_EQ(boost::json::parse(missing_version_pin.body()).at("error").as_string(), "artifact_version_not_found");

    const std::string conflict_start_payload = boost::json::serialize(boost::json::object{
        {"run_id", conflict_run_id.str()},
        {"policy_id", policy_id.str()},
        {"mode", "dry-run"},
    });
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/lifecycle-runs", conflict_start_payload,
                            "application/json")
                  .result(),
              beast_http::status::ok);

    const UuidV7 other_policy_id = UuidV7::generate();
    track_lifecycle_policy(other_policy_id);
    ASSERT_EQ(http_exchange(server_->port(), beast_http::verb::post, "/v1/lifecycle-policies",
                            make_lifecycle_policy_json(other_policy_id, "m9-http-other-policy", 1U), "application/json")
                  .result(),
              beast_http::status::ok);

    const std::string conflict_payload = boost::json::serialize(boost::json::object{
        {"run_id", conflict_run_id.str()},
        {"policy_id", other_policy_id.str()},
        {"mode", "dry-run"},
    });
    const HttpResponse run_conflict = http_exchange(server_->port(), beast_http::verb::post, "/v1/lifecycle-runs",
                                                    conflict_payload, "application/json");
    EXPECT_EQ(run_conflict.result(), beast_http::status::conflict);
    EXPECT_EQ(boost::json::parse(run_conflict.body()).at("error").as_string(), "lifecycle_run_conflict");

    repository_->create_upload_session(UploadSession{
        session_id,
        artifact_id,
        "m9-http-node-d",
        ChunkingStrategy::FixedSize,
        kChunkSizeBytes,
        std::nullopt,
        UploadSession::ImmutableMetadata{{"source", "m9-http-open-session"}},
        UploadSessionState::Open,
        std::nullopt,
    });

    const UuidV7 blocked_run_id = UuidV7::generate();
    track_lifecycle_run(blocked_run_id);
    const std::string blocked_payload = boost::json::serialize(boost::json::object{
        {"run_id", blocked_run_id.str()},
        {"policy_id", policy_id.str()},
        {"mode", "dry-run"},
    });
    const HttpResponse blocked_by_upload = http_exchange(server_->port(), beast_http::verb::post, "/v1/lifecycle-runs",
                                                         blocked_payload, "application/json");
    EXPECT_EQ(blocked_by_upload.result(), beast_http::status::conflict);
    EXPECT_EQ(boost::json::parse(blocked_by_upload.body()).at("error").as_string(),
              "lifecycle_blocked_by_open_upload_sessions");
}

}  // namespace
