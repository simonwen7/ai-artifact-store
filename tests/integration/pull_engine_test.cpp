#include "aistore/pull/pull_engine.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <boost/beast/http.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <optional>
#include <pqxx/pqxx>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "aistore/chunking/fastcdc_chunker.hpp"
#include "aistore/client/client_error.hpp"
#include "aistore/client/metadata_client.hpp"
#include "aistore/client/storage_node_client.hpp"
#include "aistore/hashing/sha256.hpp"
#include "aistore/http/http_client.hpp"
#include "aistore/http/http_server.hpp"
#include "aistore/metadata/artifact_model.hpp"
#include "aistore/metadata/chunk_metadata.hpp"
#include "aistore/metadata/chunking.hpp"
#include "aistore/metadata/object_layout_descriptor.hpp"
#include "aistore/metadata/postgres_metadata_repository.hpp"
#include "aistore/metadata/storage_location.hpp"
#include "aistore/metadata/uuid_v7.hpp"
#include "aistore/service/metadata_service.hpp"
#include "aistore/service/storage_node_service.hpp"
#include "aistore/storage/local_chunk_store.hpp"

namespace {

using aistore::chunking::ChunkBuffer;
using aistore::chunking::FastCdcChunker;
using aistore::client::MetadataClient;
using aistore::client::RemoteApiError;
using aistore::client::StorageNodeClient;
using aistore::http::HttpClientConfig;
using aistore::http::HttpEndpoint;
using aistore::http::HttpRequest;
using aistore::http::HttpResponse;
using aistore::http::HttpServer;
using aistore::http::HttpServerConfig;
using aistore::metadata::Artifact;
using aistore::metadata::ArtifactVersion;
using aistore::metadata::ChunkingStrategy;
using aistore::metadata::ChunkMetadata;
using aistore::metadata::ChunkRef;
using aistore::metadata::FastCdcParameters;
using aistore::metadata::Object;
using aistore::metadata::ObjectLayout;
using aistore::metadata::ObjectLayoutDescriptor;
using aistore::metadata::PostgresMetadataRepository;
using aistore::metadata::StorageLocation;
using aistore::metadata::StorageLocationState;
using aistore::metadata::UuidV7;
using aistore::metadata::VersionState;
using aistore::pull::PullEngine;
using aistore::pull::PullRequest;
using aistore::pull::PullResult;
using aistore::service::MetadataService;
using aistore::service::StorageNodeService;
using aistore::storage::LocalChunkStore;

namespace beast_http = aistore::http::beast_http;

constexpr std::uint64_t kMaxBodyBytes = 8ULL * 1024ULL * 1024ULL;
constexpr std::string_view kSourceNode = "m5-pull-source";

constexpr FastCdcParameters kPullFastCdcParams{
    .min_chunk_size_bytes = 64U,
    .avg_chunk_size_bytes = 128U,
    .max_chunk_size_bytes = 256U,
};

std::vector<std::byte> make_fastcdc_golden_fixture() {
    std::vector<std::byte> fixture;
    fixture.reserve(4096U);

    for (std::size_t index = 0; index < 4096U; ++index) {
        fixture.push_back(static_cast<std::byte>(index % 251U));
    }

    return fixture;
}

std::string test_database_connection_string() {
    const char* configured = std::getenv("AISTORE_TEST_DB_URL");

    if (configured != nullptr && configured[0] != '\0') {
        return configured;
    }

    return "dbname=ai_artifact_store_test";
}

class TemporaryDirectory {
   public:
    TemporaryDirectory() {
        static std::atomic<std::uint64_t> counter{0};
        const auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();
        path_ = std::filesystem::temp_directory_path() /
                ("aistore-pull-" + std::to_string(timestamp) + "-" + std::to_string(counter.fetch_add(1)));
        std::filesystem::create_directories(path_);
    }

    ~TemporaryDirectory() {
        std::error_code error;
        std::filesystem::remove_all(path_, error);
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

   private:
    std::filesystem::path path_;
};

class RunningHttpServer {
   public:
    explicit RunningHttpServer(aistore::http::RequestHandler handler)
        : server_(
              HttpServerConfig{
                  .bind_address = "127.0.0.1", .port = 0, .worker_threads = 4, .max_request_body_bytes = kMaxBodyBytes},
              std::move(handler)),
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

std::string digest_to_hex(const aistore::hashing::Sha256::Digest& digest) {
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

std::string sha256_hex(std::span<const std::byte> bytes) {
    aistore::hashing::Sha256 hasher;
    hasher.update(bytes);
    return digest_to_hex(hasher.finalize());
}

std::string sha256_hex(std::string_view text) { return sha256_hex(std::as_bytes(std::span{text.data(), text.size()})); }

std::string read_file(const std::filesystem::path& path) {
    std::ifstream input{path, std::ios::binary};
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void write_bytes_at(const std::filesystem::path& path, std::uint64_t offset, std::string_view contents) {
    std::fstream output{path, std::ios::binary | std::ios::in | std::ios::out};
    if (!output) {
        std::ofstream create{path, std::ios::binary};
        create.close();
        output.open(path, std::ios::binary | std::ios::in | std::ios::out);
    }
    output.seekp(static_cast<std::streamoff>(offset));
    output.write(contents.data(), static_cast<std::streamsize>(contents.size()));
    output.close();
}

[[nodiscard]] ObjectLayoutDescriptor make_fixed_size_descriptor(std::string_view content, std::uint64_t chunk_size) {
    std::vector<ChunkRef> refs;
    std::uint64_t offset = 0;

    while (offset < content.size()) {
        const std::uint64_t size = std::min(chunk_size, content.size() - offset);
        const std::string chunk_bytes = std::string{content.substr(offset, size)};
        refs.push_back(ChunkRef{.chunk_id = sha256_hex(chunk_bytes), .offset = offset, .size = size});
        offset += size;
    }

    return ObjectLayoutDescriptor{Object{sha256_hex(content), content.size()}, ChunkingStrategy::FixedSize,
                                  ObjectLayout{std::move(refs)}};
}

[[nodiscard]] ObjectLayoutDescriptor make_fastcdc_descriptor(std::span<const std::byte> bytes,
                                                             const FastCdcParameters& parameters) {
    FastCdcChunker chunker{static_cast<std::size_t>(parameters.min_chunk_size_bytes),
                           static_cast<std::size_t>(parameters.avg_chunk_size_bytes),
                           static_cast<std::size_t>(parameters.max_chunk_size_bytes)};

    std::vector<ChunkRef> refs;
    const auto collect = [&](ChunkBuffer chunk) {
        refs.push_back(ChunkRef{
            .chunk_id = sha256_hex(chunk.bytes),
            .offset = chunk.offset,
            .size = chunk.bytes.size(),
        });
    };

    chunker.update(bytes, collect);
    chunker.finalize(collect);

    return ObjectLayoutDescriptor{Object{sha256_hex(bytes), bytes.size()}, parameters, ObjectLayout{std::move(refs)}};
}

[[nodiscard]] std::filesystem::path partial_path_for(const std::filesystem::path& destination,
                                                     std::string_view version_id) {
    return std::filesystem::path{destination.string() + ".aistore." + std::string{version_id} + ".part"};
}

class PullEngineFixture : public ::testing::Test {
   protected:
    void SetUp() override {
        artifact_id_ = UuidV7::generate();
        unique_tag_ = UuidV7::generate().str();

        repository_.emplace(test_database_connection_string());
        repository_->create_artifact(
            Artifact{artifact_id_, std::string{"m5-pull-artifact-"} + artifact_id_.str(), "m5-pull-project"});

        metadata_service_.emplace(*repository_);
        metadata_server_.emplace(
            [&](const HttpRequest& request) { return metadata_service_->handle_request(request); });
        metadata_client_.emplace(HttpClientConfig{
            .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = metadata_server_->port()},
        });

        storage_root_.emplace();
        chunk_store_.emplace(storage_root_->path().string());
        storage_service_.emplace(*chunk_store_);
        storage_server_.emplace([&](const HttpRequest& request) { return storage_service_->handle_request(request); });
        storage_client_.emplace(HttpClientConfig{
            .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = storage_server_->port()},
        });

        dest_dir_.emplace();
    }

    void TearDown() override {
        pull_engine_.reset();
        storage_client_.reset();
        storage_server_.reset();
        storage_service_.reset();
        chunk_store_.reset();
        storage_root_.reset();
        metadata_client_.reset();
        metadata_server_.reset();
        metadata_service_.reset();

        if (repository_.has_value()) {
            cleanup_owned_rows();
            repository_.reset();
        }

        dest_dir_.reset();
    }

    void cleanup_owned_rows() {
        pqxx::connection connection{test_database_connection_string()};
        pqxx::work transaction{connection};

        for (const std::string& version_id : owned_version_ids_) {
            transaction.exec("DELETE FROM artifact_version_metadata WHERE version_id = $1", pqxx::params{version_id})
                .no_rows();
            transaction.exec("DELETE FROM artifact_versions WHERE version_id = $1", pqxx::params{version_id}).no_rows();
        }

        for (const std::string& object_id : owned_object_ids_) {
            // Clear any leftover versions that still reference owned objects (shared content IDs).
            transaction
                .exec(
                    "DELETE FROM artifact_version_metadata WHERE version_id IN "
                    "(SELECT version_id FROM artifact_versions WHERE root_object_id = $1)",
                    pqxx::params{object_id})
                .no_rows();
            transaction.exec("DELETE FROM artifact_versions WHERE root_object_id = $1", pqxx::params{object_id})
                .no_rows();
            transaction
                .exec(
                    "DELETE FROM object_layout_chunks WHERE layout_id IN "
                    "(SELECT layout_id FROM object_layouts WHERE object_id = $1)",
                    pqxx::params{object_id})
                .no_rows();
            transaction.exec("DELETE FROM object_layouts WHERE object_id = $1", pqxx::params{object_id}).no_rows();
            transaction.exec("DELETE FROM objects WHERE object_id = $1", pqxx::params{object_id}).no_rows();
        }

        transaction.exec("DELETE FROM storage_locations WHERE node_id LIKE 'm5-pull-%'").no_rows();

        for (const std::string& chunk_id : owned_chunk_ids_) {
            transaction.exec("DELETE FROM storage_locations WHERE chunk_id = $1", pqxx::params{chunk_id}).no_rows();
            transaction.exec("DELETE FROM chunks WHERE chunk_id = $1", pqxx::params{chunk_id}).no_rows();
        }

        transaction.exec("DELETE FROM artifacts WHERE artifact_id = $1::uuid", pqxx::params{artifact_id_.str()})
            .no_rows();
        transaction.commit();
    }

    void track_chunk(std::string chunk_id) { owned_chunk_ids_.push_back(std::move(chunk_id)); }

    void track_object(std::string object_id) { owned_object_ids_.push_back(std::move(object_id)); }

    void track_version(std::string version_id) { owned_version_ids_.push_back(std::move(version_id)); }

    void store_chunk(std::string_view bytes) {
        const std::string chunk_id = sha256_hex(bytes);
        track_chunk(chunk_id);
        repository_->register_chunks({ChunkMetadata{.chunk_id = chunk_id, .size_bytes = bytes.size()}});
        chunk_store_->put(chunk_id, std::as_bytes(std::span{bytes.data(), bytes.size()}));
        repository_->register_storage_location(StorageLocation{
            .chunk_id = chunk_id,
            .node_id = std::string{kSourceNode},
            .storage_path = std::string{"/v1/chunks/"} + chunk_id,
            .state = StorageLocationState::Available,
        });
    }

    void register_descriptor_chunks(const ObjectLayoutDescriptor& descriptor) {
        track_object(descriptor.object_id());
        for (const ChunkRef& chunk : descriptor.layout().chunks()) {
            track_chunk(chunk.chunk_id);
            repository_->register_chunks({ChunkMetadata{.chunk_id = chunk.chunk_id, .size_bytes = chunk.size}});
            repository_->register_storage_location(StorageLocation{
                .chunk_id = chunk.chunk_id,
                .node_id = std::string{kSourceNode},
                .storage_path = std::string{"/v1/chunks/"} + chunk.chunk_id,
                .state = StorageLocationState::Available,
            });
        }
    }

    [[nodiscard]] ArtifactVersion create_committed_version(const ObjectLayoutDescriptor& descriptor) {
        repository_->register_object(descriptor.object());
        repository_->register_object_layout(descriptor);
        register_descriptor_chunks(descriptor);

        ArtifactVersion version{artifact_id_, descriptor.object_id(), std::nullopt,
                                ArtifactVersion::ImmutableMetadata{{"source", "m5-pull"}}, VersionState::Committed};
        repository_->create_version(version);
        track_version(version.version_id());
        return version;
    }

    PullEngine& engine() {
        if (!pull_engine_.has_value()) {
            pull_engine_.emplace(*metadata_client_, *storage_client_, std::string{kSourceNode});
        }
        return *pull_engine_;
    }

    [[nodiscard]] std::filesystem::path destination_path(std::string_view name) const {
        return dest_dir_->path() / std::string{name};
    }

    UuidV7 artifact_id_{UuidV7::generate()};
    std::string unique_tag_;
    std::vector<std::string> owned_chunk_ids_;
    std::vector<std::string> owned_object_ids_;
    std::vector<std::string> owned_version_ids_;

    std::optional<PostgresMetadataRepository> repository_;
    std::optional<MetadataService> metadata_service_;
    std::optional<RunningHttpServer> metadata_server_;
    std::optional<MetadataClient> metadata_client_;

    std::optional<TemporaryDirectory> storage_root_;
    std::optional<LocalChunkStore> chunk_store_;
    std::optional<StorageNodeService> storage_service_;
    std::optional<RunningHttpServer> storage_server_;
    std::optional<StorageNodeClient> storage_client_;

    std::optional<TemporaryDirectory> dest_dir_;
    std::optional<PullEngine> pull_engine_;
};

TEST_F(PullEngineFixture, PullsMultiChunkObjectAndPublishesAtomically) {
    const std::string contents = std::string{"abcdefghij"} + unique_tag_.substr(0, 2);
    const ObjectLayoutDescriptor descriptor = make_fixed_size_descriptor(contents, 4);

    for (const ChunkRef& chunk : descriptor.layout().chunks()) {
        const std::string chunk_bytes =
            contents.substr(static_cast<std::size_t>(chunk.offset), static_cast<std::size_t>(chunk.size));
        store_chunk(chunk_bytes);
    }

    const ArtifactVersion version = create_committed_version(descriptor);
    const auto destination = destination_path("multi.bin");

    const PullResult result = engine().pull(
        PullRequest{.version_id = version.version_id(), .destination_path = destination, .overwrite = false});

    EXPECT_EQ(result.version_id, version.version_id());
    EXPECT_EQ(result.artifact_id, artifact_id_);
    EXPECT_EQ(result.source_node_id, kSourceNode);
    EXPECT_EQ(result.object_id, descriptor.object_id());
    EXPECT_EQ(result.layout_id, descriptor.layout_id());
    EXPECT_EQ(result.destination_path, destination);
    EXPECT_EQ(result.stats.bytes_restored, contents.size());
    EXPECT_EQ(result.stats.total_chunks, descriptor.layout().chunks().size());
    EXPECT_EQ(result.stats.chunks_downloaded, descriptor.layout().chunks().size());
    EXPECT_EQ(result.stats.chunks_reused_from_partial, 0U);
    EXPECT_EQ(result.stats.bytes_received_from_storage, contents.size());
    EXPECT_EQ(read_file(destination), contents);
    EXPECT_FALSE(std::filesystem::exists(partial_path_for(destination, version.version_id())));
}

TEST_F(PullEngineFixture, PullsEmptyObjectWithoutStorageRequests) {
    const ObjectLayoutDescriptor descriptor{
        Object{sha256_hex(""), 0},
        ChunkingStrategy::FixedSize,
        ObjectLayout{std::vector<ChunkRef>{}},
    };

    const ArtifactVersion version = create_committed_version(descriptor);
    const auto destination = destination_path("empty.bin");

    std::atomic<std::uint64_t> get_count{0};
    RunningHttpServer counting_storage{[&](const HttpRequest& request) {
        if (request.method() == beast_http::verb::get && request.target().starts_with("/v1/chunks/")) {
            get_count.fetch_add(1U, std::memory_order_relaxed);
        }
        return storage_service_->handle_request(request);
    }};

    StorageNodeClient counting_client{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = counting_storage.port()},
    }};
    PullEngine counting_engine{*metadata_client_, counting_client, std::string{kSourceNode}};

    const PullResult result = counting_engine.pull(
        PullRequest{.version_id = version.version_id(), .destination_path = destination, .overwrite = false});

    EXPECT_EQ(result.stats.total_chunks, 0U);
    EXPECT_EQ(result.stats.chunks_downloaded, 0U);
    EXPECT_EQ(result.stats.bytes_received_from_storage, 0U);
    EXPECT_EQ(get_count.load(), 0U);
    EXPECT_TRUE(std::filesystem::exists(destination));
    EXPECT_EQ(read_file(destination), "");
}

TEST_F(PullEngineFixture, ReusesVerifiedPartialPrefix) {
    const std::string contents = "ABCDABCDEFGH";
    const ObjectLayoutDescriptor descriptor = make_fixed_size_descriptor(contents, 4);

    store_chunk("ABCD");
    store_chunk("EFGH");

    const ArtifactVersion version = create_committed_version(descriptor);
    const auto destination = destination_path("reuse.bin");
    const auto partial = partial_path_for(destination, version.version_id());

    write_bytes_at(partial, 0, "ABCD");
    write_bytes_at(partial, 4, "ABCD");

    const PullResult result = engine().pull(
        PullRequest{.version_id = version.version_id(), .destination_path = destination, .overwrite = false});

    EXPECT_EQ(read_file(destination), contents);
    EXPECT_EQ(result.stats.chunks_reused_from_partial, 2U);
    EXPECT_EQ(result.stats.chunks_downloaded, 1U);
    EXPECT_EQ(result.stats.bytes_received_from_storage, 4U);
}

TEST_F(PullEngineFixture, TruncatesPartialChunkBeforeResume) {
    const std::string contents = "ABCDABCDEFGH";
    const ObjectLayoutDescriptor descriptor = make_fixed_size_descriptor(contents, 4);

    store_chunk("ABCD");
    store_chunk("EFGH");

    const ArtifactVersion version = create_committed_version(descriptor);
    const auto destination = destination_path("truncate-partial.bin");
    const auto partial = partial_path_for(destination, version.version_id());

    write_bytes_at(partial, 0, "ABCD");
    write_bytes_at(partial, 4, "AB");

    const PullResult result = engine().pull(
        PullRequest{.version_id = version.version_id(), .destination_path = destination, .overwrite = false});

    EXPECT_EQ(read_file(destination), contents);
    EXPECT_EQ(result.stats.chunks_reused_from_partial, 1U);
    EXPECT_EQ(result.stats.chunks_downloaded, 2U);
}

TEST_F(PullEngineFixture, TruncatesCorruptCompletedChunkBeforeResume) {
    const std::string contents = "ABCDABCDEFGH";
    const ObjectLayoutDescriptor descriptor = make_fixed_size_descriptor(contents, 4);

    store_chunk("ABCD");
    store_chunk("EFGH");

    const ArtifactVersion version = create_committed_version(descriptor);
    const auto destination = destination_path("truncate-corrupt.bin");
    const auto partial = partial_path_for(destination, version.version_id());

    write_bytes_at(partial, 0, "ABCD");
    write_bytes_at(partial, 4, "WXYZ");

    const PullResult result = engine().pull(
        PullRequest{.version_id = version.version_id(), .destination_path = destination, .overwrite = false});

    EXPECT_EQ(read_file(destination), contents);
    EXPECT_EQ(result.stats.chunks_reused_from_partial, 1U);
    EXPECT_EQ(result.stats.chunks_downloaded, 2U);
}

TEST_F(PullEngineFixture, RejectsDownloadedChunkSizeMismatchWithoutPublishing) {
    const std::string chunk_bytes = "ABCD";
    const std::string chunk_id = sha256_hex(chunk_bytes);
    const std::string object_id = sha256_hex(std::string{"size-mismatch-"} + unique_tag_);

    // Metadata claims size 8 while CAS stores the authentic 4-byte body for this chunk_id.
    track_chunk(chunk_id);
    repository_->register_chunks({ChunkMetadata{.chunk_id = chunk_id, .size_bytes = 8}});
    chunk_store_->put(chunk_id, std::as_bytes(std::span{chunk_bytes.data(), chunk_bytes.size()}));
    repository_->register_storage_location(StorageLocation{
        .chunk_id = chunk_id,
        .node_id = std::string{kSourceNode},
        .storage_path = std::string{"/v1/chunks/"} + chunk_id,
        .state = StorageLocationState::Available,
    });

    const ObjectLayoutDescriptor descriptor{
        Object{object_id, 8},
        ChunkingStrategy::FixedSize,
        ObjectLayout{{ChunkRef{.chunk_id = chunk_id, .offset = 0, .size = 8}}},
    };

    const ArtifactVersion version = create_committed_version(descriptor);
    const auto destination = destination_path("size-mismatch.bin");

    EXPECT_THROW((void)engine().pull(PullRequest{
                     .version_id = version.version_id(), .destination_path = destination, .overwrite = false}),
                 std::runtime_error);

    EXPECT_FALSE(std::filesystem::exists(destination));
    EXPECT_TRUE(std::filesystem::exists(partial_path_for(destination, version.version_id())));
}

TEST_F(PullEngineFixture, RejectsWholeObjectHashMismatchWithoutPublishing) {
    const std::string contents = "ABCDABCDEFGH";
    const ObjectLayoutDescriptor layout = make_fixed_size_descriptor(contents, 4);
    const std::string fake_object_id = sha256_hex("not-the-content");

    const ObjectLayoutDescriptor descriptor{
        Object{fake_object_id, contents.size()},
        ChunkingStrategy::FixedSize,
        layout.layout(),
    };

    store_chunk("ABCD");
    store_chunk("EFGH");

    const ArtifactVersion version = create_committed_version(descriptor);
    const auto destination = destination_path("hash-mismatch.bin");

    EXPECT_THROW((void)engine().pull(PullRequest{
                     .version_id = version.version_id(), .destination_path = destination, .overwrite = false}),
                 std::runtime_error);

    EXPECT_FALSE(std::filesystem::exists(destination));
    EXPECT_TRUE(std::filesystem::exists(partial_path_for(destination, version.version_id())));
}

TEST_F(PullEngineFixture, MissingStorageChunkFailsAndKeepsResumablePartial) {
    const std::string contents = "AAAABBBBCCCC";
    const ObjectLayoutDescriptor descriptor = make_fixed_size_descriptor(contents, 4);

    store_chunk("AAAA");
    // BBBB/CCCC remain registered as Available in metadata but absent from CAS.

    const ArtifactVersion version = create_committed_version(descriptor);
    const auto destination = destination_path("missing-chunk.bin");
    const auto partial = partial_path_for(destination, version.version_id());

    // Seed a verified prefix so failure cannot race ahead of the first ordered write.
    write_bytes_at(partial, 0, "AAAA");

    EXPECT_THROW((void)engine().pull(PullRequest{
                     .version_id = version.version_id(), .destination_path = destination, .overwrite = false}),
                 std::runtime_error);

    ASSERT_TRUE(std::filesystem::exists(partial));
    EXPECT_EQ(read_file(partial), "AAAA");
    EXPECT_FALSE(std::filesystem::exists(destination));

    store_chunk("BBBB");
    store_chunk("CCCC");

    const PullResult result = engine().pull(
        PullRequest{.version_id = version.version_id(), .destination_path = destination, .overwrite = false});

    EXPECT_EQ(read_file(destination), contents);
    EXPECT_EQ(result.stats.chunks_reused_from_partial, 1U);
    EXPECT_EQ(result.stats.chunks_downloaded, 2U);
}

TEST_F(PullEngineFixture, ExistingDestinationFailsWithoutOverwrite) {
    const std::string contents = std::string{"EX"} + unique_tag_.substr(0, 2);
    const ObjectLayoutDescriptor descriptor = make_fixed_size_descriptor(contents, 4);
    store_chunk(contents);

    const ArtifactVersion version = create_committed_version(descriptor);
    const auto destination = destination_path("exists.bin");
    std::ofstream{destination, std::ios::binary} << "already here";

    EXPECT_THROW((void)engine().pull(PullRequest{
                     .version_id = version.version_id(), .destination_path = destination, .overwrite = false}),
                 std::runtime_error);

    EXPECT_EQ(read_file(destination), "already here");
}

TEST_F(PullEngineFixture, OverwriteAtomicallyReplacesDestination) {
    const std::string contents = std::string{"OW"} + unique_tag_.substr(0, 2);
    const ObjectLayoutDescriptor descriptor = make_fixed_size_descriptor(contents, 4);
    store_chunk(contents);

    const ArtifactVersion version = create_committed_version(descriptor);
    const auto destination = destination_path("overwrite.bin");
    std::ofstream{destination, std::ios::binary} << "stale bytes";

    const PullResult result = engine().pull(
        PullRequest{.version_id = version.version_id(), .destination_path = destination, .overwrite = true});

    EXPECT_EQ(read_file(destination), contents);
    EXPECT_EQ(result.stats.bytes_restored, contents.size());
    EXPECT_FALSE(std::filesystem::exists(partial_path_for(destination, version.version_id())));
}

TEST_F(PullEngineFixture, WorkerFailureCancelsAndJoinsWithoutPublishing) {
    const std::string contents = "ABCDABCDEFGH";
    const ObjectLayoutDescriptor descriptor = make_fixed_size_descriptor(contents, 4);

    store_chunk("ABCD");
    store_chunk("EFGH");

    const ArtifactVersion version = create_committed_version(descriptor);
    const std::string failing_chunk_id = descriptor.layout().chunks().back().chunk_id;
    const auto destination = destination_path("worker-fail.bin");

    RunningHttpServer failing_storage{[this, failing_chunk_id](const HttpRequest& request) {
        if (request.method() == beast_http::verb::get &&
            request.target() == std::string{"/v1/chunks/"} + failing_chunk_id) {
            HttpResponse response{beast_http::status::internal_server_error, request.version()};
            response.set(beast_http::field::content_type, "application/json");
            response.body() = R"({"error":"injected_storage_failure"})";
            response.prepare_payload();
            return response;
        }

        return storage_service_->handle_request(request);
    }};

    StorageNodeClient failing_client{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = failing_storage.port()},
    }};
    PullEngine failing_engine{*metadata_client_, failing_client, std::string{kSourceNode}};

    try {
        (void)failing_engine.pull(
            PullRequest{.version_id = version.version_id(), .destination_path = destination, .overwrite = false});
        FAIL() << "expected RemoteApiError";
    } catch (const RemoteApiError& error) {
        EXPECT_EQ(error.status_code(), 500U);
        EXPECT_EQ(error.error_code(), "injected_storage_failure");
    }

    EXPECT_FALSE(std::filesystem::exists(destination));
    const auto partial = partial_path_for(destination, version.version_id());
    ASSERT_TRUE(std::filesystem::exists(partial));
    // A racing failure on a later chunk may cancel before the first ordered write lands.
    // The deterministic partial path must still remain for resume.
}

TEST_F(PullEngineFixture, DownloadConcurrencyNeverExceedsFourAndWindowRemainsBounded) {
    std::string contents;
    contents.reserve(80);
    for (int index = 0; index < 20; ++index) {
        contents.push_back(static_cast<char>('a' + (index % 26)));
        contents.push_back(static_cast<char>('A' + (index % 26)));
        contents.push_back(static_cast<char>('0' + (index % 10)));
        contents.push_back(static_cast<char>('z' - (index % 26)));
    }

    const ObjectLayoutDescriptor descriptor = make_fixed_size_descriptor(contents, 4);
    std::vector<std::string> chunk_ids;
    chunk_ids.reserve(descriptor.layout().chunks().size());

    for (const ChunkRef& chunk : descriptor.layout().chunks()) {
        const std::string chunk_bytes =
            contents.substr(static_cast<std::size_t>(chunk.offset), static_cast<std::size_t>(chunk.size));
        store_chunk(chunk_bytes);
        chunk_ids.push_back(chunk.chunk_id);
    }

    const ArtifactVersion version = create_committed_version(descriptor);
    const auto destination = destination_path("concurrency.bin");

    std::atomic<int> in_flight{0};
    std::atomic<int> max_in_flight{0};
    std::atomic<int> starts_before_first_done{0};
    std::atomic<int> peak_starts_before_first_done{0};
    std::atomic<bool> first_done{false};
    const std::string first_chunk_id = chunk_ids.front();

    RunningHttpServer instrumented_storage{[&, first_chunk_id](const HttpRequest& request) {
        if (request.method() != beast_http::verb::get || !request.target().starts_with("/v1/chunks/")) {
            return storage_service_->handle_request(request);
        }

        const int current = in_flight.fetch_add(1, std::memory_order_relaxed) + 1;
        int observed_max = max_in_flight.load(std::memory_order_relaxed);
        while (current > observed_max &&
               !max_in_flight.compare_exchange_weak(observed_max, current, std::memory_order_relaxed)) {
        }

        if (!first_done.load(std::memory_order_relaxed)) {
            const int started = starts_before_first_done.fetch_add(1, std::memory_order_relaxed) + 1;
            int peak = peak_starts_before_first_done.load(std::memory_order_relaxed);
            while (started > peak &&
                   !peak_starts_before_first_done.compare_exchange_weak(peak, started, std::memory_order_relaxed)) {
            }
        }

        const bool is_first = request.target() == std::string{"/v1/chunks/"} + first_chunk_id;
        if (is_first) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            first_done.store(true, std::memory_order_relaxed);
        }

        HttpResponse response = storage_service_->handle_request(request);

        in_flight.fetch_sub(1, std::memory_order_relaxed);
        return response;
    }};

    StorageNodeClient instrumented_client{HttpClientConfig{
        .endpoint = HttpEndpoint{.address = "127.0.0.1", .port = instrumented_storage.port()},
    }};
    PullEngine instrumented_engine{*metadata_client_, instrumented_client, std::string{kSourceNode}};

    const PullResult result = instrumented_engine.pull(
        PullRequest{.version_id = version.version_id(), .destination_path = destination, .overwrite = false});

    EXPECT_EQ(read_file(destination), contents);
    EXPECT_EQ(result.stats.total_chunks, 20U);
    EXPECT_LE(max_in_flight.load(), static_cast<int>(PullEngine::kWorkerCount));
    EXPECT_LE(peak_starts_before_first_done.load(), static_cast<int>(PullEngine::kWindowCapacity));
}

TEST_F(PullEngineFixture, PullsFastCdcVariableSizedLayoutWithoutChunkingSpecificLogic) {
    const std::vector<std::byte> fixture = make_fastcdc_golden_fixture();
    const ObjectLayoutDescriptor descriptor = make_fastcdc_descriptor(fixture, kPullFastCdcParams);

    ASSERT_EQ(descriptor.chunking_strategy(), ChunkingStrategy::FastCdc);
    ASSERT_TRUE(descriptor.fastcdc_parameters().has_value());
    EXPECT_EQ(*descriptor.fastcdc_parameters(), kPullFastCdcParams);
    ASSERT_GT(descriptor.layout().chunks().size(), 1U);

    for (const ChunkRef& chunk : descriptor.layout().chunks()) {
        const auto offset = static_cast<std::size_t>(chunk.offset);
        const std::string chunk_bytes{
            reinterpret_cast<const char*>(fixture.data() + offset),
            chunk.size,
        };
        store_chunk(chunk_bytes);
    }

    const ArtifactVersion version = create_committed_version(descriptor);
    const auto destination = destination_path("fastcdc-restored.bin");

    const PullResult result = engine().pull(
        PullRequest{.version_id = version.version_id(), .destination_path = destination, .overwrite = false});

    const std::string restored = read_file(destination);
    const std::string expected{reinterpret_cast<const char*>(fixture.data()), fixture.size()};
    EXPECT_EQ(restored, expected);
    EXPECT_EQ(result.object_id, descriptor.object_id());
    EXPECT_EQ(result.stats.bytes_restored, fixture.size());
    EXPECT_EQ(result.stats.total_chunks, descriptor.layout().chunks().size());
}

}  // namespace
