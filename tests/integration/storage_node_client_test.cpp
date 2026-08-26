#include "aistore/client/storage_node_client.hpp"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "aistore/client/client_error.hpp"
#include "aistore/hashing/sha256.hpp"
#include "aistore/http/http_client.hpp"
#include "aistore/http/http_server.hpp"
#include "aistore/service/storage_node_service.hpp"
#include "aistore/storage/local_chunk_store.hpp"

namespace {

using aistore::client::RemoteApiError;
using aistore::client::RemoteProtocolError;
using aistore::client::StorageNodeClient;
using aistore::http::HttpClientConfig;
using aistore::http::HttpEndpoint;
using aistore::http::HttpRequest;
using aistore::http::HttpResponse;
using aistore::http::HttpServer;
using aistore::http::HttpServerConfig;
using aistore::http::RequestHandler;
using aistore::service::StorageNodeService;
using aistore::storage::LocalChunkStore;

namespace beast_http = aistore::http::beast_http;

constexpr std::uint64_t kMaxBodyBytes = 8ULL * 1024ULL * 1024ULL;

constexpr std::string_view kAbcChunkId =
    "ba7816bf8f01cfea414140de5dae2223"
    "b00361a396177a9cb410ff61f20015ad";

constexpr std::string_view kAbcBody = "abc";

class TemporaryDirectory {
   public:
    TemporaryDirectory() {
        static std::atomic<std::uint64_t> counter{0};

        const auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();

        path_ = std::filesystem::temp_directory_path() /
                ("aistore-storage-client-" + std::to_string(timestamp) + "-" + std::to_string(counter.fetch_add(1)));

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
    explicit RunningHttpServer(StorageNodeService& service)
        : RunningHttpServer([&service](const HttpRequest& request) { return service.handle_request(request); }) {}

    explicit RunningHttpServer(RequestHandler handler)
        : server_(
              HttpServerConfig{
                  .bind_address = "127.0.0.1",
                  .port = 0,
                  .worker_threads = 2,
                  .max_request_body_bytes = kMaxBodyBytes,
              },
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

class StorageNodeClientFixture {
   public:
    StorageNodeClientFixture()
        : temporary_directory_{},
          chunk_store_{temporary_directory_.path().string()},
          service_{chunk_store_},
          server_{service_},
          client_{HttpClientConfig{
              .endpoint =
                  HttpEndpoint{
                      .address = "127.0.0.1",
                      .port = server_.port(),
                  },
          }} {}

    [[nodiscard]] StorageNodeClient& client() noexcept { return client_; }

    [[nodiscard]] LocalChunkStore& store() noexcept { return chunk_store_; }

   private:
    TemporaryDirectory temporary_directory_;
    LocalChunkStore chunk_store_;
    StorageNodeService service_;
    RunningHttpServer server_;
    StorageNodeClient client_;
};

TEST(StorageNodeClientTest, HasChunkReportsMissingAndExisting) {
    StorageNodeClientFixture fixture;

    EXPECT_FALSE(fixture.client().has_chunk(kAbcChunkId));

    const auto bytes = std::as_bytes(std::span{kAbcBody.data(), kAbcBody.size()});
    fixture.client().put_chunk(kAbcChunkId, bytes);

    EXPECT_TRUE(fixture.client().has_chunk(kAbcChunkId));
}

TEST(StorageNodeClientTest, PutChunkStoresVerifiedBytes) {
    StorageNodeClientFixture fixture;

    const auto bytes = std::as_bytes(std::span{kAbcBody.data(), kAbcBody.size()});
    fixture.client().put_chunk(kAbcChunkId, bytes);

    ASSERT_TRUE(fixture.store().contains(kAbcChunkId));
    const std::vector<std::byte> stored = fixture.store().get(kAbcChunkId);
    ASSERT_EQ(stored.size(), kAbcBody.size());

    for (std::size_t index = 0; index < stored.size(); ++index) {
        EXPECT_EQ(stored[index], static_cast<std::byte>(static_cast<unsigned char>(kAbcBody[index])));
    }
}

TEST(StorageNodeClientTest, GetChunkReturnsBinaryBytesExactly) {
    StorageNodeClientFixture fixture;

    std::vector<std::byte> payload{
        std::byte{'A'}, std::byte{0}, std::byte{'B'}, std::byte{0}, std::byte{'C'},
    };
    const std::string chunk_id = sha256_hex(payload);

    fixture.client().put_chunk(chunk_id, payload);
    const std::optional<std::vector<std::byte>> loaded = fixture.client().get_chunk(chunk_id);

    ASSERT_TRUE(loaded.has_value());
    ASSERT_EQ(loaded->size(), payload.size());
    EXPECT_EQ(*loaded, payload);
}

TEST(StorageNodeClientTest, GetMissingChunkReturnsNullopt) {
    StorageNodeClientFixture fixture;

    const std::string missing = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";
    EXPECT_FALSE(fixture.client().get_chunk(missing).has_value());
}

TEST(StorageNodeClientTest, HashMismatchProducesRemoteApiError) {
    StorageNodeClientFixture fixture;

    const auto bytes = std::as_bytes(std::span{kAbcBody.data(), kAbcBody.size()});
    const std::string wrong_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    try {
        fixture.client().put_chunk(wrong_id, bytes);
        FAIL() << "expected RemoteApiError";
    } catch (const RemoteApiError& error) {
        EXPECT_EQ(error.status_code(), 422U);
        EXPECT_EQ(error.error_code(), "chunk_hash_mismatch");
    }
}

TEST(StorageNodeClientTest, GetChunkRejectsSuccessfulBodyWhoseHashDoesNotMatchChunkId) {
    RunningHttpServer server{[](const HttpRequest& request) {
        HttpResponse response{beast_http::status::ok, request.version()};
        response.set(beast_http::field::content_type, "application/octet-stream");
        response.body() = "wrong-bytes-not-matching-requested-chunk-id";
        response.prepare_payload();
        return response;
    }};

    StorageNodeClient client{HttpClientConfig{
        .endpoint =
            HttpEndpoint{
                .address = "127.0.0.1",
                .port = server.port(),
            },
    }};

    EXPECT_THROW((void)client.get_chunk(kAbcChunkId), RemoteProtocolError);
}

TEST(StorageNodeClientTest, InvalidChunkIdIsRejectedBeforeNetwork) {
    const StorageNodeClient client{HttpClientConfig{
        .endpoint =
            HttpEndpoint{
                .address = "127.0.0.1",
                .port = 1,
            },
    }};

    EXPECT_THROW((void)client.has_chunk("not-a-chunk-id"), std::invalid_argument);
}

}  // namespace
