#include "aistore/push/push_engine.hpp"

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <fstream>
#include <functional>
#include <limits>
#include <mutex>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "aistore/chunking/fixed_size_chunker.hpp"
#include "aistore/client/client_error.hpp"
#include "aistore/hashing/sha256.hpp"
#include "aistore/metadata/chunk_metadata.hpp"
#include "aistore/metadata/object.hpp"
#include "aistore/metadata/object_layout.hpp"
#include "aistore/metadata/storage_location.hpp"
#include "aistore/metadata/upload_session.hpp"

namespace aistore::push {

namespace {

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

[[nodiscard]] std::string logical_storage_path(std::string_view chunk_id) {
    return std::string{"/v1/chunks/"} + std::string{chunk_id};
}

struct UploadTask {
    aistore::metadata::ChunkMetadata chunk;
    std::vector<std::byte> bytes;
    aistore::client::ChunkAvailability availability;
};

struct StagedChunk {
    aistore::metadata::ChunkMetadata metadata;
    std::vector<std::byte> bytes;
};

template <typename T>
class BoundedQueue {
   public:
    explicit BoundedQueue(std::size_t capacity) : capacity_{capacity} {
        if (capacity_ == 0U) {
            throw std::invalid_argument("bounded queue capacity must be greater than zero");
        }
    }

    [[nodiscard]] bool push(T item) {
        std::unique_lock lock{mutex_};
        not_full_.wait(lock, [&] { return cancelled_ || closed_ || items_.size() < capacity_; });

        if (cancelled_ || closed_) {
            return false;
        }

        items_.push_back(std::move(item));
        not_empty_.notify_one();
        return true;
    }

    [[nodiscard]] std::optional<T> pop() {
        std::unique_lock lock{mutex_};
        not_empty_.wait(lock, [&] { return cancelled_ || closed_ || !items_.empty(); });

        if (items_.empty()) {
            return std::nullopt;
        }

        T item = std::move(items_.front());
        items_.pop_front();
        not_full_.notify_one();
        return item;
    }

    void close() {
        const std::scoped_lock lock{mutex_};
        closed_ = true;
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    void cancel() {
        const std::scoped_lock lock{mutex_};
        cancelled_ = true;
        closed_ = true;
        items_.clear();
        not_empty_.notify_all();
        not_full_.notify_all();
    }

    [[nodiscard]] bool cancelled() const {
        const std::scoped_lock lock{mutex_};
        return cancelled_;
    }

   private:
    std::size_t capacity_;
    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
    std::deque<T> items_;
    bool closed_{false};
    bool cancelled_{false};
};

struct SharedWorkerState {
    BoundedQueue<UploadTask> queue{PushEngine::kQueueCapacity};
    std::mutex failure_mutex;
    std::exception_ptr first_failure;
    std::atomic<std::size_t> put_requests{0};
    std::atomic<std::size_t> verified_target_chunks{0};
    std::atomic<std::size_t> repaired_target_chunks{0};
    std::atomic<std::uint64_t> bytes_sent_to_storage{0};
};

void record_failure(SharedWorkerState& state, const std::exception_ptr& exception) {
    const std::scoped_lock lock{state.failure_mutex};

    if (state.first_failure == nullptr) {
        state.first_failure = exception;
    }
}

void process_upload_task(const UploadTask& task, aistore::client::MetadataClient& metadata_client,
                         aistore::client::StorageNodeClient& storage_client, std::string_view storage_node_id,
                         SharedWorkerState& state) {
    using aistore::client::ChunkAvailability;
    using aistore::metadata::StorageLocation;
    using aistore::metadata::StorageLocationState;

    const auto register_available = [&] {
        metadata_client.register_storage_location(StorageLocation{
            .chunk_id = task.chunk.chunk_id,
            .node_id = std::string{storage_node_id},
            .storage_path = logical_storage_path(task.chunk.chunk_id),
            .state = StorageLocationState::Available,
        });
    };

    if (task.availability == ChunkAvailability::AvailableOnTarget) {
        if (storage_client.has_chunk(task.chunk.chunk_id)) {
            state.verified_target_chunks.fetch_add(1U, std::memory_order_relaxed);
            return;
        }

        storage_client.put_chunk(task.chunk.chunk_id, task.bytes);
        state.put_requests.fetch_add(1U, std::memory_order_relaxed);
        state.repaired_target_chunks.fetch_add(1U, std::memory_order_relaxed);
        state.bytes_sent_to_storage.fetch_add(task.bytes.size(), std::memory_order_relaxed);
        register_available();
        return;
    }

    storage_client.put_chunk(task.chunk.chunk_id, task.bytes);
    state.put_requests.fetch_add(1U, std::memory_order_relaxed);
    state.bytes_sent_to_storage.fetch_add(task.bytes.size(), std::memory_order_relaxed);
    register_available();
}

void worker_loop(SharedWorkerState& state, aistore::client::MetadataClient& metadata_client,
                 aistore::client::StorageNodeClient& storage_client, const std::string& storage_node_id) {
    while (true) {
        std::optional<UploadTask> task = state.queue.pop();

        if (!task.has_value()) {
            return;
        }

        try {
            process_upload_task(*task, metadata_client, storage_client, storage_node_id, state);
        } catch (...) {
            record_failure(state, std::current_exception());
            state.queue.cancel();
            return;
        }
    }
}

class WorkerJoinGuard {
   public:
    explicit WorkerJoinGuard(std::vector<std::thread>& workers) : workers_{workers} {}

    ~WorkerJoinGuard() {
        for (std::thread& worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    WorkerJoinGuard(const WorkerJoinGuard&) = delete;

    WorkerJoinGuard& operator=(const WorkerJoinGuard&) = delete;

   private:
    std::vector<std::thread>& workers_;
};

}  // namespace

PushEngine::PushEngine(client::MetadataClient& metadata_client, client::StorageNodeClient& storage_client,
                       std::string storage_node_id)
    : metadata_client_{metadata_client}, storage_client_{storage_client}, storage_node_id_{std::move(storage_node_id)} {
    if (!is_valid_node_id(storage_node_id_)) {
        throw std::invalid_argument("storage node ID is invalid");
    }
}

PreparedPush PushEngine::push(const PushRequest& request) const {
    const std::optional<aistore::metadata::UploadSession> session =
        metadata_client_.get_upload_session(request.session_id);

    if (!session.has_value()) {
        throw std::runtime_error("upload session does not exist");
    }

    if (session->state() != aistore::metadata::UploadSessionState::Open) {
        throw std::runtime_error("upload session must be open");
    }

    if (session->target_node_id() != storage_node_id_) {
        throw std::invalid_argument("upload session target node does not match storage client node");
    }

    if (session->chunking_strategy() != aistore::metadata::ChunkingStrategy::FixedSize) {
        throw std::invalid_argument("upload session chunking strategy must be fixed-size");
    }

    if (session->chunk_size_bytes() == 0U || session->chunk_size_bytes() > kMaxM4ChunkSize ||
        session->chunk_size_bytes() > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
        throw std::invalid_argument("upload session chunk size exceeds M4 storage limits");
    }

    const auto chunk_size = static_cast<std::size_t>(session->chunk_size_bytes());

    std::ifstream input{request.source_path, std::ios::binary};

    if (!input.is_open()) {
        throw std::runtime_error("failed to open source file for push");
    }

    SharedWorkerState shared;
    std::vector<std::thread> workers;
    workers.reserve(kWorkerCount);

    std::vector<aistore::metadata::ChunkRef> layout_refs;
    std::set<std::string> seen_chunk_ids;
    std::vector<StagedChunk> staging;
    staging.reserve(kNegotiationBatchSize);

    aistore::hashing::Sha256 object_hasher;
    aistore::chunking::FixedSizeChunker chunker{chunk_size};
    std::uint64_t bytes_read = 0;

    const auto enqueue_negotiated = [&](std::vector<StagedChunk> batch) {
        if (batch.empty()) {
            return;
        }

        std::vector<aistore::metadata::ChunkMetadata> metadata_batch;
        metadata_batch.reserve(batch.size());

        for (const StagedChunk& staged : batch) {
            metadata_batch.push_back(staged.metadata);
        }

        const aistore::client::ChunkNegotiationResult negotiation =
            metadata_client_.negotiate_chunks(request.session_id, metadata_batch);

        if (negotiation.session_id != request.session_id || negotiation.target_node_id != session->target_node_id() ||
            negotiation.target_node_id != storage_node_id_ || negotiation.chunks.size() != batch.size()) {
            throw aistore::client::RemoteProtocolError{"negotiation response does not match staged batch"};
        }

        for (std::size_t index = 0; index < batch.size(); ++index) {
            const aistore::client::NegotiatedChunk& negotiated = negotiation.chunks[index];
            const StagedChunk& staged = batch[index];

            if (negotiated.chunk.chunk_id != staged.metadata.chunk_id ||
                negotiated.chunk.size_bytes != staged.metadata.size_bytes) {
                throw aistore::client::RemoteProtocolError{"negotiation chunk metadata mismatch"};
            }

            UploadTask task{
                .chunk = staged.metadata,
                .bytes = std::move(batch[index].bytes),
                .availability = negotiated.availability,
            };

            if (!shared.queue.push(std::move(task))) {
                throw std::runtime_error("push cancelled while enqueueing upload work");
            }
        }
    };

    const auto stage_unique_chunk = [&](std::string chunk_id, std::vector<std::byte> bytes) {
        StagedChunk staged{
            .metadata =
                aistore::metadata::ChunkMetadata{
                    .chunk_id = std::move(chunk_id),
                    .size_bytes = bytes.size(),
                },
            .bytes = std::move(bytes),
        };
        staging.push_back(std::move(staged));

        if (staging.size() == kNegotiationBatchSize) {
            std::vector<StagedChunk> batch = std::move(staging);
            staging.clear();
            enqueue_negotiated(std::move(batch));
        }
    };

    {
        WorkerJoinGuard join_guard{workers};

        try {
            for (std::size_t index = 0; index < kWorkerCount; ++index) {
                workers.emplace_back(worker_loop, std::ref(shared), std::ref(metadata_client_),
                                     std::ref(storage_client_), std::cref(storage_node_id_));
            }

            std::vector<std::byte> read_buffer(kReadBufferSize);

            while (input) {
                if (shared.queue.cancelled()) {
                    throw std::runtime_error("push cancelled");
                }

                input.read(reinterpret_cast<char*>(read_buffer.data()),
                           static_cast<std::streamsize>(read_buffer.size()));

                if (input.bad()) {
                    throw std::runtime_error("failed while reading source file");
                }

                const auto bytes_just_read = static_cast<std::size_t>(input.gcount());

                if (bytes_just_read == 0U) {
                    break;
                }

                const std::span<const std::byte> span{read_buffer.data(), bytes_just_read};
                object_hasher.update(span);
                bytes_read += bytes_just_read;

                chunker.update(span, [&](aistore::chunking::ChunkBuffer chunk) {
                    aistore::hashing::Sha256 chunk_hasher;
                    chunk_hasher.update(chunk.bytes);
                    const std::string chunk_id = digest_to_hex(chunk_hasher.finalize());

                    layout_refs.push_back(aistore::metadata::ChunkRef{
                        .chunk_id = chunk_id,
                        .offset = chunk.offset,
                        .size = chunk.bytes.size(),
                    });

                    const auto [iterator, inserted] = seen_chunk_ids.insert(chunk_id);

                    if (!inserted) {
                        return;
                    }

                    (void)iterator;
                    stage_unique_chunk(chunk_id, std::move(chunk.bytes));
                });
            }

            if (input.bad()) {
                throw std::runtime_error("failed while reading source file");
            }

            chunker.finalize([&](aistore::chunking::ChunkBuffer chunk) {
                aistore::hashing::Sha256 chunk_hasher;
                chunk_hasher.update(chunk.bytes);
                const std::string chunk_id = digest_to_hex(chunk_hasher.finalize());

                layout_refs.push_back(aistore::metadata::ChunkRef{
                    .chunk_id = chunk_id,
                    .offset = chunk.offset,
                    .size = chunk.bytes.size(),
                });

                const auto [iterator, inserted] = seen_chunk_ids.insert(chunk_id);

                if (!inserted) {
                    return;
                }

                (void)iterator;
                stage_unique_chunk(chunk_id, std::move(chunk.bytes));
            });

            if (!staging.empty()) {
                enqueue_negotiated(std::move(staging));
            }

            shared.queue.close();
        } catch (...) {
            record_failure(shared, std::current_exception());
            shared.queue.cancel();
        }
    }

    {
        const std::scoped_lock lock{shared.failure_mutex};

        if (shared.first_failure != nullptr) {
            std::rethrow_exception(shared.first_failure);
        }
    }

    const aistore::hashing::Sha256::Digest object_digest = object_hasher.finalize();
    const std::string object_id = digest_to_hex(object_digest);

    aistore::metadata::Object object{object_id, bytes_read};
    aistore::metadata::ObjectLayout layout{std::move(layout_refs)};
    aistore::metadata::ObjectLayoutDescriptor descriptor{
        std::move(object), aistore::metadata::ChunkingStrategy::FixedSize, std::move(layout)};

    const std::size_t total_chunks = descriptor.layout().chunks().size();
    const std::size_t unique_chunks = seen_chunk_ids.size();

    return PreparedPush{
        .session_id = request.session_id,
        .layout_descriptor = std::move(descriptor),
        .stats =
            PushStats{
                .bytes_read = bytes_read,
                .total_chunks = total_chunks,
                .unique_chunks = unique_chunks,
                .put_requests = shared.put_requests.load(std::memory_order_relaxed),
                .verified_target_chunks = shared.verified_target_chunks.load(std::memory_order_relaxed),
                .repaired_target_chunks = shared.repaired_target_chunks.load(std::memory_order_relaxed),
                .bytes_sent_to_storage = shared.bytes_sent_to_storage.load(std::memory_order_relaxed),
            },
    };
}

}  // namespace aistore::push
