#include "aistore/pull/pull_engine.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include "aistore/client/client_error.hpp"
#include "aistore/hashing/sha256.hpp"
#include "aistore/metadata/restore_plan.hpp"

namespace aistore::pull {

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

class FileDescriptor {
   public:
    explicit FileDescriptor(int descriptor) : descriptor_{descriptor} {}

    ~FileDescriptor() {
        if (descriptor_ >= 0) {
            ::close(descriptor_);
        }
    }

    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    FileDescriptor(FileDescriptor&& other) noexcept : descriptor_{std::exchange(other.descriptor_, -1)} {}

    FileDescriptor& operator=(FileDescriptor&& other) noexcept {
        if (this != &other) {
            if (descriptor_ >= 0) {
                ::close(descriptor_);
            }

            descriptor_ = std::exchange(other.descriptor_, -1);
        }

        return *this;
    }

    [[nodiscard]] int get() const noexcept { return descriptor_; }

    void close() {
        if (descriptor_ < 0) {
            return;
        }

        const int descriptor = std::exchange(descriptor_, -1);

        if (::close(descriptor) != 0) {
            throw std::system_error(errno, std::generic_category(), "failed to close partial restore file");
        }
    }

   private:
    int descriptor_;
};

void write_all(int descriptor, std::uint64_t offset, std::span<const std::byte> data) {
    std::size_t position = 0;

    while (position < data.size()) {
        const auto* current = data.data() + position;
        const std::size_t remaining = data.size() - position;

        if (remaining > static_cast<std::size_t>(std::numeric_limits<ssize_t>::max())) {
            throw std::runtime_error("partial restore write exceeds platform write limit");
        }

        const ssize_t written =
            ::pwrite(descriptor, current, static_cast<ssize_t>(remaining), static_cast<off_t>(offset + position));

        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }

            throw std::system_error(errno, std::generic_category(), "failed to write partial restore file");
        }

        if (written == 0) {
            throw std::runtime_error("partial restore write made no progress");
        }

        position += static_cast<std::size_t>(written);
    }
}

void read_all(int descriptor, std::uint64_t offset, std::span<std::byte> buffer) {
    std::size_t position = 0;

    while (position < buffer.size()) {
        const std::size_t remaining = buffer.size() - position;

        if (remaining > static_cast<std::size_t>(std::numeric_limits<ssize_t>::max())) {
            throw std::runtime_error("partial restore read exceeds platform read limit");
        }

        const ssize_t bytes_read = ::pread(descriptor, buffer.data() + position, static_cast<ssize_t>(remaining),
                                           static_cast<off_t>(offset + position));

        if (bytes_read < 0) {
            if (errno == EINTR) {
                continue;
            }

            throw std::system_error(errno, std::generic_category(), "failed to read partial restore file");
        }

        if (bytes_read == 0) {
            throw std::runtime_error("partial restore read ended before expected bytes");
        }

        position += static_cast<std::size_t>(bytes_read);
    }
}

void truncate_file(int descriptor, std::uint64_t length) {
    if (length > static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
        throw std::runtime_error("partial restore truncate length exceeds platform limit");
    }

    if (::ftruncate(descriptor, static_cast<off_t>(length)) != 0) {
        throw std::system_error(errno, std::generic_category(), "failed to truncate partial restore file");
    }
}

[[nodiscard]] std::uint64_t file_size(int descriptor) {
    struct stat status{};

    if (::fstat(descriptor, &status) != 0) {
        throw std::system_error(errno, std::generic_category(), "failed to stat partial restore file");
    }

    if (!S_ISREG(status.st_mode)) {
        throw std::runtime_error("partial restore path is not a regular file");
    }

    return static_cast<std::uint64_t>(status.st_size);
}

[[nodiscard]] FileDescriptor open_partial_file(const std::filesystem::path& partial_path) {
    int open_flags = O_RDWR | O_CREAT;

#ifdef O_NOFOLLOW
    open_flags |= O_NOFOLLOW;
#endif

    const int raw_descriptor = ::open(partial_path.c_str(), open_flags, 0600);

    if (raw_descriptor < 0) {
        throw std::system_error(errno, std::generic_category(), "failed to open partial restore file");
    }

    FileDescriptor descriptor{raw_descriptor};

    struct stat status{};

    if (::fstat(descriptor.get(), &status) != 0) {
        throw std::system_error(errno, std::generic_category(), "failed to stat partial restore file");
    }

    if (!S_ISREG(status.st_mode)) {
        throw std::runtime_error("partial restore path is not a regular file");
    }

    return descriptor;
}

void validate_destination(const PullRequest& request) {
    if (request.destination_path.empty()) {
        throw std::invalid_argument("destination path must not be empty");
    }

    const std::filesystem::path parent_path = request.destination_path.parent_path();

    if (parent_path.empty()) {
        throw std::invalid_argument("destination parent directory must exist");
    }

    if (!std::filesystem::exists(parent_path)) {
        throw std::invalid_argument("destination parent directory does not exist");
    }

    if (!std::filesystem::is_directory(parent_path)) {
        throw std::invalid_argument("destination parent path is not a directory");
    }

    if (std::filesystem::is_directory(request.destination_path)) {
        throw std::invalid_argument("destination path must not be a directory");
    }

    if (std::filesystem::exists(request.destination_path) && !request.overwrite) {
        throw std::runtime_error("destination already exists and overwrite is disabled");
    }
}

void validate_chunk_sizes(const std::vector<aistore::metadata::ChunkRef>& chunks) {
    for (const aistore::metadata::ChunkRef& chunk : chunks) {
        if (chunk.size > PullEngine::kMaxM5ChunkSize) {
            throw std::invalid_argument("restore plan chunk size exceeds M5 storage limits");
        }

        if (chunk.size > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
            throw std::invalid_argument("restore plan chunk size exceeds addressable memory limits");
        }
    }
}

[[nodiscard]] bool chunk_bytes_match_id(std::span<const std::byte> bytes, std::string_view chunk_id) {
    aistore::hashing::Sha256 chunk_hasher;
    chunk_hasher.update(bytes);

    return digest_to_hex(chunk_hasher.finalize()) == chunk_id;
}

struct ResumeState {
    std::size_t first_download_index{0};
    std::size_t chunks_reused_from_partial{0};
    aistore::hashing::Sha256 object_hasher;
};

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters) — descriptor then size then chunk list is intentional
[[nodiscard]] ResumeState resume_from_partial(int partial_descriptor, std::uint64_t total_size,
                                              const std::vector<aistore::metadata::ChunkRef>& chunks) {
    ResumeState state;

    std::uint64_t partial_size = file_size(partial_descriptor);

    if (partial_size > total_size) {
        truncate_file(partial_descriptor, 0);
        partial_size = 0;
    }

    for (std::size_t index = 0; index < chunks.size(); ++index) {
        const aistore::metadata::ChunkRef& chunk = chunks[index];
        const std::uint64_t chunk_end = chunk.offset + chunk.size;

        if (partial_size >= chunk_end) {
            if (chunk.size > static_cast<std::uint64_t>(std::numeric_limits<std::size_t>::max())) {
                throw std::runtime_error("partial restore chunk size exceeds addressable memory limits");
            }

            std::vector<std::byte> bytes(static_cast<std::size_t>(chunk.size));
            read_all(partial_descriptor, chunk.offset, bytes);

            if (!chunk_bytes_match_id(bytes, chunk.chunk_id)) {
                truncate_file(partial_descriptor, chunk.offset);
                break;
            }

            state.object_hasher.update(bytes);
            state.chunks_reused_from_partial += 1U;
            state.first_download_index = index + 1U;
            continue;
        }

        if (partial_size > chunk.offset) {
            truncate_file(partial_descriptor, chunk.offset);
        }

        break;
    }

    return state;
}

struct SharedDownloadState {
    std::mutex mutex;
    std::condition_variable cv;
    std::exception_ptr first_failure;
    bool cancelled{false};

    const std::vector<aistore::metadata::ChunkRef>* chunks{nullptr};
    const std::vector<aistore::metadata::RestoreChunkSources>* chunk_sources{nullptr};
    std::size_t total_chunks{0};
    std::size_t next_write_index{0};
    std::size_t next_claim_index{0};

    std::map<std::size_t, std::vector<std::byte>> ready;

    std::atomic<std::size_t> chunks_downloaded{0};
    std::atomic<std::uint64_t> bytes_received_from_storage{0};
};

void record_failure(SharedDownloadState& state, const std::exception_ptr& exception) {
    const std::scoped_lock lock{state.mutex};

    if (state.first_failure == nullptr) {
        state.first_failure = exception;
    }
}

void cancel_download(SharedDownloadState& state) {
    const std::scoped_lock lock{state.mutex};
    state.cancelled = true;
    state.cv.notify_all();
}

void download_worker_loop(SharedDownloadState& state, aistore::client::StorageNodeClientPool& storage_pool) {
    while (true) {
        std::size_t chunk_index = 0;

        {
            std::unique_lock lock{state.mutex};
            state.cv.wait(lock, [&] {
                if (state.cancelled || state.first_failure != nullptr) {
                    return true;
                }

                if (state.next_claim_index >= state.total_chunks) {
                    return true;
                }

                return state.next_claim_index < state.next_write_index + PullEngine::kWindowCapacity;
            });

            if (state.cancelled || state.first_failure != nullptr) {
                return;
            }

            if (state.next_claim_index >= state.total_chunks) {
                return;
            }

            chunk_index = state.next_claim_index;
            state.next_claim_index += 1U;
        }

        try {
            const aistore::metadata::ChunkRef& chunk = (*state.chunks)[chunk_index];
            const aistore::metadata::RestoreChunkSources& sources = (*state.chunk_sources)[chunk_index];

            std::optional<std::vector<std::byte>> downloaded;

            for (const aistore::metadata::RestoreNodeEndpoint& source : sources.sources) {
                try {
                    downloaded = storage_pool.client_for(source.node_id).get_chunk(chunk.chunk_id);

                    if (downloaded.has_value()) {
                        break;
                    }
                } catch (const aistore::client::RemoteApiError&) {
                    continue;
                } catch (const aistore::client::RemoteProtocolError&) {
                    continue;
                } catch (const std::runtime_error&) {
                    continue;
                }
            }

            if (!downloaded.has_value()) {
                throw std::runtime_error("required chunk missing from all configured source nodes");
            }

            if (downloaded->size() != chunk.size) {
                throw std::runtime_error("downloaded chunk size does not match restore plan");
            }

            const std::scoped_lock lock{state.mutex};
            state.ready.emplace(chunk_index, std::move(*downloaded));
            state.chunks_downloaded.fetch_add(1U, std::memory_order_relaxed);
            state.bytes_received_from_storage.fetch_add(chunk.size, std::memory_order_relaxed);
            state.cv.notify_all();
        } catch (...) {
            record_failure(state, std::current_exception());
            cancel_download(state);
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

void publish_partial(const std::filesystem::path& partial_path, const std::filesystem::path& destination_path,
                     bool overwrite) {
    const std::string partial_string = partial_path.string();
    const std::string destination_string = destination_path.string();

    if (overwrite) {
        if (::rename(partial_string.c_str(), destination_string.c_str()) != 0) {
            throw std::system_error(errno, std::generic_category(), "failed to publish restored destination");
        }

        return;
    }

    if (::link(partial_string.c_str(), destination_string.c_str()) != 0) {
        throw std::system_error(errno, std::generic_category(), "failed to publish restored destination");
    }

    if (::unlink(partial_string.c_str()) != 0) {
        // Best-effort removal after successful hard-link publication.
    }
}

}  // namespace

PullEngine::PullEngine(client::MetadataClient& metadata_client, client::StorageNodeClientPool& storage_pool)
    : metadata_client_{metadata_client}, storage_pool_{storage_pool} {}

PullResult PullEngine::pull(const PullRequest& request) const {
    validate_destination(request);

    const aistore::metadata::MultiNodeRestorePlan plan =
        metadata_client_.get_multi_node_restore_plan(request.version_id);

    const aistore::metadata::ObjectLayoutDescriptor& descriptor = plan.layout_descriptor;
    const std::vector<aistore::metadata::ChunkRef>& chunks = descriptor.layout().chunks();
    validate_chunk_sizes(chunks);

    if (plan.chunks.size() != chunks.size()) {
        throw std::runtime_error("multi-node restore plan chunk count does not match layout");
    }

    for (std::size_t index = 0; index < plan.chunks.size(); ++index) {
        if (plan.chunks[index].chunk_id != chunks[index].chunk_id ||
            plan.chunks[index].offset != chunks[index].offset || plan.chunks[index].size_bytes != chunks[index].size) {
            throw std::runtime_error("multi-node restore plan chunk metadata does not match layout");
        }
    }

    std::string primary_source_node_id;

    if (!plan.chunks.empty() && !plan.chunks.front().sources.empty()) {
        primary_source_node_id = plan.chunks.front().sources.front().node_id;
    }

    const std::filesystem::path partial_path =
        std::filesystem::path{request.destination_path.string() + ".aistore." + request.version_id + ".part"};

    FileDescriptor partial_descriptor = open_partial_file(partial_path);

    const std::uint64_t total_size = descriptor.object().total_size();
    ResumeState resume_state = resume_from_partial(partial_descriptor.get(), total_size, chunks);

    const std::size_t total_chunks = chunks.size();
    std::size_t chunks_downloaded = 0U;
    std::uint64_t bytes_received_from_storage = 0U;

    if (resume_state.first_download_index < total_chunks) {
        SharedDownloadState shared;
        shared.chunks = &chunks;
        shared.chunk_sources = &plan.chunks;
        shared.total_chunks = total_chunks;
        shared.next_write_index = resume_state.first_download_index;
        shared.next_claim_index = resume_state.first_download_index;

        std::vector<std::thread> workers;
        workers.reserve(kWorkerCount);

        {
            WorkerJoinGuard join_guard{workers};

            try {
                for (std::size_t worker_index = 0; worker_index < kWorkerCount; ++worker_index) {
                    workers.emplace_back(download_worker_loop, std::ref(shared), std::ref(storage_pool_));
                }

                for (std::size_t chunk_index = resume_state.first_download_index; chunk_index < total_chunks;
                     ++chunk_index) {
                    std::vector<std::byte> chunk_bytes;

                    {
                        std::unique_lock lock{shared.mutex};
                        shared.cv.wait(lock, [&] {
                            if (shared.cancelled || shared.first_failure != nullptr) {
                                return true;
                            }

                            return shared.ready.find(chunk_index) != shared.ready.end();
                        });

                        if (shared.first_failure != nullptr) {
                            std::rethrow_exception(shared.first_failure);
                        }

                        if (shared.cancelled) {
                            throw std::runtime_error("pull cancelled");
                        }

                        chunk_bytes = std::move(shared.ready.at(chunk_index));
                        shared.ready.erase(chunk_index);
                    }

                    const aistore::metadata::ChunkRef& chunk = chunks[chunk_index];
                    write_all(partial_descriptor.get(), chunk.offset, chunk_bytes);
                    resume_state.object_hasher.update(chunk_bytes);

                    {
                        const std::scoped_lock lock{shared.mutex};
                        shared.next_write_index = chunk_index + 1U;
                        shared.cv.notify_all();
                    }
                }

                cancel_download(shared);
            } catch (...) {
                record_failure(shared, std::current_exception());
                cancel_download(shared);
            }
        }

        {
            const std::scoped_lock lock{shared.mutex};

            if (shared.first_failure != nullptr) {
                std::rethrow_exception(shared.first_failure);
            }
        }

        chunks_downloaded = shared.chunks_downloaded.load(std::memory_order_relaxed);
        bytes_received_from_storage = shared.bytes_received_from_storage.load(std::memory_order_relaxed);
    }

    const std::string computed_object_id = digest_to_hex(resume_state.object_hasher.finalize());

    if (computed_object_id != descriptor.object_id()) {
        throw std::runtime_error("restored object hash does not match expected object ID");
    }

    if (::fsync(partial_descriptor.get()) != 0) {
        throw std::system_error(errno, std::generic_category(), "failed to flush partial restore file");
    }

    partial_descriptor.close();
    publish_partial(partial_path, request.destination_path, request.overwrite);

    return PullResult{
        .version_id = plan.version_id,
        .artifact_id = plan.artifact_id,
        .source_node_id = primary_source_node_id,
        .object_id = descriptor.object_id(),
        .layout_id = descriptor.layout_id(),
        .destination_path = request.destination_path,
        .stats =
            PullStats{
                .bytes_restored = total_size,
                .total_chunks = total_chunks,
                .chunks_downloaded = chunks_downloaded,
                .chunks_reused_from_partial = resume_state.chunks_reused_from_partial,
                .bytes_received_from_storage = bytes_received_from_storage,
            },
    };
}

}  // namespace aistore::pull
