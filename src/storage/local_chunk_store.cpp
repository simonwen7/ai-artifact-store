#include "aistore/storage/local_chunk_store.hpp"

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include "aistore/hashing/sha256.hpp"

namespace aistore::storage {

namespace {

using aistore::hashing::Sha256;

std::string digest_to_hex(const Sha256::Digest& digest) {
    constexpr std::array<char, 16> kHexDigits{
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    };

    std::string result;
    result.reserve(digest.size() * 2);

    for (const std::byte byte : digest) {
        const auto value = std::to_integer<unsigned int>(byte);

        result.push_back(kHexDigits[(value >> 4U) & 0x0FU]);
        result.push_back(kHexDigits[value & 0x0FU]);
    }

    return result;
}

std::string hash_bytes(std::span<const std::byte> data) {
    Sha256 hasher;
    hasher.update(data);

    return digest_to_hex(hasher.finalize());
}

class FileDescriptor {
   public:
    explicit FileDescriptor(int descriptor) : descriptor_(descriptor) {}

    ~FileDescriptor() {
        if (descriptor_ >= 0) {
            ::close(descriptor_);
        }
    }

    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    FileDescriptor(FileDescriptor&& other) noexcept : descriptor_(std::exchange(other.descriptor_, -1)) {}

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
            throw std::system_error(errno, std::generic_category(), "failed to close temporary chunk file");
        }
    }

   private:
    int descriptor_;
};

class TemporaryFile {
   public:
    explicit TemporaryFile(std::filesystem::path path) : path_(std::move(path)) {}

    ~TemporaryFile() {
        if (active_) {
            std::error_code error;
            std::filesystem::remove(path_, error);
        }
    }

    TemporaryFile(const TemporaryFile&) = delete;
    TemporaryFile& operator=(const TemporaryFile&) = delete;

    [[nodiscard]] const std::filesystem::path& path() const noexcept { return path_; }

    void release() noexcept { active_ = false; }

   private:
    std::filesystem::path path_;
    bool active_ = true;
};

void write_all(int descriptor, std::span<const std::byte> data) {
    std::size_t position = 0;

    while (position < data.size()) {
        const auto* current = data.data() + position;
        const std::size_t remaining = data.size() - position;

        const ssize_t written = ::write(descriptor, current, remaining);

        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }

            throw std::system_error(errno, std::generic_category(), "failed to write temporary chunk file");
        }

        if (written == 0) {
            throw std::runtime_error("temporary chunk write made no progress");
        }

        position += static_cast<std::size_t>(written);
    }
}

std::filesystem::path create_temporary_file(const std::filesystem::path& directory, FileDescriptor& descriptor) {
    std::string pattern = (directory / ".aistore-chunk-XXXXXX").string();

    std::vector<char> mutable_pattern(pattern.begin(), pattern.end());

    mutable_pattern.push_back('\0');

    const int raw_descriptor = ::mkstemp(mutable_pattern.data());

    if (raw_descriptor < 0) {
        throw std::system_error(errno, std::generic_category(), "failed to create temporary chunk file");
    }

    descriptor = FileDescriptor{raw_descriptor};

    return std::filesystem::path{mutable_pattern.data()};
}

}  // namespace

LocalChunkStore::LocalChunkStore(std::filesystem::path root_directory) : root_directory_(std::move(root_directory)) {
    if (root_directory_.empty()) {
        throw std::invalid_argument("chunk store root directory must not be empty");
    }

    std::filesystem::create_directories(root_directory_ / "chunks");
}

void LocalChunkStore::put(std::string_view expected_chunk_id, std::span<const std::byte> data) {
    validate_chunk_id(expected_chunk_id);

    const std::filesystem::path final_path = chunk_path(expected_chunk_id);

    if (std::filesystem::is_regular_file(final_path)) {
        static_cast<void>(get(expected_chunk_id));
        return;
    }

    std::filesystem::create_directories(final_path.parent_path());

    FileDescriptor descriptor{-1};

    const std::filesystem::path temporary_path = create_temporary_file(final_path.parent_path(), descriptor);

    TemporaryFile temporary_file{temporary_path};

    Sha256 hasher;
    hasher.update(data);

    write_all(descriptor.get(), data);

    if (::fsync(descriptor.get()) != 0) {
        throw std::system_error(errno, std::generic_category(), "failed to flush temporary chunk file");
    }

    descriptor.close();

    const std::string actual_chunk_id = digest_to_hex(hasher.finalize());

    if (actual_chunk_id != expected_chunk_id) {
        throw std::runtime_error("chunk content does not match expected chunk ID");
    }

    std::filesystem::rename(temporary_path, final_path);

    temporary_file.release();
}

std::vector<std::byte> LocalChunkStore::get(std::string_view chunk_id) const {
    validate_chunk_id(chunk_id);

    const std::filesystem::path path = chunk_path(chunk_id);

    if (!std::filesystem::is_regular_file(path)) {
        throw std::runtime_error("requested chunk does not exist");
    }

    const std::uintmax_t file_size = std::filesystem::file_size(path);

    if (file_size > static_cast<std::uintmax_t>(std::numeric_limits<std::size_t>::max())) {
        throw std::runtime_error("chunk is too large to load into memory");
    }

    std::vector<std::byte> data(static_cast<std::size_t>(file_size));

    std::ifstream input(path, std::ios::binary);

    if (!input) {
        throw std::runtime_error("failed to open chunk for reading");
    }

    if (!data.empty()) {
        if (data.size() > static_cast<std::size_t>(std::numeric_limits<std::streamsize>::max())) {
            throw std::runtime_error("chunk is too large for stream read");
        }

        input.read(reinterpret_cast<char*>(data.data()), static_cast<std::streamsize>(data.size()));

        if (!input) {
            throw std::runtime_error("failed to read complete chunk");
        }
    }

    if (hash_bytes(data) != chunk_id) {
        throw std::runtime_error("stored chunk failed SHA-256 verification");
    }

    return data;
}

bool LocalChunkStore::contains(std::string_view chunk_id) const {
    validate_chunk_id(chunk_id);

    return std::filesystem::is_regular_file(chunk_path(chunk_id));
}

const std::filesystem::path& LocalChunkStore::root_directory() const noexcept { return root_directory_; }

void LocalChunkStore::validate_chunk_id(std::string_view chunk_id) {
    if (chunk_id.size() != 64) {
        throw std::invalid_argument("chunk ID must contain exactly 64 hexadecimal characters");
    }

    for (const char character : chunk_id) {
        const bool is_digit = character >= '0' && character <= '9';

        const bool is_lower_hex = character >= 'a' && character <= 'f';

        if (!is_digit && !is_lower_hex) {
            throw std::invalid_argument("chunk ID must use lowercase hexadecimal characters");
        }
    }
}

std::filesystem::path LocalChunkStore::chunk_path(std::string_view chunk_id) const {
    validate_chunk_id(chunk_id);

    const std::string chunk_id_string{chunk_id};

    return root_directory_ / "chunks" / chunk_id_string.substr(0, 2) / chunk_id_string;
}

}  // namespace aistore::storage
