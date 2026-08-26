#include "aistore/metadata/upload_session.hpp"

#include <stdexcept>
#include <utility>

namespace aistore::metadata {

UploadSession::UploadSession(UuidV7 session_id, UuidV7 artifact_id, std::string target_node_id,
                             ChunkingStrategy chunking_strategy, std::optional<std::uint64_t> fixed_chunk_size_bytes,
                             std::optional<FastCdcParameters> fastcdc_parameters,
                             std::optional<std::string> parent_version_id, ImmutableMetadata immutable_metadata,
                             UploadSessionState state, std::optional<std::string> finalized_version_id)
    : session_id_(std::move(session_id)),
      artifact_id_(std::move(artifact_id)),
      target_node_id_(std::move(target_node_id)),
      chunking_strategy_(chunking_strategy),
      fixed_chunk_size_bytes_(fixed_chunk_size_bytes),
      fastcdc_parameters_(fastcdc_parameters),
      parent_version_id_(std::move(parent_version_id)),
      immutable_metadata_(std::move(immutable_metadata)),
      state_(state),
      finalized_version_id_(std::move(finalized_version_id)) {
    validate_node_id(target_node_id_);

    switch (chunking_strategy_) {
        case ChunkingStrategy::FixedSize:
            if (!fixed_chunk_size_bytes_.has_value() || *fixed_chunk_size_bytes_ == 0U) {
                throw std::invalid_argument("upload session FixedSize chunk size must be greater than zero");
            }
            if (fastcdc_parameters_.has_value()) {
                throw std::invalid_argument("upload session FixedSize must not carry FastCDC parameters");
            }
            break;

        case ChunkingStrategy::FastCdc:
            if (fixed_chunk_size_bytes_.has_value()) {
                throw std::invalid_argument("upload session FastCDC must not carry fixed chunk size");
            }
            if (!fastcdc_parameters_.has_value()) {
                throw std::invalid_argument("upload session FastCDC requires FastCDC parameters");
            }
            validate_fastcdc_parameters(*fastcdc_parameters_);
            break;
    }

    if (parent_version_id_.has_value()) {
        validate_content_id(*parent_version_id_, "parent version ID");
    }

    validate_metadata(immutable_metadata_);

    if (finalized_version_id_.has_value()) {
        validate_content_id(*finalized_version_id_, "finalized version ID");
    }

    switch (state_) {
        case UploadSessionState::Open:
        case UploadSessionState::Aborted:
            if (finalized_version_id_.has_value()) {
                throw std::invalid_argument("non-committed upload session must not have a finalized version ID");
            }
            break;

        case UploadSessionState::Committed:
            if (!finalized_version_id_.has_value()) {
                throw std::invalid_argument("committed upload session must have a finalized version ID");
            }
            break;
    }

    if (parent_version_id_.has_value() && finalized_version_id_.has_value() &&
        *parent_version_id_ == *finalized_version_id_) {
        throw std::invalid_argument("upload session finalized version ID must not equal parent version ID");
    }
}

UploadSession::UploadSession(UuidV7 session_id, UuidV7 artifact_id, std::string target_node_id,
                             ChunkingStrategy chunking_strategy, std::uint64_t chunk_size_bytes,
                             std::optional<std::string> parent_version_id, ImmutableMetadata immutable_metadata,
                             UploadSessionState state, std::optional<std::string> finalized_version_id)
    : UploadSession(std::move(session_id), std::move(artifact_id), std::move(target_node_id), chunking_strategy,
                    std::optional<std::uint64_t>{chunk_size_bytes}, std::nullopt, std::move(parent_version_id),
                    std::move(immutable_metadata), state, std::move(finalized_version_id)) {
    if (chunking_strategy != ChunkingStrategy::FixedSize) {
        throw std::invalid_argument("FixedSize UploadSession constructor requires FixedSize strategy");
    }
}

UploadSession::UploadSession(UuidV7 session_id, UuidV7 artifact_id, std::string target_node_id,
                             FastCdcParameters fastcdc_parameters, std::optional<std::string> parent_version_id,
                             ImmutableMetadata immutable_metadata, UploadSessionState state,
                             std::optional<std::string> finalized_version_id)
    : UploadSession(std::move(session_id), std::move(artifact_id), std::move(target_node_id), ChunkingStrategy::FastCdc,
                    std::nullopt, std::optional<FastCdcParameters>{fastcdc_parameters}, std::move(parent_version_id),
                    std::move(immutable_metadata), state, std::move(finalized_version_id)) {}

const UuidV7& UploadSession::session_id() const noexcept { return session_id_; }

const UuidV7& UploadSession::artifact_id() const noexcept { return artifact_id_; }

const std::string& UploadSession::target_node_id() const noexcept { return target_node_id_; }

ChunkingStrategy UploadSession::chunking_strategy() const noexcept { return chunking_strategy_; }

std::optional<std::uint64_t> UploadSession::fixed_chunk_size_bytes() const noexcept { return fixed_chunk_size_bytes_; }

std::optional<FastCdcParameters> UploadSession::fastcdc_parameters() const noexcept { return fastcdc_parameters_; }

std::uint64_t UploadSession::chunk_size_bytes() const {
    if (!fixed_chunk_size_bytes_.has_value()) {
        throw std::logic_error("chunk_size_bytes() is only valid for FixedSize upload sessions");
    }

    return *fixed_chunk_size_bytes_;
}

const std::optional<std::string>& UploadSession::parent_version_id() const noexcept { return parent_version_id_; }

const UploadSession::ImmutableMetadata& UploadSession::immutable_metadata() const noexcept {
    return immutable_metadata_;
}

UploadSessionState UploadSession::state() const noexcept { return state_; }

const std::optional<std::string>& UploadSession::finalized_version_id() const noexcept { return finalized_version_id_; }

void UploadSession::validate_node_id(std::string_view node_id) {
    if (node_id.empty() || node_id.size() > 128U) {
        throw std::invalid_argument("upload session target node ID must be 1 to 128 characters");
    }

    for (const char character : node_id) {
        const bool is_upper = character >= 'A' && character <= 'Z';
        const bool is_lower = character >= 'a' && character <= 'z';
        const bool is_digit = character >= '0' && character <= '9';
        const bool is_allowed_punct = character == '-' || character == '_' || character == '.';

        if (!is_upper && !is_lower && !is_digit && !is_allowed_punct) {
            throw std::invalid_argument("upload session target node ID contains an invalid character");
        }
    }
}

void UploadSession::validate_content_id(const std::string& content_id, const char* field_name) {
    if (content_id.size() != 64) {
        throw std::invalid_argument(std::string{field_name} +
                                    " must contain exactly 64 "
                                    "hexadecimal characters");
    }

    for (const char character : content_id) {
        const bool is_digit = character >= '0' && character <= '9';
        const bool is_lower_hex = character >= 'a' && character <= 'f';

        if (!is_digit && !is_lower_hex) {
            throw std::invalid_argument(std::string{field_name} +
                                        " must use lowercase "
                                        "hexadecimal characters");
        }
    }
}

void UploadSession::validate_metadata(const ImmutableMetadata& immutable_metadata) {
    for (const auto& [key, value] : immutable_metadata) {
        static_cast<void>(value);

        if (key.empty()) {
            throw std::invalid_argument("immutable metadata key must not be empty");
        }
    }
}

}  // namespace aistore::metadata
