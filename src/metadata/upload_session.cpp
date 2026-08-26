#include "aistore/metadata/upload_session.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace aistore::metadata {

UploadSession::UploadSession(UuidV7 session_id, UuidV7 artifact_id, std::uint8_t replication_factor,
                             std::vector<std::string> placement_node_ids, ChunkingStrategy chunking_strategy,
                             std::optional<std::uint64_t> fixed_chunk_size_bytes,
                             std::optional<FastCdcParameters> fastcdc_parameters,
                             std::optional<std::string> parent_version_id, ImmutableMetadata immutable_metadata,
                             UploadSessionState state, std::optional<std::string> finalized_version_id)
    : session_id_(std::move(session_id)),
      artifact_id_(std::move(artifact_id)),
      target_node_id_(placement_node_ids.empty() ? std::string{} : placement_node_ids.front()),
      replication_factor_(replication_factor),
      placement_node_ids_(std::move(placement_node_ids)),
      chunking_strategy_(chunking_strategy),
      fixed_chunk_size_bytes_(fixed_chunk_size_bytes),
      fastcdc_parameters_(fastcdc_parameters),
      parent_version_id_(std::move(parent_version_id)),
      immutable_metadata_(std::move(immutable_metadata)),
      state_(state),
      finalized_version_id_(std::move(finalized_version_id)) {
    validate_node_id(target_node_id_);
    validate_replication_configuration(replication_factor_, placement_node_ids_, target_node_id_);

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
    : UploadSession(std::move(session_id), std::move(artifact_id), 1U,
                    std::vector<std::string>{std::move(target_node_id)}, chunking_strategy,
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
    : UploadSession(std::move(session_id), std::move(artifact_id), 1U,
                    std::vector<std::string>{std::move(target_node_id)}, ChunkingStrategy::FastCdc, std::nullopt,
                    std::optional<FastCdcParameters>{fastcdc_parameters}, std::move(parent_version_id),
                    std::move(immutable_metadata), state, std::move(finalized_version_id)) {}

UploadSession::UploadSession(UuidV7 session_id, UuidV7 artifact_id, std::uint8_t replication_factor,
                             std::vector<std::string> placement_node_ids, ChunkingStrategy chunking_strategy,
                             std::uint64_t chunk_size_bytes, std::optional<std::string> parent_version_id,
                             ImmutableMetadata immutable_metadata, UploadSessionState state,
                             std::optional<std::string> finalized_version_id)
    : UploadSession(std::move(session_id), std::move(artifact_id), replication_factor, std::move(placement_node_ids),
                    chunking_strategy, std::optional<std::uint64_t>{chunk_size_bytes}, std::nullopt,
                    std::move(parent_version_id), std::move(immutable_metadata), state,
                    std::move(finalized_version_id)) {
    if (chunking_strategy != ChunkingStrategy::FixedSize) {
        throw std::invalid_argument("FixedSize UploadSession constructor requires FixedSize strategy");
    }
}

UploadSession::UploadSession(UuidV7 session_id, UuidV7 artifact_id, std::uint8_t replication_factor,
                             std::vector<std::string> placement_node_ids, FastCdcParameters fastcdc_parameters,
                             std::optional<std::string> parent_version_id, ImmutableMetadata immutable_metadata,
                             UploadSessionState state, std::optional<std::string> finalized_version_id)
    : UploadSession(std::move(session_id), std::move(artifact_id), replication_factor, std::move(placement_node_ids),
                    ChunkingStrategy::FastCdc, std::nullopt, std::optional<FastCdcParameters>{fastcdc_parameters},
                    std::move(parent_version_id), std::move(immutable_metadata), state,
                    std::move(finalized_version_id)) {}

const UuidV7& UploadSession::session_id() const noexcept { return session_id_; }

const UuidV7& UploadSession::artifact_id() const noexcept { return artifact_id_; }

const std::string& UploadSession::target_node_id() const noexcept { return target_node_id_; }

std::uint8_t UploadSession::replication_factor() const noexcept { return replication_factor_; }

const std::vector<std::string>& UploadSession::placement_node_ids() const noexcept { return placement_node_ids_; }

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

void UploadSession::validate_replication_configuration(std::uint8_t replication_factor,
                                                       const std::vector<std::string>& placement_node_ids,
                                                       std::string_view target_node_id) {
    if (replication_factor < 1U || replication_factor > 8U) {
        throw std::invalid_argument("upload session replication factor must be between 1 and 8");
    }

    if (placement_node_ids.empty() || placement_node_ids.size() > 64U) {
        throw std::invalid_argument("upload session placement node set size must be between 1 and 64");
    }

    if (replication_factor > placement_node_ids.size()) {
        throw std::invalid_argument("upload session replication factor must not exceed placement node count");
    }

    if (placement_node_ids.front() != target_node_id) {
        throw std::invalid_argument("upload session target node ID must equal the first placement node");
    }

    for (std::size_t index = 0; index < placement_node_ids.size(); ++index) {
        validate_node_id(placement_node_ids[index]);

        if (index > 0U && placement_node_ids[index - 1U] >= placement_node_ids[index]) {
            throw std::invalid_argument("upload session placement node IDs must be sorted and unique");
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

bool upload_session_identity_matches(const UploadSession& left, const UploadSession& right) {
    return left.session_id() == right.session_id() && left.artifact_id() == right.artifact_id() &&
           left.target_node_id() == right.target_node_id() && left.replication_factor() == right.replication_factor() &&
           left.placement_node_ids() == right.placement_node_ids() &&
           left.chunking_strategy() == right.chunking_strategy() &&
           left.fixed_chunk_size_bytes() == right.fixed_chunk_size_bytes() &&
           left.fastcdc_parameters() == right.fastcdc_parameters() &&
           left.parent_version_id() == right.parent_version_id() &&
           left.immutable_metadata() == right.immutable_metadata();
}

}  // namespace aistore::metadata
