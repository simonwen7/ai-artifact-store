#ifndef AISTORE_METADATA_OBJECT_LAYOUT_DESCRIPTOR_HPP
#define AISTORE_METADATA_OBJECT_LAYOUT_DESCRIPTOR_HPP

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "aistore/metadata/chunking.hpp"
#include "aistore/metadata/object.hpp"
#include "aistore/metadata/object_layout.hpp"

namespace aistore::metadata {

class ObjectLayoutDescriptor {
   public:
    static constexpr std::uint32_t kFormatVersion = 1;

    ObjectLayoutDescriptor(Object object, ChunkingStrategy chunking_strategy, ObjectLayout layout);

    ObjectLayoutDescriptor(Object object, FastCdcParameters fastcdc_parameters, ObjectLayout layout);

    [[nodiscard]] const Object& object() const noexcept;

    [[nodiscard]] const std::string& object_id() const noexcept;

    [[nodiscard]] ChunkingStrategy chunking_strategy() const noexcept;

    [[nodiscard]] std::optional<FastCdcParameters> fastcdc_parameters() const noexcept;

    [[nodiscard]] const ObjectLayout& layout() const noexcept;

    [[nodiscard]] const std::vector<std::byte>& canonical_bytes() const noexcept;

    [[nodiscard]] const std::string& layout_id() const noexcept;

   private:
    static std::vector<std::byte> serialize(const Object& object, ChunkingStrategy chunking_strategy,
                                            const std::optional<FastCdcParameters>& fastcdc_parameters,
                                            const ObjectLayout& layout);

    static std::string hash_canonical_bytes(const std::vector<std::byte>& canonical_bytes);

    Object object_;
    ChunkingStrategy chunking_strategy_;
    std::optional<FastCdcParameters> fastcdc_parameters_;
    ObjectLayout layout_;
    std::vector<std::byte> canonical_bytes_;
    std::string layout_id_;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_OBJECT_LAYOUT_DESCRIPTOR_HPP
