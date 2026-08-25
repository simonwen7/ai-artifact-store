#ifndef AISTORE_METADATA_OBJECT_DESCRIPTOR_HPP
#define AISTORE_METADATA_OBJECT_DESCRIPTOR_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "aistore/metadata/object_layout.hpp"

namespace aistore::metadata {

class ObjectDescriptor {
   public:
    static constexpr std::uint32_t kFormatVersion = 1;

    explicit ObjectDescriptor(ObjectLayout layout);

    [[nodiscard]] const ObjectLayout& layout() const noexcept;

    [[nodiscard]] const std::vector<std::byte>& canonical_bytes() const noexcept;

    [[nodiscard]] const std::string& object_id() const noexcept;

   private:
    static std::vector<std::byte> serialize(const ObjectLayout& layout);

    static std::string hash_canonical_bytes(const std::vector<std::byte>& canonical_bytes);

    ObjectLayout layout_;
    std::vector<std::byte> canonical_bytes_;
    std::string object_id_;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_OBJECT_DESCRIPTOR_HPP
