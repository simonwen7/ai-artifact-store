#ifndef AISTORE_METADATA_MANIFEST_MODEL_HPP
#define AISTORE_METADATA_MANIFEST_MODEL_HPP

#include <cstddef>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace aistore::metadata {

class Manifest {
   public:
    using Entries = std::map<std::string, std::string>;

    static constexpr std::uint32_t kFormatVersion = 1;

    explicit Manifest(Entries entries);

    [[nodiscard]]
    const std::string& manifest_id() const noexcept;

    [[nodiscard]]
    const Entries& entries() const noexcept;

    [[nodiscard]]
    const std::vector<std::byte>& canonical_bytes() const noexcept;

   private:
    static void validate_entries(const Entries& entries);

    static std::vector<std::byte> serialize(const Entries& entries);

    static std::string hash_canonical_bytes(const std::vector<std::byte>& canonical_bytes);

    Entries entries_;
    std::vector<std::byte> canonical_bytes_;
    std::string manifest_id_;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_MANIFEST_MODEL_HPP
