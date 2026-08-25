#ifndef AISTORE_METADATA_OBJECT_HPP
#define AISTORE_METADATA_OBJECT_HPP

#include <cstdint>
#include <string>
#include <string_view>

namespace aistore::metadata {

class Object {
   public:
    Object(std::string object_id, std::uint64_t total_size);

    [[nodiscard]] const std::string& object_id() const noexcept;

    [[nodiscard]] std::uint64_t total_size() const noexcept;

   private:
    static void validate_object_id(std::string_view object_id);

    std::string object_id_;
    std::uint64_t total_size_;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_OBJECT_HPP
