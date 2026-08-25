#ifndef AISTORE_METADATA_UUID_V7_HPP
#define AISTORE_METADATA_UUID_V7_HPP

#include <string>
#include <string_view>

namespace aistore::metadata {

class UuidV7 {
   public:
    [[nodiscard]] static UuidV7 generate();

    explicit UuidV7(std::string value);

    [[nodiscard]] const std::string& str() const noexcept;

    friend bool operator==(const UuidV7&, const UuidV7&) = default;

   private:
    static void validate(std::string_view value);

    std::string value_;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_UUID_V7_HPP
