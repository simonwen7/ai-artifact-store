#include "aistore/metadata/object.hpp"

#include <stdexcept>
#include <utility>

namespace aistore::metadata {

Object::Object(std::string object_id, std::uint64_t total_size)
    : object_id_(std::move(object_id)), total_size_(total_size) {
    validate_object_id(object_id_);
}

const std::string& Object::object_id() const noexcept { return object_id_; }

std::uint64_t Object::total_size() const noexcept { return total_size_; }

void Object::validate_object_id(std::string_view object_id) {
    if (object_id.size() != 64) {
        throw std::invalid_argument("object ID must contain exactly 64 hexadecimal characters");
    }

    for (const char character : object_id) {
        const bool is_digit = character >= '0' && character <= '9';

        const bool is_lower_hex = character >= 'a' && character <= 'f';

        if (!is_digit && !is_lower_hex) {
            throw std::invalid_argument("object ID must use lowercase hexadecimal characters");
        }
    }
}

}  // namespace aistore::metadata
