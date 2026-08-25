#ifndef AISTORE_METADATA_OBJECT_LAYOUT_HPP
#define AISTORE_METADATA_OBJECT_LAYOUT_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace aistore::metadata {

struct ChunkRef {
    std::string chunk_id;
    std::uint64_t offset;
    std::uint64_t size;
};

class ObjectLayout {
   public:
    explicit ObjectLayout(std::vector<ChunkRef> chunks);

    [[nodiscard]] const std::vector<ChunkRef>& chunks() const noexcept;

    [[nodiscard]] std::uint64_t total_size() const noexcept;

    [[nodiscard]] bool empty() const noexcept;

   private:
    static void validate(const std::vector<ChunkRef>& chunks);

    std::vector<ChunkRef> chunks_;
    std::uint64_t total_size_ = 0;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_OBJECT_LAYOUT_HPP
