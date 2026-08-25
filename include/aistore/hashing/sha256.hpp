#ifndef AISTORE_HASHING_SHA256_HPP
#define AISTORE_HASHING_SHA256_HPP

#include <array>
#include <cstddef>
#include <memory>
#include <span>

namespace aistore::hashing {

class Sha256 {
   public:
    static constexpr std::size_t kDigestSize = 32;
    using Digest = std::array<std::byte, kDigestSize>;

    Sha256();
    ~Sha256();

    Sha256(const Sha256&) = delete;
    Sha256& operator=(const Sha256&) = delete;

    Sha256(Sha256&&) noexcept;
    Sha256& operator=(Sha256&&) noexcept;

    void update(std::span<const std::byte> data);

    [[nodiscard]] Digest finalize();

   private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace aistore::hashing

#endif  // AISTORE_HASHING_SHA256_HPP
