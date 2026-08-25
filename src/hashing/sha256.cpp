#include "aistore/hashing/sha256.hpp"

#include <openssl/err.h>
#include <openssl/evp.h>

#include <array>
#include <stdexcept>
#include <string>
#include <utility>

namespace aistore::hashing {

namespace {

[[noreturn]] void throw_openssl_error(const char* operation) {
    const unsigned long error_code = ERR_get_error();

    if (error_code == 0) {
        throw std::runtime_error(std::string(operation) + " failed");
    }

    std::array<char, 256> error_buffer{};
    ERR_error_string_n(error_code, error_buffer.data(), error_buffer.size());

    throw std::runtime_error(std::string(operation) + " failed: " + error_buffer.data());
}

}  // namespace

class Sha256::Impl {
   public:
    Impl() {
        context = EVP_MD_CTX_new();

        if (context == nullptr) {
            throw_openssl_error("EVP_MD_CTX_new");
        }

        if (EVP_DigestInit_ex(context, EVP_sha256(), nullptr) != 1) {
            EVP_MD_CTX_free(context);
            context = nullptr;
            throw_openssl_error("EVP_DigestInit_ex");
        }
    }

    ~Impl() { EVP_MD_CTX_free(context); }

    Impl(const Impl&) = delete;
    Impl& operator=(const Impl&) = delete;

    EVP_MD_CTX* context = nullptr;
    bool finalized = false;
};

Sha256::Sha256() : impl_(std::make_unique<Impl>()) {}

Sha256::~Sha256() = default;

Sha256::Sha256(Sha256&&) noexcept = default;

Sha256& Sha256::operator=(Sha256&&) noexcept = default;

void Sha256::update(std::span<const std::byte> data) {
    if (impl_->finalized) {
        throw std::logic_error("cannot update a finalized SHA-256 context");
    }

    if (data.empty()) {
        return;
    }

    if (EVP_DigestUpdate(impl_->context, data.data(), data.size()) != 1) {
        throw_openssl_error("EVP_DigestUpdate");
    }
}

Sha256::Digest Sha256::finalize() {
    if (impl_->finalized) {
        throw std::logic_error("SHA-256 context has already been finalized");
    }

    Digest digest{};
    unsigned int digest_size = 0;

    if (EVP_DigestFinal_ex(impl_->context, reinterpret_cast<unsigned char*>(digest.data()), &digest_size) != 1) {
        throw_openssl_error("EVP_DigestFinal_ex");
    }

    if (digest_size != digest.size()) {
        throw std::runtime_error("unexpected SHA-256 digest size");
    }

    impl_->finalized = true;

    return digest;
}

}  // namespace aistore::hashing
