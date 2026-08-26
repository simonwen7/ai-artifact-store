#ifndef AISTORE_CLIENT_CLIENT_ERROR_HPP
#define AISTORE_CLIENT_CLIENT_ERROR_HPP

#include <stdexcept>
#include <string>

namespace aistore::client {

class RemoteApiError : public std::runtime_error {
   public:
    RemoteApiError(unsigned int status_code, std::string error_code, std::string response_body);

    [[nodiscard]] unsigned int status_code() const noexcept;

    [[nodiscard]] const std::string& error_code() const noexcept;

    [[nodiscard]] const std::string& response_body() const noexcept;

   private:
    unsigned int status_code_;
    std::string error_code_;
    std::string response_body_;
};

class RemoteProtocolError : public std::runtime_error {
   public:
    explicit RemoteProtocolError(std::string message);
};

}  // namespace aistore::client

#endif  // AISTORE_CLIENT_CLIENT_ERROR_HPP
