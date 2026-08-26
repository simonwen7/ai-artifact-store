#include "aistore/client/client_error.hpp"

#include <utility>

namespace aistore::client {

RemoteApiError::RemoteApiError(unsigned int status_code, std::string error_code, std::string response_body)
    : std::runtime_error{"remote API error"},
      status_code_{status_code},
      error_code_{std::move(error_code)},
      response_body_{std::move(response_body)} {}

unsigned int RemoteApiError::status_code() const noexcept { return status_code_; }

const std::string& RemoteApiError::error_code() const noexcept { return error_code_; }

const std::string& RemoteApiError::response_body() const noexcept { return response_body_; }

// NOLINTNEXTLINE(performance-unnecessary-value-param)
RemoteProtocolError::RemoteProtocolError(std::string message) : std::runtime_error{message} {}

}  // namespace aistore::client
