#ifndef AISTORE_SERVICE_METADATA_SERVICE_M8_ROUTES_HPP
#define AISTORE_SERVICE_METADATA_SERVICE_M8_ROUTES_HPP

#include <mutex>
#include <optional>

#include "aistore/http/http_server.hpp"
#include "aistore/metadata/postgres_metadata_repository.hpp"

namespace aistore::service {

[[nodiscard]] std::optional<aistore::http::HttpResponse> try_handle_m8_routes(
    const aistore::http::HttpRequest& request, metadata::PostgresMetadataRepository& repository,
    std::mutex& repository_mutex);

}  // namespace aistore::service

#endif  // AISTORE_SERVICE_METADATA_SERVICE_M8_ROUTES_HPP
