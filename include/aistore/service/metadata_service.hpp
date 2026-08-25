#ifndef AISTORE_SERVICE_METADATA_SERVICE_HPP
#define AISTORE_SERVICE_METADATA_SERVICE_HPP

#include <mutex>

#include "aistore/http/http_server.hpp"
#include "aistore/metadata/postgres_metadata_repository.hpp"

namespace aistore::service {

class MetadataService {
   public:
    explicit MetadataService(metadata::PostgresMetadataRepository& repository);

    [[nodiscard]] aistore::http::HttpResponse handle_request(const aistore::http::HttpRequest& request) const;

   private:
    metadata::PostgresMetadataRepository& repository_;
    mutable std::mutex repository_mutex_;
};

}  // namespace aistore::service

#endif  // AISTORE_SERVICE_METADATA_SERVICE_HPP
