#ifndef AISTORE_SERVICE_METADATA_SERVICE_HPP
#define AISTORE_SERVICE_METADATA_SERVICE_HPP

#include "aistore/http/http_server.hpp"

namespace aistore::service {

class MetadataService {
   public:
    [[nodiscard]] aistore::http::HttpResponse handle_request(const aistore::http::HttpRequest& request) const;
};

}  // namespace aistore::service

#endif  // AISTORE_SERVICE_METADATA_SERVICE_HPP
