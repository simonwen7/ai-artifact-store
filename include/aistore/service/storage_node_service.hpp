#ifndef AISTORE_SERVICE_STORAGE_NODE_SERVICE_HPP
#define AISTORE_SERVICE_STORAGE_NODE_SERVICE_HPP

#include "aistore/http/http_server.hpp"
#include "aistore/storage/local_chunk_store.hpp"

namespace aistore::service {

class StorageNodeService {
   public:
    explicit StorageNodeService(storage::LocalChunkStore& chunk_store);

    [[nodiscard]] aistore::http::HttpResponse handle_request(const aistore::http::HttpRequest& request) const;

   private:
    storage::LocalChunkStore& chunk_store_;
};

}  // namespace aistore::service

#endif  // AISTORE_SERVICE_STORAGE_NODE_SERVICE_HPP
