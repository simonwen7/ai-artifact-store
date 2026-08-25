#ifndef AISTORE_METADATA_STORAGE_LOCATION_HPP
#define AISTORE_METADATA_STORAGE_LOCATION_HPP

#include <cstdint>
#include <string>

namespace aistore::metadata {

enum class StorageLocationState : std::uint8_t {
    Available,
    Missing,
    Corrupt,
};

struct StorageLocation {
    std::string chunk_id;
    std::string node_id;
    std::string storage_path;
    StorageLocationState state;
};

}  // namespace aistore::metadata

#endif  // AISTORE_METADATA_STORAGE_LOCATION_HPP
