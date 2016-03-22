#pragma once

#include <cstdint>

namespace jonoondb_api {
struct BlobMetadata {
  std::int32_t fileKey;
  std::int64_t offset;
};
}  // jonoondb_api
