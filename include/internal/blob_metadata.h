#pragma once

#include <cstdint>

namespace jonoondb_api
{
  struct BlobMetadata
  {
    int32_t fileKey;
    int64_t offset;
  };
} // jonoondb_api