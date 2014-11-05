#pragma once

#include <cstdint>

namespace jonoondb_api {
enum class SchemaType {
  FLAT_BUFFERS = 1
};

enum class IndexType : std::int16_t {
  WAHCompressedBitmap = 1
};

} // namespace jonoondb_api
