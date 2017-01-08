#pragma once

#include <cstdint>
#include "jonoondb_api_export.h"

namespace jonoondb_api {
enum class SchemaType
    : std::int32_t {
  FLAT_BUFFERS = 1
};
extern SchemaType ToSchemaType(std::int32_t type);


enum class IndexType
    : std::int32_t {
  INVERTED_COMPRESSED_BITMAP = 1,
  VECTOR = 2,
};
JONOONDB_API_EXPORT extern IndexType ToIndexType(std::int32_t type);

enum class SqlType : std::int32_t {
  INTEGER = 1,
  DOUBLE = 2,
  STRING = 3,
  BLOB = 4,
  DB_NULL = 5
};
}  // namespace jonoondb_api
