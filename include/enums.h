#pragma once

#include <cstdint>

namespace jonoondb_api {
enum class SchemaType
  : std::int32_t {
    FLAT_BUFFERS = 1
};

enum class IndexType
  : std::int32_t {
    EWAHCompressedBitmap = 1
};

enum class FieldType
  : std::int32_t {
    BASE_TYPE_UINT8,
  BASE_TYPE_UINT16,
  BASE_TYPE_UINT32,
  BASE_TYPE_UINT64,
  BASE_TYPE_INT8,
  BASE_TYPE_INT16,
  BASE_TYPE_INT32,
  BASE_TYPE_INT64,
  BASE_TYPE_FLOAT32,
  BASE_TYPE_DOUBLE,
  BASE_TYPE_STRING,
  BASE_TYPE_VECTOR,
  BASE_TYPE_COMPLEX
};

// TODO: Come up with better management of enum to string functionality
static const char* FieldTypeStrings[] = { "BASE_TYPE_UINT8", "BASE_TYPE_UINT16",
    "BASE_TYPE_UINT32", "BASE_TYPE_UINT64", "BASE_TYPE_INT8", "BASE_TYPE_INT16",
    "BASE_TYPE_INT32", "BASE_TYPE_INT64", "BASE_TYPE_FLOAT32",
    "BASE_TYPE_DOUBLE", "BASE_TYPE_STRING", "BASE_TYPE_VECTOR",
    "BASE_TYPE_COMPLEX" };

static const char* GetFieldString(FieldType fieldType) {
  return FieldTypeStrings[static_cast<int32_t>(fieldType)];
}

enum class IndexConstraintOperator
  : std::int32_t {
  EQUAL,
  LESS_THAN,
  LESS_THAN_EQUAL,
  GREATER_THAN,
  GREATER_THAN_EQUAL,
  MATCH
};
}  // namespace jonoondb_api
