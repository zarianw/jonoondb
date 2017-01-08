#pragma once
#include <cstdint>
#include <cstddef>
#include <string>

namespace jonoondb_api {
enum class FieldType
  : std::int8_t {
  INT8,
  INT16,
  INT32,
  INT64,
  FLOAT,
  DOUBLE,
  STRING,
  VECTOR,
  COMPLEX,
  UNION,
  BLOB
};

// TODO: Come up with better management of enum to string functionality
static const char* FieldTypeStrings[] = {
  "INT8",
  "INT16",
  "INT32",
  "INT64",
  "FLOAT",
  "DOUBLE",
  "STRING",
  "VECTOR",
  "COMPLEX",
  "UNION",
  "BLOB" };

static const char* GetFieldString(FieldType fieldType) {
  return FieldTypeStrings[static_cast<int32_t>(fieldType)];
}

class Field {
 public:
  virtual ~Field() {
  }
  virtual const std::string GetName() const = 0;
  virtual FieldType GetType() const = 0;
  virtual FieldType GetElementType() const = 0;
  virtual std::size_t GetSubFieldCount() const = 0;
  virtual void GetSubField(std::size_t index, Field*& field) const = 0;
  virtual Field* AllocateField() const = 0;
  virtual void Dispose() = 0;
};
}  // jonoondb_api
