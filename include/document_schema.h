#pragma once

#include <cstdint>

namespace jonoondb_api {
// Forward declaration
class Status;
enum class ColumnType : std::int16_t;

class DocumentSchema {
public:
  virtual const char* GetSchemaText() = 0;
  virtual Status GetColumnType(const char* columnName, ColumnType& columnType) = 0;
};
}