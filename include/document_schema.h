#pragma once

#include <cstdint>

namespace jonoondb_api {
// Forward declaration
class Status;
enum class ColumnType : std::int32_t;

class DocumentSchema {
public:  
  virtual ~DocumentSchema() {}
  virtual const char* GetSchemaText() const = 0;
  virtual Status GetColumnType(const char* columnName, ColumnType& columnType) const = 0;
};
}