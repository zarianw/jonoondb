#pragma once

#include <cstdint>
#include <string>

namespace jonoondb_api {
// Forward declaration
enum class FieldType : std::int8_t;
enum class SchemaType : std::int32_t;
class Field;

class DocumentSchema {
 public:
  virtual ~DocumentSchema() {}
  virtual const std::string& GetSchemaText() const = 0;
  virtual SchemaType GetSchemaType() const = 0;
  virtual FieldType GetFieldType(const std::string& fieldName) const = 0;
  virtual std::size_t GetRootFieldCount() const = 0;
  virtual void GetRootField(size_t index, Field*& field) const = 0;
  virtual Field* AllocateField() const = 0;
};
}  // namespace jonoondb_api
