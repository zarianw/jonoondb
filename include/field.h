#pragma once
#include <cstdint>

namespace jonoondb_api {
// Forward Declarations
enum class FieldType : std::int32_t;
class Status;
class Field {
public:
  virtual const char* GetName() const = 0;
  virtual FieldType GetType() const = 0;
  virtual std::size_t GetSubFieldCount() const = 0;
  virtual Status GetSubField(size_t index, Field*& field) const = 0;
  virtual Status AllocateField(Field*& field) const = 0;
  virtual void Dispose() = 0;
};
} // jonoondb_api