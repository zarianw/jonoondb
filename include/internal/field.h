#pragma once
#include <cstdint>
#include <cstddef>
#include <string>

namespace jonoondb_api {
// Forward Declarations
enum class FieldType
: std::int8_t;
class Field {
 public:
  virtual ~Field() {
  }
  virtual const std::string& GetName() const = 0;
  virtual FieldType GetType() const = 0;
  virtual std::size_t GetSubFieldCount() const = 0;
  virtual void GetSubField(std::size_t index, Field*& field) const = 0;
  virtual Field* AllocateField() const = 0;
  virtual void Dispose() = 0;
};
}  // jonoondb_api
