#pragma once

#include <string>
#include <cstdint>

namespace jonoondb_api {
// Forward declarations
enum class IndexConstraintOperator : std::int8_t;
enum class FieldType : std::int8_t;

union Operand {
  std::int8_t int8Val;
  std::int16_t int16Val;
  std::int32_t int32Val;
  std::int64_t int64Val;
  std::uint8_t uint8Val;
  std::uint16_t uint16Val;
  std::uint32_t uint32Val;
  std::uint64_t uint64Val;
  float floatVal;
  double doubleVal; 
  std::string* strVal;
};

struct Constraint {
  Constraint(const std::string& colName, IndexConstraintOperator oper)
    : columnName(colName), op(oper) {
  }

  const std::string& columnName;
  IndexConstraintOperator op;  
  Operand operand;   
};
} // namespace jonoondb_api