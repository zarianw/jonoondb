#pragma once

#include <string>
#include <cstdint>

namespace jonoondb_api {
// Forward declarations
enum class IndexConstraintOperator: std::int8_t;
enum class FieldType: std::int8_t;

union Operand {
  std::int64_t int64Val;
  double doubleVal;
};

enum class OperandType: std::int32_t {
  INTEGER,
  DOUBLE,
  STRING
};

struct Constraint {
  Constraint(const std::string& colName, IndexConstraintOperator oper)
      : columnName(colName), op(oper) {
  }

  const std::string& columnName;
  std::string strVal;
  IndexConstraintOperator op;
  Operand operand;
  OperandType operandType;
};
} // namespace jonoondb_api