#pragma once

#include <string>
#include <cstdint>
#include "buffer_impl.h"

namespace jonoondb_api {
enum class IndexConstraintOperator
  : std::int8_t {
  EQUAL,
  LESS_THAN,
  LESS_THAN_EQUAL,
  GREATER_THAN,
  GREATER_THAN_EQUAL,
  MATCH,
  LIKE,
  GLOB,
  REGEX
};

// Forward declarations
enum class FieldType: std::int8_t;

union Operand {
  std::int64_t int64Val;
  double doubleVal;
};

enum class OperandType : std::int32_t {
  INTEGER,
  DOUBLE,
  STRING,
  BLOB
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
  BufferImpl blobVal;
};
} // namespace jonoondb_api