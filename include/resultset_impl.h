#pragma once

#include <cstddef>
#include <cstdint>
#include "sqlite3.h"

namespace jonoondb_api {
class Status;

class ResultSetImpl {
public:
  bool Next();
  std::int8_t GetInt8(int columnIndex) const;
  std::int16_t GetInt16(int columnIndex) const;
  std::int32_t GetInt32(int columnIndex) const;
  std::int64_t GetInt64(int columnIndex) const;
  float GetFloat(int columnIndex) const;
  double GetDouble(int columnIndex) const;
  std::string GetStringValue(int columnIndex) const;
  int GetColumnIndex(const std::string& columnLabel) const;
private:
  sqlite3* m_db;
  sqlite3* m_stmt;
};
} // jonoondb_api