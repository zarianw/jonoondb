#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include "sqlite3.h"
#include "object_pool.h"

namespace jonoondb_api {
class Status;

class ResultSetImpl {
public:
  ResultSetImpl(ObjectPoolGuard<sqlite3>& db, const std::string& selectStmt);
  ResultSetImpl(ResultSetImpl&& other);
  ResultSetImpl& operator=(ResultSetImpl&& other);
  ResultSetImpl(const ResultSetImpl& other) = delete;
  ResultSetImpl& operator=(const ResultSetImpl& other) = delete;


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
  ObjectPoolGuard<sqlite3> m_db;  
  std::unique_ptr<sqlite3_stmt, void(*)(sqlite3_stmt*)> m_stmt;
};
} // jonoondb_api