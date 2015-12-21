#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <unordered_map>
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
  std::int64_t GetInteger(int columnIndex) const;
  double GetDouble(int columnIndex) const;
  std::string GetString(int columnIndex) const;
  int GetColumnIndex(const std::string& columnLabel) const;
private:
  ObjectPoolGuard<sqlite3> m_db;  
  std::unique_ptr<sqlite3_stmt, void(*)(sqlite3_stmt*)> m_stmt;
  std::unordered_map<std::string, int> m_columnMap;
  mutable std::string m_tmpStrStorage;
};
} // jonoondb_api