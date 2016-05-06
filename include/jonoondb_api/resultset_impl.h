#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <map>
#include <boost/utility/string_ref.hpp>
#include "sqlite3.h"
#include "object_pool.h"
#include "enums.h"

namespace jonoondb_api {
class ResultSetImpl {
public:
  ResultSetImpl(ObjectPoolGuard<sqlite3> db, const std::string& selectStmt);
  ResultSetImpl(ResultSetImpl&& other);
  ResultSetImpl& operator=(ResultSetImpl&& other);
  ResultSetImpl(const ResultSetImpl& other) = delete;
  ResultSetImpl& operator=(const ResultSetImpl& other) = delete;

  bool Next();
  std::int64_t GetInteger(std::int32_t columnIndex) const;
  double GetDouble(std::int32_t columnIndex) const;
  const std::string& GetString(std::int32_t columnIndex) const;
  std::int32_t GetColumnIndex(const boost::string_ref& columnLabel) const;
  std::int32_t GetColumnCount();
  SqlType GetColumnType(std::int32_t columnIndex);
  const std::string& GetColumnLabel(std::int32_t columnIndex);
  bool IsNull(std::int32_t columnIndex);
private:
  ObjectPoolGuard<sqlite3> m_db;  
  std::unique_ptr<sqlite3_stmt, void(*)(sqlite3_stmt*)> m_stmt;
  std::map<boost::string_ref, int> m_columnMap;
  std::vector<std::string> m_columnMapStringStore;
  std::vector<int> m_columnSqlType;
  mutable std::string m_tmpStrStorage;
  bool m_resultSetConsumed = false;
};
} // jonoondb_api
