#include "resultset_impl.h"
#include "guard_funcs.h"
#include "jonoondb_exceptions.h"
#include <sstream>

using namespace jonoondb_api;

ResultSetImpl::ResultSetImpl(ObjectPoolGuard<sqlite3> db, const std::string& selectStmt) :
m_db(std::move(db)), m_stmt(nullptr, GuardFuncs::SQLite3Finalize) {
  sqlite3_stmt* stmt = nullptr;
  int code = sqlite3_prepare_v2(m_db, selectStmt.c_str(), selectStmt.size(), &stmt, nullptr);
  m_stmt.reset(stmt);
  if (code != SQLITE_OK) {
    // We can safely use sqlite3_errmsg because each ResultSetImpl
    // has a dedicated sqlite3 connection, so it will only be used 
    // by one thread at any given time.
    const char* errMsg = sqlite3_errmsg(m_db);
    if (errMsg != nullptr) {
      std::string sqliteErrorMsg = errMsg;
      throw SQLException(sqliteErrorMsg, __FILE__, __func__, __LINE__);
    }

    throw SQLException(sqlite3_errstr(code), __FILE__, __func__, __LINE__);
  }

  int colCount = sqlite3_column_count(m_stmt.get());
  for (size_t i = 0; i < colCount; i++) {
    const char* colName = sqlite3_column_name(m_stmt.get(), i);
    if (colName == nullptr) {
      throw SQLException("Failed to get column names for the resultset.",
        __FILE__, __func__, __LINE__);
    }
    // Todo: maybe we need to handle UTF8 strings here     
    m_columnMapStringStore.push_back(colName); 
    
    m_columnSqlType.push_back(sqlite3_column_type(m_stmt.get(), i));
  }

  for (size_t i = 0; i < m_columnMapStringStore.size(); i++) {
    m_columnMap[m_columnMapStringStore[i]] = i;
  }
}

// Todo: Use more efficient move sematics (e.g. pimpl idom)
ResultSetImpl::ResultSetImpl(ResultSetImpl&& other) : m_stmt(nullptr, GuardFuncs::SQLite3Finalize) {
  this->m_db = std::move(other.m_db);
  this->m_stmt = std::move(other.m_stmt); 
  this->m_columnMapStringStore = std::move(other.m_columnMapStringStore);
  this->m_columnMap = std::move(other.m_columnMap);
  this->m_tmpStrStorage = std::move(other.m_tmpStrStorage);
}

ResultSetImpl& ResultSetImpl::operator=(ResultSetImpl&& other) {
  if (this != &other) {
    this->m_db = std::move(other.m_db);
    this->m_stmt = std::move(other.m_stmt);
    this->m_columnMapStringStore = std::move(other.m_columnMapStringStore);
    this->m_columnMap = std::move(other.m_columnMap);
    this->m_tmpStrStorage = std::move(other.m_tmpStrStorage);
  }

  return *this;
}

bool ResultSetImpl::Next() {
  int code = sqlite3_step(m_stmt.get());
  if (code == SQLITE_ROW) {
    return true;
  } else if (code == SQLITE_DONE) {
    return false;
  } else {
    throw SQLException(sqlite3_errstr(code), __FILE__, __func__, __LINE__);
  }
}

std::int64_t ResultSetImpl::GetInteger(std::int32_t columnIndex) const {
  return sqlite3_column_int64(m_stmt.get(), columnIndex);
}

double ResultSetImpl::GetDouble(std::int32_t columnIndex) const {
  return sqlite3_column_double(m_stmt.get(), columnIndex);
}

const std::string& ResultSetImpl::GetString(std::int32_t columnIndex) const {
  auto val = sqlite3_column_text(m_stmt.get(), columnIndex);
  if (val == nullptr) {
    std::ostringstream ss;
    ss << "Failed to allocate memory for string for column at index " << columnIndex << ".";
    throw OutOfMemoryException(ss.str(), __FILE__, __func__, __LINE__);
  }
  auto size = sqlite3_column_bytes(m_stmt.get(), columnIndex);
  m_tmpStrStorage = std::string(reinterpret_cast<const char*>(val), size);
  return m_tmpStrStorage;
}

std::int32_t ResultSetImpl::GetColumnIndex(const boost::string_ref& columnLabel) const {
  auto iter = m_columnMap.find(columnLabel);
  if (iter == m_columnMap.end()) {
    std::ostringstream ss;
    ss << "Unable to find column index for column label '" << columnLabel << "' in the resultset.";
    throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }

  return iter->second;  
}

std::int32_t ResultSetImpl::GetColumnCount() {
  return m_columnMapStringStore.size();
}

SqlType ResultSetImpl::GetColumnType(std::int32_t columnIndex) {
  auto type = m_columnSqlType[columnIndex];
  switch (type) {
    case SQLITE_INTEGER:
      return SqlType::INTEGER;
    case SQLITE_FLOAT:
      return SqlType::DOUBLE;
    case SQLITE_TEXT:
      return SqlType::TEXT;
    default: {
      std::ostringstream ss;
      ss << "Unknown/Unsupported type " << type 
        << " encountered for columnIndex " << columnIndex << ".";
      throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
    }
  }
}

const std::string & ResultSetImpl::GetColumnLabel(std::int32_t columnIndex) {
  return m_columnMapStringStore[columnIndex];
}
