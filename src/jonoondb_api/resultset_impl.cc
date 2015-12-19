#include "resultset_impl.h"
#include "guard_funcs.h"

using namespace jonoondb_api;

ResultSetImpl::ResultSetImpl(ObjectPoolGuard<sqlite3>& db, const std::string& selectStmt) :
m_db(std::move(db)), m_stmt(nullptr, GuardFuncs::SQLite3Finalize) {
  sqlite3_stmt* stmt = nullptr;
  int code = sqlite3_prepare_v2(m_db, selectStmt.c_str(), selectStmt.size(), &stmt, nullptr);
  m_stmt.reset(stmt);
  if (code != SQLITE_OK) {    
    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  }
}

ResultSetImpl::ResultSetImpl(ResultSetImpl&& other) : m_stmt(nullptr, GuardFuncs::SQLite3Finalize) {
  if (this != &other) {
    this->m_db = std::move(other.m_db);
    this->m_stmt = std::move(other.m_stmt);
  }
}

ResultSetImpl& ResultSetImpl::operator=(ResultSetImpl&& other) {
  if (this != &other) {
    this->m_db = std::move(other.m_db);
    this->m_stmt = std::move(other.m_stmt);
  }

  return *this;
}

