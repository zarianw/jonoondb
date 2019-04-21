#pragma once
#include <memory>
#include <sstream>
#include <string>
#include "sqlite3.h"
#include "guard_funcs.h"
#include "object_pool.h"

namespace jonoondb_api {
// Forward declarations
class DocumentCollection;
class DocumentSchema;
class ResultSetImpl;

class QueryProcessor final {
 public:
  QueryProcessor(const std::string& dbPath, const std::string& dbName);
  QueryProcessor(const QueryProcessor&) = delete;
  QueryProcessor(QueryProcessor&&) = delete;
  QueryProcessor& operator=(const QueryProcessor&) = delete;
  void AddCollection(const std::shared_ptr<DocumentCollection>& collection);
  void RemoveCollection(const std::string& collectionName);
  ResultSetImpl ExecuteSelect(const std::string& selectStatement);
  std::int64_t Delete(const std::string& deleteStatement);

 private:
  sqlite3* OpenConnection();
  std::unique_ptr<sqlite3, void (*)(sqlite3*)> m_readWriteDBConnection;
  std::unique_ptr<sqlite3, void (*)(sqlite3*)> m_deleteStmtConnection;
  std::unique_ptr<ObjectPool<sqlite3>> m_dbConnectionPool;
  std::string m_dbConnStr;
  std::string m_dbName;
};
}  // jonoondb_api
