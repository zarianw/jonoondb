#pragma once
#include <memory>
#include <sstream>
#include "sqlite3.h"
#include "guard_funcs.h"
#include "object_pool.h"

namespace jonoondb_api {
// Forward declarations
class Status;
class DocumentCollection;
class DocumentSchema;
class ResultSetImpl;

class QueryProcessor final {
public:
  QueryProcessor(const std::string& dbPath, const std::string& dbName);
  QueryProcessor(const QueryProcessor&) = delete;
  QueryProcessor(QueryProcessor&&) = delete;
  QueryProcessor& operator=(const QueryProcessor&) = delete;  
  Status AddCollection(const std::shared_ptr<DocumentCollection>& collection);
  ResultSetImpl ExecuteSelect(const std::string& selectStatement);

private:  
  sqlite3* OpenConnection();
  std::unique_ptr<sqlite3, void(*)(sqlite3*)> m_readWriteDBConnection;
  std::unique_ptr<ObjectPool<sqlite3>> m_dbConnectionPool;
  std::string m_fullDBpath;
};
}  // jonoondb_api
