#pragma once
#include <memory>
#include <sstream>
#include "sqlite3.h"
#include "guard_funcs.h"

namespace jonoondb_api {
// Forward declarations
class Status;
class DocumentCollection;
class DocumentSchema;
class ResultSetImpl;

class QueryProcessor final {
public:
  QueryProcessor(const QueryProcessor&) = delete;
  QueryProcessor(QueryProcessor&&) = delete;
  QueryProcessor& operator=(const QueryProcessor&) = delete;
  QueryProcessor();  
  Status AddCollection(const std::shared_ptr<DocumentCollection>& collection);
  ResultSetImpl ExecuteSelect(const std::string& selectStatement);

private:  
  std::unique_ptr<sqlite3, void(*)(sqlite3*)> m_db;
};
}  // jonoondb_api
