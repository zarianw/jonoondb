#pragma once
#include <memory>
#include "sqlite3.h"

namespace jonoondb_api {
// Forward declarations
class Status;
class DocumentSchema;

class QueryProcessor final {
public:  
  Status Construct(QueryProcessor*& obj);
  QueryProcessor(const QueryProcessor&) = delete;
  QueryProcessor(QueryProcessor&&) = delete;
  QueryProcessor& operator=(const QueryProcessor&) = delete;
  Status AddCollection(const DocumentSchema* documentSchema);

private:
  QueryProcessor(sqlite3* db);
  sqlite3* m_db = nullptr;
};

} // jonoondb_api