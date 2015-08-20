#include "query_processor.h"
#include "status.h"
#include "exception_utils.h"
#include "document_schema.h"

using namespace jonoondb_api;

struct sqlite3_api_routines;
int jonoondb_vtable_init(sqlite3 *db, char **error,
                         const sqlite3_api_routines *api);

Status QueryProcessor::Construct(QueryProcessor*& obj) {
  int code = sqlite3_auto_extension((void (*)(void))jonoondb_vtable_init);if (code != SQLITE_OK) {
    return ExceptionUtils::GetSQLiteErrorStatusFromSQLiteErrorCode(code);
  }

  sqlite3* db = nullptr;
  code = sqlite3_open(":memory:", &db);
  if (code != SQLITE_OK) {
    return ExceptionUtils::GetSQLiteErrorStatusFromSQLiteErrorCode(code);
  }

  obj = new QueryProcessor(db);

  return Status();
}

Status QueryProcessor::AddCollection(const DocumentSchema* documentSchema) {
  return Status();
}

QueryProcessor::QueryProcessor(sqlite3* db)
    : m_db(db) {
}

