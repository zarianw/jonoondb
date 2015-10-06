#include <string>
#include <sstream>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "sqlite3.h"
#include "query_processor.h"
#include "status.h"
#include "exception_utils.h"
#include "document_collection.h"
#include "document_collection_dictionary.h"
#include "document_schema.h"
#include "field.h"
#include "enums.h"
#include "guard_funcs.h"
#include "resultset.h"

using namespace std;
using namespace jonoondb_api;

struct sqlite3_api_routines;
int jonoondb_vtable_init(sqlite3 *db, char **error,
                         const sqlite3_api_routines *api);

Status QueryProcessor::Construct(QueryProcessor*& obj) {
  int code = sqlite3_auto_extension((void (*)(void))jonoondb_vtable_init);
  if (code != SQLITE_OK) {
    return ExceptionUtils::GetSQLiteErrorStatusFromSQLiteErrorCode(code);
  }

  sqlite3* db = nullptr;
  code = sqlite3_open(":memory:", &db);
  if (code != SQLITE_OK) {
    return ExceptionUtils::GetSQLiteErrorStatusFromSQLiteErrorCode(code);
  }

  obj = new QueryProcessor(std::unique_ptr<sqlite3, void(*)(sqlite3*)>(db, GuardFuncs::SQLite3Close));

  return Status();
}

Status QueryProcessor::AddCollection(const shared_ptr<DocumentCollection>& collection) {
  // Generate key and insert the collection in a singleton dictionary.
  // vtable will use this key to get the collection.
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  string key = "'";
  key.append(boost::uuids::to_string(uuid)).append("'");
  DocumentCollectionDictionary::Instance()->Insert(key, collection);

  ostringstream sqlStmt;
  sqlStmt << "CREATE VIRTUAL TABLE " << collection->GetName()
    << " USING jonoondb_vtable(" << key << ")";

  char* errMsg;
  int code = sqlite3_exec(m_db.get(), sqlStmt.str().c_str(), nullptr, nullptr, &errMsg);
  // Remove the collection from dictionary
  DocumentCollectionDictionary::Instance()->Remove(key);
  if (code != SQLITE_OK) {
    if (errMsg != nullptr) {
      string sqliteErrorMsg = errMsg;
      sqlite3_free(errMsg);
      return Status(kStatusSQLiteErrorCode, sqliteErrorMsg.c_str(),
        __FILE__, "", __LINE__);      
    }

    return ExceptionUtils::GetSQLiteErrorStatusFromSQLiteErrorCode(code);
  }

  return Status();
}

Status QueryProcessor::ExecuteSelect(const char* selectStatement, ResultSet*& resultSet) {
  char* errMsg;
  int code = sqlite3_exec(m_db.get(), selectStatement, nullptr, nullptr, &errMsg);
  if (code != SQLITE_OK) {
    if (errMsg != nullptr) {
      string sqliteErrorMsg = errMsg;
      sqlite3_free(errMsg);
      return Status(kStatusSQLiteErrorCode, sqliteErrorMsg.c_str(),
        __FILE__, "", __LINE__);
    }

    return ExceptionUtils::GetSQLiteErrorStatusFromSQLiteErrorCode(code);
  }

  return Status();
}

QueryProcessor::QueryProcessor(std::unique_ptr<sqlite3, void(*)(sqlite3*)> db)
    : m_db(move(db)) {
}