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
#include "resultset_impl.h"
#include "jonoondb_exceptions.h"

using namespace std;
using namespace jonoondb_api;

struct sqlite3_api_routines;
int jonoondb_vtable_init(sqlite3 *db, char **error,
                         const sqlite3_api_routines *api);

QueryProcessor::QueryProcessor() : m_db(nullptr, GuardFuncs::SQLite3Close) {
  int code = sqlite3_auto_extension((void(*)(void))jonoondb_vtable_init);
  if (code != SQLITE_OK) {    
    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  }

  sqlite3* db = nullptr;
  code = sqlite3_open(":memory:", &db);  
  std::unique_ptr<sqlite3, void(*)(sqlite3*)> dbPtr(db, GuardFuncs::SQLite3Close);
  if (code != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  }

  m_db = move(dbPtr);
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

ResultSetImpl QueryProcessor::ExecuteSelect(const std::string& selectStatement) {
  char* errMsg;
  int code = sqlite3_exec(m_db.get(), selectStatement.c_str(), nullptr, nullptr, &errMsg);
  if (code != SQLITE_OK) {
    if (errMsg != nullptr) {
      string sqliteErrorMsg = errMsg;
      sqlite3_free(errMsg);
      throw SQLException(sqliteErrorMsg, __FILE__, "", __LINE__);
    }

    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  }

  return ResultSetImpl();
}