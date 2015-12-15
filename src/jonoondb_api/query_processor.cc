#include <string>
#include <sstream>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/filesystem.hpp>
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

using namespace jonoondb_api;
using namespace boost::filesystem;

struct sqlite3_api_routines;
int jonoondb_vtable_init(sqlite3 *db, char **error,
                         const sqlite3_api_routines *api);

class DBConnectionFactory {
  static std::string path;
};

QueryProcessor::QueryProcessor(const std::string& dbPath, const std::string& dbName) :
m_writableDBConnection(nullptr, GuardFuncs::SQLite3Close), m_dbConnectionPool(nullptr) {
  path pathObj(dbPath);

  // check if the db folder exists
  if (!boost::filesystem::exists(pathObj)) {
    std::ostringstream ss;
    ss << "Database folder " << pathObj.string() << " does not exist.";
    throw MissingDatabaseFolderException(ss.str(), __FILE__, "", __LINE__);
  }

  pathObj += dbName;
  pathObj += ".dat";
  auto fullDbPath = pathObj.string();

  if (!boost::filesystem::exists(pathObj)) {
    std::ostringstream ss;
    ss << "Database file " << fullDbPath << " does not exist.";
    throw MissingDatabaseFileException(ss.str(), __FILE__, "", __LINE__);
  }

  int code = sqlite3_auto_extension((void(*)(void))jonoondb_vtable_init);
  if (code != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  }

  sqlite3* db = nullptr;
  code = sqlite3_open(pathObj.string().c_str(), &db);
  m_writableDBConnection.reset(db);
  if (code != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  } 
}

Status QueryProcessor::AddCollection(const std::shared_ptr<DocumentCollection>& collection) {
  // Generate key and insert the collection in a singleton dictionary.
  // vtable will use this key to get the collection.
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  std::string key = "'";
  key.append(boost::uuids::to_string(uuid)).append("'");
  DocumentCollectionDictionary::Instance()->Insert(key, collection);

  std::ostringstream sqlStmt;
  sqlStmt << "CREATE VIRTUAL TABLE " << collection->GetName()
    << " USING jonoondb_vtable(" << key << ")";

  char* errMsg;
  int code = sqlite3_exec(m_writableDBConnection.get(), sqlStmt.str().c_str(), nullptr, nullptr, &errMsg);
  // Remove the collection from dictionary
  DocumentCollectionDictionary::Instance()->Remove(key);
  if (code != SQLITE_OK) {
    if (errMsg != nullptr) {
      std::string sqliteErrorMsg = errMsg;
      sqlite3_free(errMsg);
      throw SQLException(sqliteErrorMsg, __FILE__, "", __LINE__);      
    }

    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  }

  return Status();
}

ResultSetImpl QueryProcessor::ExecuteSelect(const std::string& selectStatement) {
  char* errMsg;
  int code = sqlite3_exec(m_writableDBConnection.get(), selectStatement.c_str(), nullptr, nullptr, &errMsg);
  if (code != SQLITE_OK) {
    if (errMsg != nullptr) {
      std::string sqliteErrorMsg = errMsg;
      sqlite3_free(errMsg);
      throw SQLException(sqliteErrorMsg, __FILE__, "", __LINE__);
    }

    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  }

  return ResultSetImpl();
}