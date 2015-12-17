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

QueryProcessor::QueryProcessor(const std::string& dbPath, const std::string& dbName) :
m_readWriteDBConnection(nullptr, GuardFuncs::SQLite3Close), m_dbConnectionPool(nullptr) {
  path pathObj(dbPath);

  // check if the db folder exists
  if (!boost::filesystem::exists(pathObj)) {
    std::ostringstream ss;
    ss << "Database folder " << pathObj.string() << " does not exist.";
    throw MissingDatabaseFolderException(ss.str(), __FILE__, "", __LINE__);
  }

  pathObj += dbName;
  pathObj += ".dat";
  m_fullDBpath = pathObj.string();

  if (!boost::filesystem::exists(pathObj)) {
    std::ostringstream ss;
    ss << "Database file " << m_fullDBpath << " does not exist.";
    throw MissingDatabaseFileException(ss.str(), __FILE__, "", __LINE__);
  }

  int code = sqlite3_auto_extension((void(*)(void))jonoondb_vtable_init);
  if (code != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  }

  sqlite3* db = nullptr;
  code = sqlite3_open(m_fullDBpath.c_str(), &db);
  m_readWriteDBConnection.reset(db);
  if (code != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  } 

  //Initialize the connection pool
  m_dbConnectionPool.reset(new ObjectPool<sqlite3>(5, 10, std::bind(&QueryProcessor::OpenConnection, this), GuardFuncs::SQLite3Close));
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
  int code = sqlite3_exec(m_readWriteDBConnection.get(), sqlStmt.str().c_str(), nullptr, nullptr, &errMsg);
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
  // connection comming from m_dbConnectionPool are readonly so we dont have to worry about
  // sql injection
  //ObjectPoolGuard<sqlite3> guard(m_dbConnectionPool.get(), m_dbConnectionPool->Take());
  int code = sqlite3_exec(m_readWriteDBConnection.get(), selectStatement.c_str(), nullptr, nullptr, &errMsg);
  if (code != SQLITE_OK) {
    if (errMsg != nullptr) {
      std::string sqliteErrorMsg(errMsg);
      sqlite3_free(errMsg);
      throw SQLException(sqliteErrorMsg, __FILE__, "", __LINE__);
    }

    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  }

  return ResultSetImpl();
}

sqlite3* QueryProcessor::OpenConnection() {
  sqlite3* db = nullptr;
  int code = sqlite3_open_v2(m_fullDBpath.c_str(), &db, SQLITE_OPEN_READONLY, nullptr);
  if (code != SQLITE_OK) {
    sqlite3_close(db);
    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  }

  return db;
}