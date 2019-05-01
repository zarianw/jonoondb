#include <string>
#include <sstream>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include "sqlite3.h"
#include "query_processor.h"
#include "exception_utils.h"
#include "document_collection.h"
#include "document_collection_dictionary.h"
#include "document_schema.h"
#include "field.h"
#include "enums.h"
#include "guard_funcs.h"
#include "resultset_impl.h"
#include "jonoondb_exceptions.h"
#include "path_utils.h"
#include "string_utils.h"
#include "jonoondb_api/sqlite_utils.h"

using namespace jonoondb_api;
using namespace boost::filesystem;
using namespace std;

struct sqlite3_api_routines;
int jonoondb_vtable_init(sqlite3* db, char** error,
                         const sqlite3_api_routines* api);

static bool IsMasterTableOperation(int ac, const char* param1) {
  if (ac == SQLITE_UPDATE && (param1 && strcmp(param1, "sqlite_master") == 0)) {
    return true;
  }

  return false;
}

static int jonoondb_delete_auth_callback(void* /*userData*/, int actionCode,
                                         const char* param1, const char* /*param2*/,
                                         const char* /*dbName*/, const char* /*triggerName*/) {
  if (actionCode == SQLITE_DELETE || actionCode == SQLITE_READ || actionCode == SQLITE_SELECT) {
    return SQLITE_OK;
  } else if (IsMasterTableOperation(actionCode, param1)) {
    return SQLITE_OK;
  }
  return SQLITE_DENY;
}

QueryProcessor::QueryProcessor(const std::string& dbPath,
                               const std::string& dbName) :
    m_readWriteDBConnection(nullptr, GuardFuncs::SQLite3Close),
    m_deleteStmtConnection(nullptr, GuardFuncs::SQLite3Close),
    m_dbConnectionPool(nullptr), m_dbName(dbName) {
  std::string normalizedDBPath = PathUtils::NormalizePath(dbPath);
  normalizedDBPath += dbName;
  boost::replace_all(normalizedDBPath, "/", "_");
  boost::replace_all(normalizedDBPath, ":", "_");
  boost::replace_all(normalizedDBPath, ".", "_");

  m_dbConnStr = "file::";
  m_dbConnStr += normalizedDBPath;
  m_dbConnStr += "?mode=memory&cache=shared";

  int code = sqlite3_auto_extension((void (*)(void)) jonoondb_vtable_init);
  SQLiteUtils::HandleSQLiteCode(code);

  sqlite3* db = nullptr;
  code = sqlite3_open_v2(
      m_dbConnStr.c_str(), &db,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, nullptr);
  m_readWriteDBConnection.reset(db);
  SQLiteUtils::HandleSQLiteCode(code);

  db = nullptr;
  code = sqlite3_open_v2(
      m_dbConnStr.c_str(), &db,
      SQLITE_OPEN_READWRITE | SQLITE_OPEN_URI, nullptr);
  m_deleteStmtConnection.reset(db);
  SQLiteUtils::HandleSQLiteCode(code);
  code = sqlite3_set_authorizer(db, jonoondb_delete_auth_callback, nullptr);
  SQLiteUtils::HandleSQLiteCode(code);

  //Initialize the connection pool
  m_dbConnectionPool.reset(new ObjectPool<sqlite3>(5,
                                                   10,
                                                   std::bind(&QueryProcessor::OpenConnection,
                                                             this),
                                                   GuardFuncs::SQLite3Close));
}

const char* GetSQLiteTypeString(FieldType fieldType) {
  static std::string integer = "INTEGER";
  static std::string real = "REAL";
  static std::string text = "TEXT";
  static std::string blob = "BLOB";
  switch (fieldType) {
    case jonoondb_api::FieldType::INT8:
    case jonoondb_api::FieldType::INT16:
    case jonoondb_api::FieldType::INT32:
    case jonoondb_api::FieldType::INT64:
      return integer.c_str();
    case jonoondb_api::FieldType::FLOAT:
    case jonoondb_api::FieldType::DOUBLE:
      return real.c_str();
    case jonoondb_api::FieldType::STRING:
      return text.c_str();
    case jonoondb_api::FieldType::BLOB:
      return blob.c_str();
    case jonoondb_api::FieldType::VECTOR:
    case jonoondb_api::FieldType::COMPLEX:
    default: {
      std::ostringstream ss;
      ss << "Argument fieldType has a value " << static_cast<int32_t>(fieldType)
          << " which does not have a correponding sql type.";
      throw InvalidArgumentException(ss.str(), __FILE__, __func__, __LINE__);
    }
  }
}

void BuildCreateTableStatement(const Field* complexField,
                               std::list<std::string>& prefixes,
                               std::ostringstream& stringStream,
                               std::vector<ColumnInfo>& columnNames) {
  assert(complexField->GetType() == FieldType::COMPLEX);
  Field* field = complexField->AllocateField();
  std::unique_ptr<Field, void (*)(Field*)>
      fieldGuard(field, GuardFuncs::DisposeField);

  for (size_t i = 0; i < complexField->GetSubFieldCount(); i++) {
    complexField->GetSubField(i, field);

    if (field->GetType() == FieldType::COMPLEX) {
      prefixes.push_back(field->GetName());
      BuildCreateTableStatement(field, prefixes, stringStream, columnNames);
    } else if (field->GetType() == FieldType::UNION ||
               field->GetType() == FieldType::VECTOR) {
      // We don't support these types yet for querying
      continue;
    } else {
      std::string fullName;
      for (auto& prefix : prefixes) {
        fullName.append(prefix).append(".");
      }     
      fullName.append(field->GetName());
      columnNames.push_back(ColumnInfo(fullName, field->GetType(),
                                       StringUtils::Split(fullName, ".")));
      stringStream << "'" << fullName << "'" << " "
          << GetSQLiteTypeString(field->GetType());
      stringStream << ", ";
    }
  }

  if (!prefixes.empty()) {
    prefixes.pop_back();
  }
}

void GenerateCreateTableStatementForCollection(
    const std::shared_ptr<DocumentCollection>& collection,
    std::ostringstream& stringStream,
    std::vector<ColumnInfo>& columnNames) {
  Field* field = collection->GetDocumentSchema()->AllocateField();
  std::unique_ptr<Field, void (*)(Field*)>
      fieldGuard(field, GuardFuncs::DisposeField);

  stringStream.clear();
  stringStream << "CREATE TABLE " << collection->GetName() << " (";
  auto count = collection->GetDocumentSchema()->GetRootFieldCount();
  for (size_t i = 0; i < count; i++) {
    collection->GetDocumentSchema()->GetRootField(i, field);
    if (field->GetType() == FieldType::COMPLEX) {
      std::list<std::string> prefixes;
      prefixes.push_back(field->GetName());      
      BuildCreateTableStatement(field, prefixes, stringStream, columnNames);
    } else if (field->GetType() == FieldType::UNION ||
               field->GetType() == FieldType::VECTOR) {
      // We don't support these types yet for querying
      continue;
    } else {
      columnNames.push_back(ColumnInfo(field->GetName(), field->GetType(),
                                       StringUtils::Split(field->GetName(),
                                                          ".")));
      stringStream << field->GetName() << " "
          << GetSQLiteTypeString(field->GetType());
      stringStream << ", ";
    }
  }

  // Add hidden columns
  stringStream << "_document BLOB HIDDEN);";  
}

void QueryProcessor::AddCollection(const std::shared_ptr<DocumentCollection>& collection) {
  std::ostringstream ss;
  auto docColInfo = std::make_shared<DocumentCollectionInfo>();
  GenerateCreateTableStatementForCollection(collection,
                                            ss,
                                            docColInfo->columnsInfo);
  docColInfo->createVTableStmt = ss.str();
  docColInfo->collection = collection;

  // Generate key and insert the collection in a singleton dictionary.
  // vtable will use this key to get the collectionInfo  
  std::string key("'");
  key.append(m_dbName).append(">").append(collection->GetName()).append("'");
  DocumentCollectionDictionary::Instance()->Insert(key, docColInfo);

  std::ostringstream sqlStmt;
  sqlStmt << "CREATE VIRTUAL TABLE " << collection->GetName()
      << " USING jonoondb_vtable(" << key << ")";

  char* errMsg;
  int code = sqlite3_exec(m_readWriteDBConnection.get(),
                          sqlStmt.str().c_str(),
                          nullptr,
                          nullptr,
                          &errMsg);
  if (code != SQLITE_OK) {
    // DocumentCollectionDictionary should have the collection after successful addition.
    // This is required when jonoondb_connect is called instead on jonoondb_create.
    // However if we fail to add the collection then it should be removed.    
    DocumentCollectionDictionary::Instance()->Remove(key);
    SQLiteUtils::HandleSQLiteCode(code, errMsg);
  }
}

void jonoondb_api::QueryProcessor::RemoveCollection(const std::string& collectionName) {
  std::string stmt = "DROP TABLE IF EXISTS ";
  stmt.append(collectionName).append(";");
  char* errMsg;
  int code = sqlite3_exec(m_readWriteDBConnection.get(),
                          stmt.c_str(),
                          nullptr,
                          nullptr,
                          &errMsg);
  SQLiteUtils::HandleSQLiteCode(code, errMsg);

  std::string key("'");
  key.append(m_dbName).append(">").append(collectionName).append("'");
  DocumentCollectionDictionary::Instance()->Remove(key);
}

ResultSetImpl QueryProcessor::ExecuteSelect(const std::string& selectStatement) {
  // connection comming from m_dbConnectionPool are readonly
  // so we dont have to worry about sql injection
  return ResultSetImpl(ObjectPoolGuard<sqlite3>(m_dbConnectionPool.get(),
                                                m_dbConnectionPool->Take()),
                       selectStatement);
}

std::int64_t QueryProcessor::Delete(const std::string& deleteStatement) {
  char* errMsg;
  int code = sqlite3_exec(m_deleteStmtConnection.get(),
                          deleteStatement.c_str(),
                          nullptr,
                          nullptr,
                          &errMsg);
  if (code == SQLITE_DENY || code == SQLITE_AUTH) {
    ostringstream ss;
    ss << "Only delete and select statement is allowed on delete API.";
    if (errMsg) {
      ss << " Additional info: ";
      ss << errMsg;
    }
    throw ApiMisuseException(ss.str(),
                             __FILE__, __func__, __LINE__);
  }
  SQLiteUtils::HandleSQLiteCode(code, errMsg);

  return sqlite3_changes(m_deleteStmtConnection.get());
}

sqlite3* QueryProcessor::OpenConnection() {
  sqlite3* db = nullptr;
  int code =
      sqlite3_open_v2(m_dbConnStr.c_str(), &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, nullptr);
  if (code != SQLITE_OK) {
    sqlite3_close(db);
    throw SQLException(sqlite3_errstr(code), __FILE__, __func__, __LINE__);
  }

  return db;
}