#include <string>
#include <sstream>
#include <boost/filesystem.hpp>
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

using namespace jonoondb_api;
using namespace boost::filesystem;

struct sqlite3_api_routines;
int jonoondb_vtable_init(sqlite3 *db, char **error,
                         const sqlite3_api_routines *api);

QueryProcessor::QueryProcessor(const std::string& dbPath, const std::string& dbName) :
m_readWriteDBConnection(nullptr, GuardFuncs::SQLite3Close), m_dbConnectionPool(nullptr), m_dbName(dbName) {
  path pathObj(dbPath);

  // check if the db folder exists
  if (!boost::filesystem::exists(pathObj)) {
    std::ostringstream ss;
    ss << "Database folder " << pathObj.string() << " does not exist.";
    throw MissingDatabaseFolderException(ss.str(), __FILE__, __func__, __LINE__);
  }

  pathObj += dbName;
  pathObj += ".dat";
  m_fullDBpath = pathObj.string();

  if (!boost::filesystem::exists(pathObj)) {
    std::ostringstream ss;
    ss << "Database file " << m_fullDBpath << " does not exist.";
    throw MissingDatabaseFileException(ss.str(), __FILE__, __func__, __LINE__);
  }  

  int code = sqlite3_auto_extension((void(*)(void))jonoondb_vtable_init);
  if (code != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(code), __FILE__, __func__, __LINE__);
  }

  sqlite3* db = nullptr;
  code = sqlite3_open(m_fullDBpath.c_str(), &db);
  m_readWriteDBConnection.reset(db);
  if (code != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(code), __FILE__, __func__, __LINE__);
  } 

  //Initialize the connection pool
  m_dbConnectionPool.reset(new ObjectPool<sqlite3>(5, 10, std::bind(&QueryProcessor::OpenConnection, this), GuardFuncs::SQLite3Close));
}

const char* GetSQLiteTypeString(FieldType fieldType) {
  static std::string integer = "INTEGER";
  static std::string real = "REAL";
  static std::string text = "TEXT";
  switch (fieldType) {
    case jonoondb_api::FieldType::BASE_TYPE_UINT8:
    case jonoondb_api::FieldType::BASE_TYPE_UINT16:
    case jonoondb_api::FieldType::BASE_TYPE_UINT32:
    case jonoondb_api::FieldType::BASE_TYPE_UINT64:
    case jonoondb_api::FieldType::BASE_TYPE_INT8:
    case jonoondb_api::FieldType::BASE_TYPE_INT16:
    case jonoondb_api::FieldType::BASE_TYPE_INT32:
    case jonoondb_api::FieldType::BASE_TYPE_INT64:
      return integer.c_str();
    case jonoondb_api::FieldType::BASE_TYPE_FLOAT32:
    case jonoondb_api::FieldType::BASE_TYPE_DOUBLE:
      return real.c_str();
    case jonoondb_api::FieldType::BASE_TYPE_STRING:
      return text.c_str();
    case jonoondb_api::FieldType::BASE_TYPE_VECTOR:
    case jonoondb_api::FieldType::BASE_TYPE_COMPLEX:
    default:
    {
      std::ostringstream ss;
      ss << "Argument fieldType has a value " << static_cast<int32_t>(fieldType)
        << " which does not have a correponding sql type.";
      throw InvalidArgumentException(ss.str(), __FILE__, __func__, __LINE__);
    }
  }
}

void BuildCreateTableStatement(const Field* complexField,
  std::string& prefix,
  std::ostringstream& stringStream,
  std::vector<ColumnInfo>& columnNames) {
  assert(complexField->GetType() == FieldType::BASE_TYPE_COMPLEX);
  Field* field = complexField->AllocateField();  
  std::unique_ptr<Field, void(*)(Field*)> fieldGuard(field, GuardFuncs::DisposeField);

  for (size_t i = 0; i < complexField->GetSubFieldCount(); i++) {
    complexField->GetSubField(i, field);    

    if (field->GetType() == FieldType::BASE_TYPE_COMPLEX) {
      prefix.append(field->GetName());
      prefix.append(".");
      BuildCreateTableStatement(field, prefix, stringStream, columnNames);      
    } else {
      auto fullName = prefix;
      fullName.append(field->GetName());
      columnNames.push_back(ColumnInfo(fullName, field->GetType()));
      stringStream << "'" << fullName << "'" << " "
        << GetSQLiteTypeString(field->GetType());
      stringStream << ", ";
    }
  }
}

void GenerateCreateTableStatementForCollection(const std::shared_ptr<DocumentCollection>& collection,
  std::ostringstream& stringStream, std::vector<ColumnInfo>& columnNames) {
  Field* field = collection->GetDocumentSchema()->AllocateField();
  std::unique_ptr<Field, void(*)(Field*)> fieldGuard(field, GuardFuncs::DisposeField);

  stringStream.clear();
  stringStream << "CREATE TABLE " << collection->GetName() << " (";
  auto count = collection->GetDocumentSchema()->GetRootFieldCount();
  for (size_t i = 0; i < count; i++) {
    collection->GetDocumentSchema()->GetRootField(i, field);
    if (field->GetType() == FieldType::BASE_TYPE_COMPLEX) {
      std::string prefix;
      prefix.append(field->GetName());
      prefix.append(".");
      BuildCreateTableStatement(field, prefix, stringStream, columnNames);      
    } else {
      columnNames.push_back(ColumnInfo(field->GetName(), field->GetType()));
      stringStream << field->GetName() << " " << GetSQLiteTypeString(field->GetType());
      stringStream << ", ";
    }
  }

  if (count > 0) {
    long pos = stringStream.tellp();
    stringStream.seekp(pos - 2);
  }

  stringStream << ");";
}

void QueryProcessor::AddCollection(const std::shared_ptr<DocumentCollection>& collection) {
  std::ostringstream ss;
  auto docColInfo = std::make_shared<DocumentCollectionInfo>();
  GenerateCreateTableStatementForCollection(collection, ss, docColInfo->columnsInfo);
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
  int code = sqlite3_exec(m_readWriteDBConnection.get(), sqlStmt.str().c_str(), nullptr, nullptr, &errMsg);  
  if (code != SQLITE_OK) {
    // Remove the collection from dictionary
    DocumentCollectionDictionary::Instance()->Remove(key);
    if (errMsg != nullptr) {
      std::string sqliteErrorMsg = errMsg;
      sqlite3_free(errMsg);
      throw SQLException(sqliteErrorMsg, __FILE__, __func__, __LINE__);      
    }

    throw SQLException(sqlite3_errstr(code), __FILE__, __func__, __LINE__);
  }
}

ResultSetImpl QueryProcessor::ExecuteSelect(const std::string& selectStatement) {
  // connection comming from m_dbConnectionPool are readonly
  // so we dont have to worry about sql injection
  return ResultSetImpl(ObjectPoolGuard<sqlite3>(m_dbConnectionPool.get(), m_dbConnectionPool->Take()), selectStatement);
}

sqlite3* QueryProcessor::OpenConnection() {
  sqlite3* db = nullptr;
  int code = sqlite3_open_v2(m_fullDBpath.c_str(), &db, SQLITE_OPEN_READONLY, nullptr);
  if (code != SQLITE_OK) {
    sqlite3_close(db);
    throw SQLException(sqlite3_errstr(code), __FILE__, __func__, __LINE__);
  }

  return db;
}