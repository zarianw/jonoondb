#include <string>
#include <memory>
#include <sstream>
#include <boost/filesystem.hpp>
#include "sqlite3.h"
#include "database_metadata_manager.h"
#include "status.h"
#include "string_utils.h"
#include "exception_utils.h"
#include "sqlite_utils.h"
#include "exception_utils.h"
#include "index_info_impl.h"
#include "serializer_utils.h"
#include "enums.h"
#include "jonoondb_exceptions.h"
#include "guard_funcs.h"

using namespace std;
using namespace boost::filesystem;
using namespace jonoondb_api;

DatabaseMetadataManager::DatabaseMetadataManager(const std::string& dbPath,
                                                 const std::string& dbName,
                                                 bool createDBIfMissing) :
    m_dbPath(dbPath), m_dbName(dbName), m_metadataDBConnection(nullptr, GuardFuncs::SQLite3Close) {
  // Validate arguments
  if (StringUtils::IsNullOrEmpty(m_dbPath)) {
    throw InvalidArgumentException("Argument dbPath is null or empty.",
      __FILE__, "", __LINE__);
  }

  if (StringUtils::IsNullOrEmpty(m_dbName)) {
    throw InvalidArgumentException("Argument dbName is null or empty.",
      __FILE__, "", __LINE__);
  }  
  
  Initialize(createDBIfMissing);  
}

DatabaseMetadataManager::~DatabaseMetadataManager() {
  FinalizeStatements();
}

void DatabaseMetadataManager::Initialize(bool createDBIfMissing) {
  path pathObj(m_dbPath);

  // check if the db folder exists
  if (!boost::filesystem::exists(pathObj)) {
    std::ostringstream ss;
    ss << "Database folder " << pathObj.string() << " does not exist.";
    throw MissingDatabaseFolderException(ss.str(), __FILE__, "", __LINE__);
  }

  pathObj += m_dbName;
  pathObj += ".dat";
  m_fullDbPath = pathObj.string();

  if (!boost::filesystem::exists(pathObj) && !createDBIfMissing) {
    std::ostringstream ss;
    ss << "Database file " << m_fullDbPath << " does not exist.";    
    throw MissingDatabaseFileException(ss.str(), __FILE__, "", __LINE__);
  }

  sqlite3* db;
  int sqliteCode = sqlite3_open(pathObj.string().c_str(), &db);
  m_metadataDBConnection.reset(db);
  if (sqliteCode != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, "", __LINE__);
  }
   
  CreateTables();
  PrepareStatements();
}

void DatabaseMetadataManager::CreateTables() {
  // Set DB Pragmas
  int sqliteCode = 0;
  sqliteCode = sqlite3_exec(m_metadataDBConnection.get(),
                            "PRAGMA synchronous = FULL;", 0, 0, 0);
  if (sqliteCode != SQLITE_OK) {
    std::string msg = sqlite3_errstr(sqliteCode);
    throw SQLException(msg, __FILE__, "", __LINE__);
  }

  sqliteCode = sqlite3_exec(m_metadataDBConnection.get(),
                            "PRAGMA journal_mode = WAL;", 0, 0, 0);
  if (sqliteCode != SQLITE_OK) {
    std::string msg = sqlite3_errstr(sqliteCode);
    throw SQLException(msg, __FILE__, "", __LINE__);
  }

  sqliteCode = sqlite3_busy_handler(m_metadataDBConnection.get(),
                                    SQLiteUtils::SQLiteGenericBusyHandler,
                                    nullptr);
  if (sqliteCode != SQLITE_OK) {
    std::string msg = sqlite3_errstr(sqliteCode);
    throw SQLException(msg, __FILE__, "", __LINE__);
  }

  // Create the necessary tables if they do not exist
  string sql = "create table if not exists CollectionSchema("
    "CollectionName text primary key, CollectionSchema text, "
    "CollectionSchemaType int)";
  sqliteCode = sqlite3_exec(m_metadataDBConnection.get(), sql.c_str(), NULL, NULL, NULL);
  if (sqliteCode != SQLITE_OK) {
    std::string msg = sqlite3_errstr(sqliteCode);
    throw SQLException(msg, __FILE__, "", __LINE__);
  }  

  sql = "create table if not exists CollectionIndex("
    "IndexName text primary key, CollectionName text, IndexInfo blob)";
  sqliteCode = sqlite3_exec(m_metadataDBConnection.get(), sql.c_str(), NULL, NULL, NULL);
  if (sqliteCode != SQLITE_OK) {
    std::string msg = sqlite3_errstr(sqliteCode);
    throw SQLException(msg, __FILE__, "", __LINE__);
  }

  //sql = "create table if not exists CollectionDocumentFile(FileKey int primary key, FileName text, FileDataLength int, foreign key(CollectionName) references CollectionMetadata(CollectionName))";
  sql = "create table if not exists CollectionDocumentFile("
    "FileKey int primary key, FileName text, "
    "FileDataLength int, CollectionName text)";
  sqliteCode = sqlite3_exec(m_metadataDBConnection.get(), sql.c_str(), NULL, NULL, NULL);
  if (sqliteCode != SQLITE_OK) {
    std::string msg = sqlite3_errstr(sqliteCode);
    throw SQLException(msg, __FILE__, "", __LINE__);
  }
}

void DatabaseMetadataManager::PrepareStatements() {
  int sqliteCode =
      sqlite3_prepare_v2(
          m_metadataDBConnection.get(),
          "insert into CollectionIndex (IndexName, CollectionName, IndexInfo) values (?, ?, ?)",  // stmt
          -1,  // If greater than zero, then stmt is read up to the first null terminator
          &m_insertCollectionIndexStmt,  //Statement that is to be prepared
          0  // Pointer to unused portion of stmt
          );

  if (sqliteCode != SQLITE_OK) {
    std::string msg = sqlite3_errstr(sqliteCode);
    throw SQLException(msg, __FILE__, "", __LINE__);
  }

  sqliteCode =
      sqlite3_prepare_v2(
          m_metadataDBConnection.get(),
          "insert into CollectionSchema (CollectionName, CollectionSchema, CollectionSchemaType) values (?, ?, ?)",  // stmt
          -1,  // If greater than zero, then stmt is read up to the first null terminator
          &m_insertCollectionSchemaStmt,  //Statement that is to be prepared
          0  // Pointer to unused portion of stmt
          );

  if (sqliteCode != SQLITE_OK) {
    std::string msg = sqlite3_errstr(sqliteCode);
    throw SQLException(msg, __FILE__, "", __LINE__);
  }
}

void DatabaseMetadataManager::AddCollection(const std::string& name, SchemaType schemaType,
  const std::string& schema, const std::vector<IndexInfoImpl*>& indexes) {
  //statement guard will make sure that the statement is cleared and reset when statementGuard object goes out of scope
  unique_ptr<sqlite3_stmt, void(*)(sqlite3_stmt*)> statementGuard(
      m_insertCollectionSchemaStmt, SQLiteUtils::ClearAndResetStatement);
  

  // 1. Prepare stmt to add data in CollectionSchema table
  int code = sqlite3_bind_text(m_insertCollectionSchemaStmt, 1,  // Index of wildcard
                                     name.c_str(),  // CollectionName
                                     -1,  // length of the string is the number of bytes up to the first zero terminator
                                     SQLITE_STATIC);

  if (code != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  }

  code = sqlite3_bind_text(m_insertCollectionSchemaStmt, 2,  // Index of wildcard
                                 schema.c_str(),  // CollectionSchema
                                 -1,  // length of the string is the number of bytes up to the first zero terminator
                                 SQLITE_STATIC);
  if (code != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  }

  code = sqlite3_bind_int(m_insertCollectionSchemaStmt, 3,  // Index of wildcard
                                static_cast<int>(schemaType));
  if (code != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  }

  // 2. Start Transaction before issuing insert
  code = sqlite3_exec(m_metadataDBConnection.get(), "BEGIN", 0, 0, 0);
  if (code != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  }

  code = sqlite3_step(m_insertCollectionSchemaStmt);
  if (code != SQLITE_DONE) {
    if (code == SQLITE_CONSTRAINT) {
      //Key already exists
      ostringstream ss;
      ss << "Collection with name \"" << name << "\" already exists.";
      std::string errorMsg = ss.str();
      sqlite3_exec(m_metadataDBConnection.get(), "ROLLBACK", 0, 0, 0);
      throw CollectionAlreadyExistException(ss.str(), __FILE__, "", __LINE__);
    } else {
      sqlite3_exec(m_metadataDBConnection.get(), "ROLLBACK", 0, 0, 0);
      throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
    }
  }

  // 3. Add all the collection indexes
  try {
    for (int i = 0; i < indexes.size(); i++) {
      CreateIndex(name.c_str(), *indexes[i]);
    }
  } catch (...) {
    sqlite3_exec(m_metadataDBConnection.get(), "ROLLBACK", 0, 0, 0);    
    throw;
  }

  code = sqlite3_exec(m_metadataDBConnection.get(), "COMMIT", 0, 0, 0);
  if (code != SQLITE_OK) {
    // Comment copied from sqlite documentation. It is recommended that
    // applications respond to the errors listed above by explicitly issuing
    // a ROLLBACK command.If the transaction has already been rolled back
    // automatically by the error response, then the ROLLBACK command will
    // fail with an error, but no harm is caused by this.
    sqlite3_exec(m_metadataDBConnection.get(), "ROLLBACK", 0, 0, 0);
    throw SQLException(sqlite3_errstr(code), __FILE__, "", __LINE__);
  }  
}

const std::string& DatabaseMetadataManager::GetFullDBPath() const {
  return m_fullDbPath;
}

const std::string& DatabaseMetadataManager::GetDBPath() const {
  return m_dbPath;
}

const std::string& DatabaseMetadataManager::GetDBName() const {
  return m_dbName;
}

void DatabaseMetadataManager::CreateIndex(const std::string& collectionName,
                                            const IndexInfoImpl& indexInfo) {
  unique_ptr<sqlite3_stmt, void(*)(sqlite3_stmt*)> statementGuard(
      m_insertCollectionIndexStmt, SQLiteUtils::ClearAndResetStatement);

  int sqliteCode = sqlite3_bind_text(m_insertCollectionIndexStmt, 1,  // Index of wildcard
                                     indexInfo.GetIndexName().c_str(), -1,  // length of the string is the number of bytes up to the first zero terminator
                                     SQLITE_STATIC);
  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, "", __LINE__);

  sqliteCode = sqlite3_bind_text(m_insertCollectionIndexStmt, 2,  // Index of wildcard
                                 collectionName.c_str(), -1,  // length of the string is the number of bytes up to the first zero terminator
                                 SQLITE_STATIC);
  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, "", __LINE__);

  BufferImpl buffer;
  SerializerUtils::IndexInfoToBytes(indexInfo, buffer);
  sqliteCode = sqlite3_bind_blob(m_insertCollectionIndexStmt, 3,  // Index of wildcard
                                 buffer.GetData(), buffer.GetLength(),
                                 SQLITE_STATIC);
  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, "", __LINE__);

  //Now insert the record
  sqliteCode = sqlite3_step(m_insertCollectionIndexStmt);
  if (sqliteCode != SQLITE_DONE) {
    if (sqliteCode == SQLITE_CONSTRAINT) {
      //Key already exists
      ostringstream ss;
      ss << "Index with name " << indexInfo.GetIndexName() << " already exists.";
      throw IndexAlreadyExistException(ss.str(), __FILE__, "", __LINE__);
    } else {
      throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, "", __LINE__);
    }
  }
}

void DatabaseMetadataManager::FinalizeStatements() {
  GuardFuncs::SQLite3Finalize(m_insertCollectionIndexStmt);
  GuardFuncs::SQLite3Finalize(m_insertCollectionSchemaStmt);  
}
