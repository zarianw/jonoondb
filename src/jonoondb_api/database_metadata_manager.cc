#include <string>
#include <memory>
#include "sqlite3.h"
#include "boost/filesystem.hpp"
#include "database_metadata_manager.h"
#include "status.h"
#include "string_utils.h"
#include "exception_utils.h"
#include "sqlite_utils.h"
#include "exception_utils.h"
#include "index_info.h"
#include "serializer_utils.h"

using namespace std;
using namespace boost::filesystem;
using namespace jonoondb_api;
using namespace jonoon_utils;

DatabaseMetadataManager::DatabaseMetadataManager(const char* dbPath,
                                                 const char* dbName,
                                                 bool createDBIfMissing)
    : m_dbPath(dbPath),
      m_dbName(dbName),
      m_createDBIfMissing(createDBIfMissing),
      m_metadataDBConnection(nullptr) {
}

DatabaseMetadataManager::~DatabaseMetadataManager() {
  if (m_metadataDBConnection != nullptr) {
    if (sqlite3_close(m_metadataDBConnection) != SQLITE_OK) {
      //Todo: Handle SQLITE_BUSY response here
    }
    m_metadataDBConnection = nullptr;
  }
}

Status DatabaseMetadataManager::Initialize() {
  string errorMessage;

  path pathObj(m_dbPath);
  pathObj += m_dbName;
  pathObj += ".dat";

  if (!boost::filesystem::exists(pathObj) && !m_createDBIfMissing) {
    return Status(kStatusMissingDatabaseFileCode, errorMessage.c_str(),
                  (int32_t) errorMessage.length());
  }

  int sqliteCode = sqlite3_open(pathObj.string().c_str(),
                                &m_metadataDBConnection);  //, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr);

  if (sqliteCode != SQLITE_OK) {
    errorMessage = ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(
        sqliteCode);
    return Status(kStatusFailedToOpenMetadataDatabaseFileCode,
                  errorMessage.c_str(), errorMessage.length());
  }

  auto status = CreateTables();
  if (!status.OK()) {
    return status;
  }

  status = PrepareStatements();

  return status;
}

Status DatabaseMetadataManager::CreateTables() {
  // Set DB Pragmas
  int sqliteCode = 0;
  sqliteCode = sqlite3_exec(m_metadataDBConnection,
                            "PRAGMA synchronous = FULL;", 0, 0, 0);

  if (sqliteCode != SQLITE_OK) {
    string errorMessage = ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(
        sqliteCode);
    return Status(kStatusSQLiteErrorCode, errorMessage.c_str(),
                  errorMessage.length());
  }

  sqliteCode = sqlite3_exec(m_metadataDBConnection,
                            "PRAGMA journal_mode = WAL;", 0, 0, 0);
  if (sqliteCode != SQLITE_OK) {
    string errorMessage = ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(
        sqliteCode);
    return Status(kStatusSQLiteErrorCode, errorMessage.c_str(),
                  errorMessage.length());
  }

  sqliteCode = sqlite3_busy_handler(m_metadataDBConnection,
                                    SQLiteUtils::SQLiteGenericBusyHandler,
                                    nullptr);

  if (sqliteCode != SQLITE_OK) {
    string errorMessage = ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(
        sqliteCode);
    return Status(kStatusSQLiteErrorCode, errorMessage.c_str(),
                  errorMessage.length());
  }

  // Create the necessary tables if they do not exist
  string sql =
      "create table if not exists CollectionSchema(CollectionName text primary key, CollectionSchema text, CollectionSchemaType int)";
  auto status = SQLiteUtils::ExecuteSQL(m_metadataDBConnection, sql);
  if (!status.OK()) {
    return status;
  }

  sql =
      "create table if not exists CollectionIndex(IndexName text primary key, CollectionName text, IndexInfo blob)";
  status = SQLiteUtils::ExecuteSQL(m_metadataDBConnection, sql);
  if (!status.OK()) {
    return status;
  }

  //sql = "create table if not exists CollectionDocumentFile(FileKey int primary key, FileName text, FileDataLength int, foreign key(CollectionName) references CollectionMetadata(CollectionName))";
  sql =
      "create table if not exists CollectionDocumentFile(FileKey int primary key, FileName text, FileDataLength int, CollectionName text)";

  status = SQLiteUtils::ExecuteSQL(m_metadataDBConnection, sql);
  if (!status.OK()) {
    return status;
  }

  return Status();
}

Status DatabaseMetadataManager::PrepareStatements() {
  int sqliteCode =
      sqlite3_prepare_v2(
          m_metadataDBConnection,
          "insert into CollectionIndex (IndexName, CollectionName, IndexInfo) values (?, ?, ?)",  // stmt
          -1,  // If greater than zero, then stmt is read up to the first null terminator
          &m_insertCollectionIndexStmt,  //Statement that is to be prepared
          0  // Pointer to unused portion of stmt
          );

  if (sqliteCode != SQLITE_OK) {
    std::string errorMessage =
        ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(sqliteCode);
    return Status(kStatusSQLiteErrorCode, errorMessage.c_str(),
                  errorMessage.length());
  }

  sqliteCode =
      sqlite3_prepare_v2(
          m_metadataDBConnection,
          "insert into CollectionSchema (CollectionName, CollectionSchema, CollectionSchemaType) values (?, ?, ?)",  // stmt
          -1,  // If greater than zero, then stmt is read up to the first null terminator
          &m_insertCollectionSchemaStmt,  //Statement that is to be prepared
          0  // Pointer to unused portion of stmt
          );

  if (sqliteCode != SQLITE_OK) {
    std::string errorMessage =
        ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(sqliteCode);
    return Status(kStatusSQLiteErrorCode, errorMessage.c_str(),
                  errorMessage.length());
  }

  return Status();
}

Status DatabaseMetadataManager::Open(
    const char* dbPath, const char* dbName, bool createDBIfMissing,
    DatabaseMetadataManager*& databaseMetadataManager) {
  string errorMessage;

  // Validate function arguments
  if (StringUtils::IsNullOrEmpty(dbPath)) {
    errorMessage = "Argument dbPath is null or empty.";
    return Status(kStatusInvalidArgumentCode, errorMessage.c_str(),
                  errorMessage.length());
  }

  if (StringUtils::IsNullOrEmpty(dbName)) {
    errorMessage = "Argument dbName is null or empty.";
    return Status(kStatusInvalidArgumentCode, errorMessage.c_str(),
                  errorMessage.length());
  }

  unique_ptr<DatabaseMetadataManager> dbMetadataManager(
      new DatabaseMetadataManager(dbPath, dbName, createDBIfMissing));
  auto status = dbMetadataManager->Initialize();
  if (!status.OK()) {
    return status;
  }

  databaseMetadataManager = dbMetadataManager.release();

  return status;
}

Status DatabaseMetadataManager::AddCollection(const char* name, int schemaType,
                                              const char* schema,
                                              const IndexInfo indexes[],
                                              int indexesLength) {
  //statement guard will make sure that the statement is cleared and reset when statementGuard object goes out of scope
  unique_ptr<sqlite3_stmt, Status (*)(sqlite3_stmt*)> statementGuard(
      m_insertCollectionSchemaStmt, SQLiteUtils::ClearAndResetStatement);

  int sqliteCode = sqlite3_bind_text(m_insertCollectionSchemaStmt, 1,  // Index of wildcard
                                     name,  // CollectionName
                                     -1,  // length of the string is the number of bytes up to the first zero terminator
                                     SQLITE_STATIC);

  if (sqliteCode != SQLITE_OK) {
    std::string errorMsg = ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(
        sqliteCode);
    return Status(kStatusSQLiteErrorCode, errorMsg.c_str(), errorMsg.length());
  }

  sqliteCode = sqlite3_bind_text(m_insertCollectionSchemaStmt, 2,  // Index of wildcard
                                 schema,  // CollectionSchema
                                 -1,  // length of the string is the number of bytes up to the first zero terminator
                                 SQLITE_STATIC);

  if (sqliteCode != SQLITE_OK) {
    std::string errorMsg = ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(
        sqliteCode);
    return Status(kStatusSQLiteErrorCode, errorMsg.c_str(), errorMsg.length());
  }

  sqliteCode = sqlite3_bind_int(m_insertCollectionSchemaStmt, 3,  // Index of wildcard
                                schemaType);

  if (sqliteCode != SQLITE_OK) {
    std::string errorMsg = ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(
        sqliteCode);
    return Status(kStatusSQLiteErrorCode, errorMsg.c_str(), errorMsg.length());
  }

  //Now insert the record
  sqliteCode = sqlite3_step(m_insertCollectionSchemaStmt);
  if (sqliteCode != SQLITE_DONE) {
    if (sqliteCode == SQLITE_CONSTRAINT) {
      //Key already exists
      std::string errorMsg = "Collection with the same name already exists.";
      return Status(kStatusCollectionAlreadyExistCode, errorMsg.c_str(),
                    errorMsg.length());
    } else {
      std::string errorMsg = ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(
          sqliteCode);
      return Status(kStatusSQLiteErrorCode, errorMsg.c_str(), errorMsg.length());
    }
  }

  // Add all the collection indexes
  for (int i = 0; i < indexesLength; i++) {
    CreateIndex(name, indexes[i]);
  }

  return Status();
}

Status DatabaseMetadataManager::CreateIndex(const char* collectionName,
                                            const IndexInfo& indexInfo) {
  unique_ptr<sqlite3_stmt, Status (*)(sqlite3_stmt*)> statementGuard(
      m_insertCollectionIndexStmt, SQLiteUtils::ClearAndResetStatement);

  int sqliteCode = sqlite3_bind_text(m_insertCollectionIndexStmt, 1,  // Index of wildcard
                                     indexInfo.GetName(), -1,  // length of the string is the number of bytes up to the first zero terminator
                                     SQLITE_STATIC);

  if (sqliteCode != SQLITE_OK) {
    std::string errorMsg = ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(
        sqliteCode);
    return Status(kStatusSQLiteErrorCode, errorMsg.c_str(), errorMsg.length());
  }

  sqliteCode = sqlite3_bind_text(m_insertCollectionIndexStmt, 2,  // Index of wildcard
                                 collectionName, -1,  // length of the string is the number of bytes up to the first zero terminator
                                 SQLITE_STATIC);

  if (sqliteCode != SQLITE_OK) {
    std::string errorMsg = ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(
        sqliteCode);
    return Status(kStatusSQLiteErrorCode, errorMsg.c_str(), errorMsg.length());
  }

  Buffer buffer;
  auto status = SerializerUtils::IndexInfoToBytes(indexInfo, buffer);
  if (!status.OK()) {
    return status;
  }

  sqliteCode = sqlite3_bind_blob(m_insertCollectionIndexStmt, 3,  // Index of wildcard
                                 buffer.GetData(), buffer.GetLength(),
                                 SQLITE_STATIC);

  if (sqliteCode != SQLITE_OK) {
    std::string errorMsg = ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(
        sqliteCode);
    return Status(kStatusSQLiteErrorCode, errorMsg.c_str(), errorMsg.length());
  }

  //Now insert the record
  sqliteCode = sqlite3_step(m_insertCollectionIndexStmt);
  if (sqliteCode != SQLITE_DONE) {
    if (sqliteCode == SQLITE_CONSTRAINT) {
      //Key already exists
      std::string errorMsg = "Index with the same name already exists.";
      return Status(kStatusIndexAlreadyExistCode, errorMsg.c_str(),
                    errorMsg.length());
    } else {
      std::string errorMsg = ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(
          sqliteCode);
      return Status(kStatusSQLiteErrorCode, errorMsg.c_str(), errorMsg.length());
    }
  }

  return Status();
}
