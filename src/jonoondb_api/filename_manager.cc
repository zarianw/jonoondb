#include <string>
#include <sstream>
#include <boost/filesystem.hpp>
#include "filename_manager.h"
#include "sqlite_utils.h"
#include "constants.h"
#include "file_info.h"
#include "guard_funcs.h"
#include "path_utils.h"

using namespace jonoondb_api;

FileNameManager::FileNameManager(const std::string& dbPath, const std::string& dbName,
                                 const std::string& collectionName, bool createDBIfMissing)
    : m_collectionName(collectionName), m_db(nullptr, GuardFuncs::SQLite3Close), m_getFileNameStatement(nullptr),
      m_getLastFileKeyStatement(nullptr), m_putStatement(nullptr), m_updateStatement(nullptr) {

  // Validate arguments
  if (dbPath.size() == 0) {
    throw InvalidArgumentException("Argument dbPath is empty.",
                                   __FILE__, __func__, __LINE__);
  }

  if (dbName.size() == 0) {
    throw InvalidArgumentException("Argument dbName is empty.",
                                   __FILE__, __func__, __LINE__);
  }

  m_dbPath = PathUtils::NormalizePath(dbPath);
  m_dbName = dbName;

  boost::filesystem::path pathObj(m_dbPath);

  // check if the db folder exists
  if (!boost::filesystem::exists(pathObj)) {
    std::ostringstream ss;
    ss << "Database folder " << pathObj.generic_string() << " does not exist.";
    throw MissingDatabaseFolderException(ss.str(), __FILE__, __func__, __LINE__);
  }

  pathObj += m_dbName;
  pathObj += ".dat";    

  if (!boost::filesystem::exists(pathObj) && !createDBIfMissing) {
    std::ostringstream ss;
    ss << "Database file " << pathObj.generic_string() << " does not exist.";
    throw MissingDatabaseFileException(ss.str(), __FILE__, __func__, __LINE__);
  }

  sqlite3* db = nullptr;
  int sqliteCode = sqlite3_open(pathObj.generic_string().c_str(), &db);  //, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr);
  m_db.reset(db);

  if (sqliteCode != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);
  }

  sqliteCode = sqlite3_exec(m_db.get(), "PRAGMA journal_mode = WAL;", 0, 0, 0);
  if (sqliteCode != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);
  }

  //Create the necessary tables if they do not exist
  std::string sql = "CREATE TABLE IF NOT EXISTS CollectionDataFile ("
    "CollectionName Text,"
    "FileKey INT, "
    "FileName TEXT, "
    "FileDataLength INT, "
    "PRIMARY KEY (CollectionName, FileKey))";

  sqliteCode = sqlite3_exec(m_db.get(), sql.c_str(), nullptr, nullptr, nullptr);
  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);


  sqliteCode = sqlite3_busy_handler(m_db.get(), SQLiteUtils::SQLiteGenericBusyHandler, nullptr);
  if (sqliteCode != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);
  }

  sqliteCode = sqlite3_prepare_v2(
    m_db.get(),
    "INSERT INTO CollectionDataFile (CollectionName, FileKey, FileName, FileDataLength) VALUES (?, ?, ?, ?)",  // stmt
    -1, // If greater than zero, then stmt is read up to the first null terminator
    &m_putStatement, //Statement that is to be prepared
    0  // Pointer to unused portion of stmt
    );

  if (sqliteCode != SQLITE_OK) {
    FinalizeStatements();
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);
  }

  sqliteCode = sqlite3_prepare_v2(
    m_db.get(),
    "SELECT FileName FROM CollectionDataFile WHERE CollectionName = ? AND FileKey = ?",  // stmt
    -1, // If greater than zero, then stmt is read up to the first null terminator
    &m_getFileNameStatement, //Statement that is to be prepared
    0  // Pointer to unused portion of stmt
    );

  if (sqliteCode != SQLITE_OK) {
    FinalizeStatements();
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);
  }

  sqliteCode = sqlite3_prepare_v2(
    m_db.get(),
    "SELECT FileKey, FileDataLength "
    "FROM CollectionDataFile "
    "WHERE CollectionName = ? "
    "ORDER BY FileKey DESC LIMIT 1",  // stmt
    -1, // Stmt is read up to the first null terminator
    &m_getLastFileKeyStatement, // Statement that is to be prepared
    0  // Pointer to unused portion of stmt
    );

  if (sqliteCode != SQLITE_OK) {
    FinalizeStatements();
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);
  }

  sqliteCode = sqlite3_prepare_v2(
    m_db.get(),
    "UPDATE CollectionDataFile SET FileDataLength = ? WHERE CollectionName = ? AND FileKey = ?",  // stmt
    -1, // If greater than zero, then stmt is read up to the first null terminator
    &m_updateStatement, // Statement that is to be prepared
    0  // Pointer to unused portion of stmt
    );

  if (sqliteCode != SQLITE_OK) {
    FinalizeStatements();
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);
  }
}

FileNameManager::~FileNameManager() {
  //close all prepared statements.  
  FinalizeStatements();
}

void FileNameManager::GetCurrentDataFileInfo(bool createIfMissing, FileInfo& fileInfo) {
  std::lock_guard<std::mutex> lock(m_mutex);

  //statement guard will make sure that the statement is cleared and reset when statementGuard object goes out of scope
  std::unique_ptr<sqlite3_stmt, void(*)(sqlite3_stmt*)> statementGuard(m_getLastFileKeyStatement, SQLiteUtils::ClearAndResetStatement);

  int sqliteCode = sqlite3_bind_text(
    m_getLastFileKeyStatement,
    1,  // Index of wildcard
    m_collectionName.c_str(),
    -1, // -1 means go until NULL char
    SQLITE_STATIC);

  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);

  //Get the last fileKey
  sqliteCode = sqlite3_step(m_getLastFileKeyStatement);

  if (sqliteCode != SQLITE_ROW) {
    if (sqliteCode == SQLITE_DONE) {
      //This means there are no object file records.
      if (createIfMissing) {
        //Create the first entry
        int fileKey = 0;

        std::ostringstream ss;
        ss << m_dbName << "_" << m_collectionName << "." << fileKey;
        std::string newFileName = ss.str();

        AddFileRecord(fileKey, newFileName);

        fileInfo.fileKey = fileKey;
        fileInfo.fileName = newFileName;        
        auto path = m_dbPath / newFileName;
        fileInfo.fileNameWithPath = path.generic_string();
        fileInfo.dataLength = -1;
      } else {
        throw JonoonDBException("Cannot get the current FileInfo because there are no FileInfo records in the database.",
          __FILE__, __func__, __LINE__);
      }
    } else {
      throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);
    }
  } else {
    //Read the last FileKey
    auto fileKey = sqlite3_column_int(m_getLastFileKeyStatement, 0);
    std::ostringstream ss;
    ss << m_dbName << "_" << m_collectionName << "." << fileKey;
    std::string newFileName = ss.str();

    fileInfo.fileKey = fileKey;
    fileInfo.fileName = newFileName;
    auto path = m_dbPath / newFileName;
    fileInfo.fileNameWithPath = path.generic_string();
    fileInfo.dataLength = sqlite3_column_int64(m_getLastFileKeyStatement, 1);
  }
}

void FileNameManager::GetNextDataFileInfo(FileInfo& fileInfo) {
  int fileKey;
  std::string newFileName;
  //Read the last FileKey
  std::lock_guard<std::mutex> lock(m_mutex);

  {
    //statement guard will make sure that the statement is cleared and reset when statementGuard object goes out of scope
    std::unique_ptr<sqlite3_stmt, void(*)(sqlite3_stmt*)> statementGuard(m_getLastFileKeyStatement, SQLiteUtils::ClearAndResetStatement);

    //Get the last fileKey
    int sqliteCode = sqlite3_bind_text(
      m_getLastFileKeyStatement,
      1,  // Index of wildcard
      m_collectionName.c_str(),
      -1, // -1 means go until NULL char
      SQLITE_STATIC);

    if (sqliteCode != SQLITE_OK)
      throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);

    sqliteCode = sqlite3_step(m_getLastFileKeyStatement);

    if (sqliteCode != SQLITE_ROW) {
      if (sqliteCode == SQLITE_DONE) {
        //This means there are no object file records.			
        throw JonoonDBException("Cannot get the next FileInfo because there are no FileInfo records in the database.",
          __FILE__, __func__, __LINE__);
      } else {
        throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);
      }
    }

    //Read the last FileKey
    fileKey = sqlite3_column_int(m_getLastFileKeyStatement, 0);
    fileKey++;
    std::ostringstream ss;
    ss << m_dbName << "_" << m_collectionName << "." << fileKey;
    newFileName = ss.str();
  }

  AddFileRecord(fileKey, newFileName);

  fileInfo.fileKey = fileKey;
  fileInfo.fileName = newFileName;
  auto path = m_dbPath / newFileName;
  fileInfo.fileNameWithPath = path.generic_string();
  fileInfo.dataLength = -1;
}

void FileNameManager::UpdateDataFileLength(int fileKey, int64_t length) {
  std::lock_guard<std::mutex> lock(m_mutex);

  //statement guard will make sure that the statement is cleared and reset when statementGuard object goes out of scope
  std::unique_ptr<sqlite3_stmt, void(*)(sqlite3_stmt*)> statementGuard(m_updateStatement, SQLiteUtils::ClearAndResetStatement);

  int sqliteCode = sqlite3_bind_int64(
    m_updateStatement,
    1,  // Index of wildcard
    length);

  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);

  sqliteCode = sqlite3_bind_text(
    m_updateStatement,
    2,  // Index of wildcard
    m_collectionName.c_str(),
    -1, // -1 means go until NULL char
    SQLITE_STATIC);

  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);

  

  sqliteCode = sqlite3_bind_int(
    m_updateStatement,
    3,  // Index of wildcard
    fileKey);

  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);

  // Now insert the record
  sqliteCode = sqlite3_step(m_updateStatement);
  if (sqliteCode != SQLITE_DONE) {
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);
  }
}

void FileNameManager::GetFileInfo(const int fileKey, std::shared_ptr<FileInfo>& fileInfo) {
  //First try to get it from in-memory map
  if (!m_fileInfoMap.Find(fileKey, fileInfo)) {
    //Not found in the map, get it from db
    std::lock_guard<std::mutex> lock(m_mutex);

    //statement guard will make sure that the statement is cleared and reset when statementGuard object goes out of scope
    std::unique_ptr<sqlite3_stmt, void(*)(sqlite3_stmt*)> statementGuard(m_getFileNameStatement, SQLiteUtils::ClearAndResetStatement);

    int sqliteCode = sqlite3_bind_text(
      m_getFileNameStatement,
      1,  // Index of wildcard
      m_collectionName.c_str(),
      -1, // -1 means go until NULL char
      SQLITE_STATIC);

    if (sqliteCode != SQLITE_OK)
      throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);

    sqliteCode = sqlite3_bind_int64(
      m_getFileNameStatement,
      2,  // Index of wildcard
      fileKey);

    if (sqliteCode != SQLITE_OK)
      throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);

    sqliteCode = sqlite3_step(m_getFileNameStatement);

    if (sqliteCode != SQLITE_ROW) {
      if (sqliteCode == SQLITE_DONE) {
        // This means the key does not exist
        // This means that the ObjectIndex record is pointing to a FileKey
        // that does not exist in CollectionDataFile table. This is a serious error
        // and should never happen. However the code has to handle this and report
        // the correct error to the user.
        std::ostringstream ss;
        ss << "Could not find FileInfo for FileKey " << fileKey 
          << " and CollectionName " << m_collectionName << ".";
        throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
      } else {
        throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);
      }
    }

    //Fill the FileInfo	
    fileInfo.reset(new FileInfo());
    fileInfo->fileKey = fileKey;
    fileInfo->fileName = (const char *)sqlite3_column_text(m_getFileNameStatement, 0);
    auto path = m_dbPath / fileInfo->fileName;
    fileInfo->fileNameWithPath = path.generic_string();

    //Add it in the map for future lookups
    m_fileInfoMap.Add(fileKey, fileInfo);
  }
}

void FileNameManager::AddFileRecord(int fileKey, const std::string& fileName) {
  //statement guard will make sure that the statement is cleared and reset when statementGuard object goes out of scope
  std::unique_ptr<sqlite3_stmt, void(*)(sqlite3_stmt*)> statementGuard(m_putStatement, SQLiteUtils::ClearAndResetStatement);

  int sqliteCode = sqlite3_bind_text(
    m_putStatement,
    1,  // Index of wildcard
    m_collectionName.c_str(),
    -1, // -1 means go until NULL char
    SQLITE_STATIC);

  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);

  sqliteCode = sqlite3_bind_int(
    m_putStatement,
    2,  // Index of wildcard
    fileKey);

  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);

  sqliteCode = sqlite3_bind_text(
    m_putStatement,
    3,  // Index of wildcard
    fileName.c_str(),
    -1, // -1 means go until NULL char
    SQLITE_STATIC);

  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);

  sqliteCode = sqlite3_bind_int64(
    m_putStatement,
    4,  // Index of wildcard
    -1); // Default value, -1 represents that the length has not been set

  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);

  //Now insert the record
  sqliteCode = sqlite3_step(m_putStatement);
  if (sqliteCode != SQLITE_DONE) {
    if (sqliteCode == SQLITE_CONSTRAINT) {
      //Key already exists     
      std::ostringstream ss;
      ss << "The specified file key '" << fileKey 
        << "' already exists for collection '" << m_collectionName << "'.";
      throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
    } else {
      throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);
    }
  }
}

void FileNameManager::FinalizeStatements() {
  GuardFuncs::SQLite3Finalize(m_getFileNameStatement);
  GuardFuncs::SQLite3Finalize(m_getLastFileKeyStatement);
  GuardFuncs::SQLite3Finalize(m_putStatement);
  GuardFuncs::SQLite3Finalize(m_updateStatement);
}
