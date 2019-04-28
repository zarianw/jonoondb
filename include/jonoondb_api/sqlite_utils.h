#pragma once

#include <assert.h>
#include <string>
#include <thread>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem.hpp>
#include "sqlite3.h"
#include "jonoondb_api/jonoondb_exceptions.h"
#include "jonoondb_api/path_utils.h"
#include "jonoondb_api/guard_funcs.h"

namespace jonoondb_api {
const std::chrono::milliseconds SQLiteBusyHandlerRetryIntervalInMillisecs(200);
const int SQLiteBusyHandlerRetryCount = 20;

class SQLiteUtils {
 public:
  static void ClearAndResetStatement(sqlite3_stmt* statement) {
    if (statement != nullptr) {
      // Reset all params back to null
      auto code = sqlite3_clear_bindings(statement);
      // Reset the statement so that it can be re-executed
      sqlite3_reset(statement);

      if (code != SQLITE_OK)
        throw SQLException(sqlite3_errstr(code), __FILE__, __func__, __LINE__);
    }
  }

  static int SQLiteGenericBusyHandler(void* input, int retryCount) {
    if (retryCount > SQLiteBusyHandlerRetryCount) {
      return 0;  // This will stop the retry attempts and SQLITE_BUSY will be
                 // returned to the caller
    }

    std::this_thread::sleep_for(SQLiteBusyHandlerRetryIntervalInMillisecs);

    return 1;
  }

  static void CloseSQLiteConnection(sqlite3* dbConnection) {
    if (dbConnection != nullptr) {
      if (sqlite3_close(dbConnection) != SQLITE_OK) {
        // Todo: Handle SQLITE_BUSY response here
      }
      dbConnection = nullptr;
    }
  }

  static void HandleSQLiteCode(int sqliteCode, char* errMsg = nullptr) {
    if (sqliteCode != SQLITE_OK) {
      if (errMsg != nullptr) {
        std::string sqliteErrorMsg = errMsg;
        sqlite3_free(errMsg);
        throw SQLException(sqliteErrorMsg, __FILE__, __func__, __LINE__);
      }

      throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);
    }
  }

  static std::unique_ptr<sqlite3, void (*)(sqlite3*)>
  NormalizePathAndCreateDBConnection(const std::string& dbPath,
                                     const std::string& dbName,
                                     bool createDBIfMissing,
                                     boost::filesystem::path& normalizedPath) {
    // Validate arguments
    if (dbPath.size() == 0) {
      throw InvalidArgumentException("Argument dbPath is empty.", __FILE__,
                                     __func__, __LINE__);
    }

    if (dbName.size() == 0) {
      throw InvalidArgumentException("Argument dbName is empty.", __FILE__,
                                     __func__, __LINE__);
    }

    normalizedPath = PathUtils::NormalizePath(dbPath);

    boost::filesystem::path pathObj(normalizedPath);

    // check if the db folder exists
    if (!boost::filesystem::exists(pathObj)) {
      std::ostringstream ss;
      ss << "Database folder " << pathObj.generic_string()
         << " does not exist.";
      throw MissingDatabaseFolderException(ss.str(), __FILE__, __func__,
                                           __LINE__);
    }

    pathObj += dbName;
    pathObj += ".dat";

    if (!boost::filesystem::exists(pathObj) && !createDBIfMissing) {
      std::ostringstream ss;
      ss << "Database file " << pathObj.generic_string() << " does not exist.";
      throw MissingDatabaseFileException(ss.str(), __FILE__, __func__,
                                         __LINE__);
    }

    sqlite3* db = nullptr;
    int sqliteCode =
        sqlite3_open(pathObj.generic_string().c_str(),
                     &db);  //, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                            // SQLITE_OPEN_FULLMUTEX, nullptr);

    std::unique_ptr<sqlite3, void (*)(sqlite3*)> dbPtr(
        db, GuardFuncs::SQLite3Close);
    HandleSQLiteCode(sqliteCode);

    return dbPtr;
  }
};

}  // namespace jonoondb_api
