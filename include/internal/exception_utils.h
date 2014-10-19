#pragma once

#include <string>
#include "sqlite3.h"

namespace jonoondb_api {
class ExceptionUtils {
 public:

  static std::string GetSQLiteErrorFromSQLiteErrorCode(int errorCode) {
    std::string sqliteErrorMsg = sqlite3_errstr(errorCode);  //Memory for sqliteError is managed internally by sqlite
    return sqliteErrorMsg;
  }
};
}  // joonondb_api
