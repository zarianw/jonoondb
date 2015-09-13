#pragma once

#include <string>
#include <sstream>
#include "sqlite3.h"
#include "status.h"

namespace jonoondb_api {
class ExceptionUtils {
 public:

  static Status GetSQLiteErrorStatusFromSQLiteErrorCode(int errorCode) {
    std::string sqliteErrorMsg = sqlite3_errstr(errorCode);  //Memory for sqliteError is managed internally by sqlite
    return Status(kStatusSQLiteErrorCode, sqliteErrorMsg.c_str(),
                  sqliteErrorMsg.length());
  }

  static Status GetMissingFieldErrorStatus(const char* fieldName) {
    std::ostringstream ss;
    ss << "Field definition for " << fieldName
       << " not found in the parsed schema.";
    std::string errorMsg = ss.str();
    return Status(kStatusGenericErrorCode, errorMsg.c_str(), errorMsg.length());
  }

  static Status GetInvalidStructFieldErrorStatus(const char* fieldName,
                                                 const char* fullName) {
    std::ostringstream ss;
    ss << "Field " << fieldName
       << " is not of type struct. Full name provided was " << fullName;
    std::string errorMsg = ss.str();
    return Status(kStatusGenericErrorCode, errorMsg.c_str(), errorMsg.length());
  }
};
}  // namespace jonoondb_api
