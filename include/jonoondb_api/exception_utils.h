#pragma once

#include <string>
#include <sstream>
#include "sqlite3.h"
#include "jonoondb_exceptions.h"

namespace jonoondb_api {
class ExceptionUtils {
public:
  static std::string GetMissingFieldErrorString(const std::string& fieldName) {
    std::ostringstream ss;
    ss << "Field definition for " << fieldName
       << " not found in the parsed schema.";
    return ss.str();    
  }

  static std::string GetInvalidStructFieldErrorString(const std::string& fieldName,
    const std::string& fullName) {
    std::ostringstream ss;
    ss << "Field " << fieldName
       << " is not of type struct. Full name provided was " << fullName;
    return ss.str();    
  }
};
}  // namespace jonoondb_api
