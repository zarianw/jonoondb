#pragma once

#include <sstream>
#include <string>

namespace jonoondb_api {
class ExceptionUtils {
 public:
  static std::string GetMissingFieldErrorString(const std::string& fieldName);
  static std::string GetInvalidStructFieldErrorString(
      const std::string& fieldName, const std::string& fullName);
  static int GetError();
  static std::string GetErrorTextFromErrorCode(int errorCode);
};
}  // namespace jonoondb_api
