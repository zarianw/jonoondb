#pragma once
#include <string>
#include <cstring>

namespace jonoon_utils {
class StringUtils {
 public:

  static bool IsNullOrEmpty(const char* str) {
    if (str == nullptr) {
      return true;
    }

    if (strlen(str) == 0) {
      return true;
    }

    return false;
  }

  static bool IsNullOrEmpty(const std::string& str) {
    if (str.size() == 0) {
      return true;
    }

    return false;
  }

};

}  // jonoondb_api
