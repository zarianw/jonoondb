#pragma once

#include <string>
#include <limits>

namespace jonoondb_api {
class NullHelpers {
public:
  // This function checks if the str is null according to jonoondb rules
  // jonoondb considers a string of size 4 will all null characters as null
  static bool IsNull(std::string& str) {
    int zero = 0;
    if (str.size() == 4 && memcmp(str.data(), &zero, 4) == 0) {
      return true;
    }

    return false;
  }

  static bool IsNull(std::int32_t val) {
    if (val == std::numeric_limits<std::int32_t>::min()) {
      return true;
    }

    return false;
  }

  static bool IsNull(std::int64_t val) {
    if (val == std::numeric_limits<std::int64_t>::min()) {
      return true;
    }

    return false;
  }

  static bool IsNull(double val) {
    if (val == std::numeric_limits<double>::min()) {
      return true;
    }

    return false;
  }
};

}  // namespace jonoondb_api
