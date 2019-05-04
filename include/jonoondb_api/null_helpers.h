#pragma once

#include <cstring>
#include <limits>
#include <string>

namespace jonoondb_api {

const std::string JONOONDB_NULL_STR("\0\0\0\0", 4);
const std::int32_t JONOONDB_NULL_INT32 =
    std::numeric_limits<std::int32_t>::min();
const std::int64_t JONOONDB_NULL_INT64 =
    std::numeric_limits<std::int64_t>::min();
const double JONOONDB_NULL_DOUBLE = std::numeric_limits<double>::min();

class NullHelpers {
 public:
  // This function checks if the str is null according to jonoondb rules
  // jonoondb considers a string of size 4 will all null characters as null
  static bool IsNull(const std::string& str) {
    int zero = 0;
    if (str.size() == 4 && std::memcmp(str.data(), &zero, 4) == 0) {
      return true;
    }

    return false;
  }

  static bool IsNull(std::int32_t val) {
    return val == JONOONDB_NULL_INT32;
  }

  static bool IsNull(std::int64_t val) {
    return val == JONOONDB_NULL_INT64;
  }

  static bool IsNull(double val) {
    return val == JONOONDB_NULL_DOUBLE;
  }
};

}  // namespace jonoondb_api
