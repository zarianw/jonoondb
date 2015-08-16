#pragma once
#include <string>
#include <cstring>
#include <vector>
#include <boost/tokenizer.hpp>

namespace jonoondb_api {
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

  static std::vector<std::string> Split(const std::string& strToSplit, const char* seperators) {
    boost::char_separator<char> sep(seperators);   
    boost::tokenizer<boost::char_separator<char>> tokens(strToSplit, sep);
    return std::vector<std::string>(tokens.begin(), tokens.end());
  }

};

}  // jonoondb_api
