#pragma once

#include <string>
#include <vector>
#include <boost/tokenizer.hpp>

namespace jonoondb_api {
class StringUtils {
 public:
  static std::vector<std::string> Split(const std::string& strToSplit,
                                        const char* seperators) {
    boost::char_separator<char> sep(seperators);
    boost::tokenizer<boost::char_separator<char>> tokens(strToSplit, sep);
    return std::vector<std::string>(tokens.begin(), tokens.end());
  }
};

}  // jonoondb_api
