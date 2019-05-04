#pragma once

#include <boost/tokenizer.hpp>
#include <string>
#include <vector>

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

}  // namespace jonoondb_api
