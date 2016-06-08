#pragma once

#include <string>
#include <boost/filesystem.hpp>

namespace jonoondb_api {
class PathUtils {
 public:
  static std::string NormalizePath(const std::string& path) {
    boost::filesystem::path pathObj(path);
    std::string normalizedPath = pathObj.generic_string();
    if (normalizedPath.back() != '/') {
      normalizedPath.append("/");
    }

    return normalizedPath;
  }
};
} // namespace jonoondb_api