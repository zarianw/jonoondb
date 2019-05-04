#pragma once

#include <boost/filesystem.hpp>
#include <string>

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
}  // namespace jonoondb_api