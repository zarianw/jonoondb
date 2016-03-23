#pragma once

#include <cstdint>
#include <string>

namespace jonoondb_api {
struct FileInfo {
  int32_t fileKey;
  std::string fileName;
  std::string fileNameWithPath;
  int64_t dataLength;
};
}  // namespace jonoondb_api
