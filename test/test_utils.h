#pragma once

#include <string>
#include "buffer_impl.h"
#include "database.h"

namespace jonoondb_test {
extern std::string g_TestRootDirectory;
extern std::string g_SchemaFolderPath;
extern void RemoveAndCreateFile(const char* path, size_t fileSize);
extern std::string ReadTextFile(const char* path);
extern jonoondb_api::BufferImpl GetTweetObject();
extern jonoondb_api::Buffer GetTweetObject2();
extern void GetTweetObject2(int tweetId, int userId, std::string& nameStr,
  std::string& textStr, jonoondb_api::Buffer& buffer);
}  // namespace jonoondb_test
