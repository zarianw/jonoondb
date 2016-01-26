#pragma once

#include <string>
#include "buffer_impl.h"
#include "database.h"

namespace jonoondb_test {
extern std::string g_TestRootDirectory;
extern std::string g_SchemaFolderPath;
extern void RemoveAndCreateFile(const char* path, size_t fileSize);
extern std::string ReadTextFile(const std::string& path);
extern jonoondb_api::BufferImpl GetTweetObject();
extern jonoondb_api::Buffer GetTweetObject2();
extern jonoondb_api::Buffer GetTweetObject2(std::size_t tweetId, std::size_t userId,
                                            std::string& nameStr, std::string& textStr);
}  // namespace jonoondb_test
