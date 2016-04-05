#pragma once

#include <string>
#include "buffer_impl.h"
#include "database.h"

namespace jonoondb_test {
extern std::string g_TestRootDirectory;
extern std::string g_ResourcesFolderPath;
extern void RemoveAndCreateFile(const char* path, size_t fileSize);
extern jonoondb_api::BufferImpl GetTweetObject();
extern jonoondb_api::Buffer GetTweetObject2(std::size_t tweetId, std::size_t userId,
                                            const std::string* nameStr, const std::string* textStr,
                                            double rating);
extern std::string GetSchemaFilePath(const std::string& fileName);
}  // namespace jonoondb_test
