#pragma once

#include <string>
#include "buffer.h"
#include "database.h"

namespace jonoondb_test {
extern std::string g_TestRootDirectory;
extern std::string g_SchemaFolderPath;
extern void RemoveAndCreateFile(const char* path, size_t fileSize);
extern std::string ReadTextFile(const char* path);
extern jonoondb_api::BufferImpl GetTweetObject();
extern jonoondb_api::Buffer GetTweetObject2();
}  // namespace jonoondb_test
