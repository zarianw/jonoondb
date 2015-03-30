#pragma once

#include <string>
#include <status.h>
#include <buffer.h>

namespace jonoondb_test {
extern std::string g_TestRootDirectory;
extern std::string g_SchemaFilePath;
extern void RemoveAndCreateFile(const char* path, size_t fileSize);
extern std::string ReadTextFile(const char* path);
extern jonoondb_api::Status GetTweetObject(jonoondb_api::Buffer& buffer);
}  // namespace jonoondb_test
