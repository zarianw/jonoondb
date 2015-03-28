#pragma once

#include <string>

namespace jonoondb_test {
extern std::string g_TestRootDirectory;
extern std::string g_SchemaFilePath;
extern void RemoveAndCreateFile(const char* path, size_t fileSize);
extern std::string ReadTextFile(const char* path);
}  // namespace jonoondb_test
