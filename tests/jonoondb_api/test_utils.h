#pragma once

#include <string>

namespace jonoondb_test {
extern std::string g_TestRootDirectory;
extern std::string g_ResourcesFolderPath;
extern void RemoveAndCreateFile(const char* path, size_t fileSize);
extern std::string GetSchemaFilePath(const std::string& fileName);
}  // namespace jonoondb_test
