#pragma once

#include <string>

namespace jonoondb_test {
extern std::string g_TestRootDirectory;
extern void RemoveAndCreateFile(const char* path, size_t fileSize);
}  // namespace jonoondb_test
