#pragma once

#include <string>
#include "buffer_impl.h"
#include "database.h"

namespace jonoondb_test {
extern std::string g_TestRootDirectory;
extern std::string g_ResourcesFolderPath;
extern void RemoveAndCreateFile(const char* path, size_t fileSize);
extern jonoondb_api::BufferImpl GetTweetObject();
extern jonoondb_api::Buffer
    GetTweetObject2(std::size_t tweetId, std::size_t userId,
                    const std::string* nameStr, const std::string* textStr,
                    double rating, const std::string* binData);
extern std::string GetSchemaFilePath(const std::string& fileName);
extern jonoondb_api::Buffer GetAllFieldTypeObjectBuffer(
  char field1, unsigned char field2, bool field3, std::int16_t field4,
  std::uint16_t field5, std::int32_t field6, std::uint32_t field7, float field8,
  std::int64_t field9, double field10, const std::string& field11,
  const std::string& field12, const std::string& field13);
extern jonoondb_api::Options GetDefaultDBOptions();
}  // namespace jonoondb_test
