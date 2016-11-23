#pragma once

#include <string>
#include "database.h"

namespace jonoondb_api_vx_test {
class TestUtils {
public:
  static jonoondb_api::Buffer
    GetTweetObject(std::size_t tweetId, std::size_t userId,
                    const std::string* nameStr, const std::string* textStr,
                    double rating, const std::string* binData);
  static jonoondb_api::Options GetDefaultDBOptions();  
  static jonoondb_api::Buffer GetAllFieldTypeObjectBuffer(
    char field1, unsigned char field2, bool field3, std::int16_t field4,
    std::uint16_t field5, std::int32_t field6, std::uint32_t field7, float field8,
    std::int64_t field9, double field10, const std::string& field11,
    const std::string& field12, const std::string& field13);
};
}  // namespace jonoondb_test
