#include <vector>
#include "gtest/gtest.h"
#include "jonoondb_utils/varint.h"

using namespace jonoondb_utils;

TEST(Varint, EncodeDecode) {
  char buffer[10];
  for (size_t i = 0; i < 10; i++) {
    auto size = Varint::EncodeVarint<uint32_t>(i, (uint8_t*)buffer);
    ASSERT_EQ(1, size);
    auto num = Varint::DecodeVarint<uint32_t>((uint8_t*)buffer);
    ASSERT_EQ(i, num);
  }  
}