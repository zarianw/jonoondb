#include <vector>
#include <cmath>
#include "gtest/gtest.h"
#include "jonoondb_utils/varint.h"

using namespace jonoondb_utils;

bool RunFastVarintTests = true;

TEST(Varint, EncodeDecode) {
  char buffer[10];  
  uint32_t num;

  for (auto i = 0; i < pow(2, 7); i++) {
    auto size = Varint::EncodeVarint<uint32_t>(i, (uint8_t*)buffer);
    ASSERT_EQ(1, size);
    Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num);
    ASSERT_EQ(i, num);
  }  

  for (auto i = pow(2, 7); i < pow(2, 14); i++) {
    auto size = Varint::EncodeVarint<uint32_t>(i, (uint8_t*)buffer);
    ASSERT_EQ(2, size);
    Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num);
    ASSERT_EQ(i, num);
  }

  for (auto i = pow(2, 14); i < pow(2, 21); i++) {
    auto size = Varint::EncodeVarint<uint32_t>(i, (uint8_t*)buffer);
    ASSERT_EQ(3, size);
    Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num);
    ASSERT_EQ(i, num);
  }

  if(RunFastVarintTests) {
    for (auto i = pow(2, 21); i < pow(2, 22); i++) {
      auto size = Varint::EncodeVarint<uint32_t>(i, (uint8_t*)buffer);
      //ASSERT_EQ(4, size);
      Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num);
      //ASSERT_EQ(i, num);
    }

    for (auto i = pow(2, 27); i < pow(2, 28); i++) {
      auto size = Varint::EncodeVarint<uint32_t>(i, (uint8_t*)buffer);
      //ASSERT_EQ(4, size);
      Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num);
      //ASSERT_EQ(i, num);
    }

    for (auto i = pow(2, 28); i < pow(2, 29); i++) {
      auto size = Varint::EncodeVarint<uint32_t>(i, (uint8_t*)buffer);
      //ASSERT_EQ(4, size);
      Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num);
      //ASSERT_EQ(i, num);
    }

    for (auto i = pow(2, 31); i < pow(2, 32); i++) {
      auto size = Varint::EncodeVarint<uint32_t>(i, (uint8_t*)buffer);
      //ASSERT_EQ(4, size);
      Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num);
      //ASSERT_EQ(i, num);
    }
  } else {
    for (auto i = pow(2, 21); i < pow(2, 28); i++) {
      auto size = Varint::EncodeVarint<uint32_t>(i, (uint8_t*)buffer);
      //ASSERT_EQ(4, size);
      Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num);
      //ASSERT_EQ(i, num);
    }

    for (auto i = pow(2, 28); i < pow(2, 32); i++) {
      auto size = Varint::EncodeVarint<uint32_t>(i, (uint8_t*)buffer);
      //ASSERT_EQ(4, size);
      Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num);
      //ASSERT_EQ(i, num);
    }
  }
}