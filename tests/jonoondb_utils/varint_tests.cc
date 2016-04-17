#include <vector>
#include <cmath>
#include "gtest/gtest.h"
#include "jonoondb_utils/varint.h"

using namespace jonoondb_utils;

TEST(Varint, FastEncodeDecode) {
  char buffer[10];
  uint32_t num;
  uint64_t num64;

  for (auto i = 0; i < pow(2, 7); i++) {
    auto size = Varint::EncodeVarint<uint32_t>(static_cast<uint32_t>(i), (uint8_t*)buffer);
    ASSERT_EQ(1, size);
    ASSERT_TRUE(Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num));
    ASSERT_EQ(i, num);
  }

  for (auto i = pow(2, 7); i < pow(2, 14); i++) {
    auto size = Varint::EncodeVarint<uint32_t>(static_cast<uint32_t>(i), (uint8_t*)buffer);
    ASSERT_EQ(2, size);
    ASSERT_TRUE(Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num));
    ASSERT_EQ(i, num);
  }

  for (auto i = pow(2, 14); i < pow(2, 14) + 10; i++) {
    auto size = Varint::EncodeVarint<uint32_t>(static_cast<uint32_t>(i), (uint8_t*)buffer);
    ASSERT_EQ(3, size);
    ASSERT_TRUE(Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num));
    ASSERT_EQ(i, num);
  }

  for (auto i = pow(2, 21); i < pow(2, 21) + 10; i++) {
    auto size = Varint::EncodeVarint<uint32_t>(static_cast<uint32_t>(i), (uint8_t*)buffer);
    ASSERT_EQ(4, size);
    ASSERT_TRUE(Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num));
    ASSERT_EQ(i, num);
  }

  for (auto i = pow(2, 27); i < pow(2, 27) + 10; i++) {
    auto size = Varint::EncodeVarint<uint32_t>(static_cast<uint32_t>(i), (uint8_t*)buffer);
    ASSERT_EQ(4, size);
    ASSERT_TRUE(Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num));
    ASSERT_EQ(i, num);
  }

  for (auto i = pow(2, 28); i < pow(2, 28) + 10; i++) {
    auto size = Varint::EncodeVarint<uint32_t>(static_cast<uint32_t>(i), (uint8_t*)buffer);
    ASSERT_EQ(5, size);
    ASSERT_TRUE(Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num));
    ASSERT_EQ(i, num);
  }

  for (auto i = pow(2, 32) - 10; i < pow(2, 32); i++) {
    auto size = Varint::EncodeVarint<uint32_t>(static_cast<uint32_t>(i), (uint8_t*)buffer);
    ASSERT_EQ(5, size);
    ASSERT_TRUE(Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num));
    ASSERT_EQ(i, num);
  }
}

TEST(Varint, DISABLED_CompleteEncodeDecode) {
  char buffer[10];
  uint32_t num;
  uint64_t num64;

  for (auto i = 0; i < pow(2, 7); i++) {
    auto size = Varint::EncodeVarint<uint32_t>(static_cast<uint32_t>(i), (uint8_t*)buffer);
    ASSERT_EQ(1, size);
    ASSERT_TRUE(Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num));
    ASSERT_EQ(i, num);
  }

  for (auto i = pow(2, 7); i < pow(2, 14); i++) {
    auto size = Varint::EncodeVarint<uint32_t>(static_cast<uint32_t>(i), (uint8_t*)buffer);
    ASSERT_EQ(2, size);
    ASSERT_TRUE(Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num));
    ASSERT_EQ(i, num);
  }

  for (auto i = pow(2, 14); i < pow(2, 21); i++) {
    auto size = Varint::EncodeVarint<uint32_t>(static_cast<uint32_t>(i), (uint8_t*)buffer);
    ASSERT_EQ(3, size);
    ASSERT_TRUE(Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num));
    ASSERT_EQ(i, num);
  }

  for (auto i = pow(2, 21); i < pow(2, 28); i++) {
    auto size = Varint::EncodeVarint<uint32_t>(static_cast<uint32_t>(i), (uint8_t*)buffer);
    ASSERT_EQ(4, size);
    ASSERT_TRUE(Varint::DecodeVarint<uint32_t>((uint8_t*)buffer, &num));
    ASSERT_EQ(i, num);
  }

  for (auto i = pow(2, 28); i < pow(2, 35); i++) {
    auto size = Varint::EncodeVarint<uint64_t>(static_cast<uint64_t>(i), (uint8_t*)buffer);
    ASSERT_EQ(5, size);
    ASSERT_TRUE(Varint::DecodeVarint<uint64_t>((uint8_t*)buffer, &num64));
    ASSERT_EQ(i, num64);
  }

  for (auto i = pow(2, 35); i < pow(2, 42); i++) {
    auto size = Varint::EncodeVarint<uint64_t>(static_cast<uint64_t>(i), (uint8_t*)buffer);
    ASSERT_EQ(6, size);
    ASSERT_TRUE(Varint::DecodeVarint<uint64_t>((uint8_t*)buffer, &num64));
    ASSERT_EQ(i, num64);
  }
}