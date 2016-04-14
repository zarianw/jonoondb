#pragma once

#include <cstdint>

namespace jonoondb_utils {
class Varint {
  template<typename int_t>
  int EncodeVarint(int_t value,
                   std::uint8_t* target) {
    auto base = target;
    while (value >= 0x80) {
      *target = static_cast<uint8>(value | 0x80);
      value >>= 7;
      ++target;
    }
    *target = static_cast<uint8>(value);
    return target - base;
  }

  template<typename int_t>
  int_t DecodeVarint(std::uint8_t* input) {
    int_t result = 0;
    int count = 0;
    std::uint32_t b;

    do {
      if (count == kMaxVarintBytes) {
        // throw        
      }

      b = *input;
      result |= static_cast<uint64>(b & 0x7F) << (7 * count);
      ++input;
      ++count;
    } while (b & 0x80);

    return result;
  }

  inline uint32 ZigZagEncode32(int32 n) {
    // Note:  the right-shift must be arithmetic
    return (static_cast<uint32>(n) << 1) ^ (n >> 31);
  }

  inline int32 ZigZagDecode32(uint32 n) {
    return (n >> 1) ^ -static_cast<int32>(n & 1);
  }

  inline uint64 ZigZagEncode64(int64 n) {
    // Note:  the right-shift must be arithmetic
    return (static_cast<uint64>(n) << 1) ^ (n >> 63);
  }

  inline int64 ZigZagDecode64(uint64 n) {
    return (n >> 1) ^ -static_cast<int64>(n & 1);
  }
private:
  const int kMaxVarintBytes = 10;
};
} // namespace jonoondb_utils