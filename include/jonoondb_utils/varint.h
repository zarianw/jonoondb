#pragma once

#include <cstdint>

namespace jonoondb_utils {
#define kMaxVarintBytes 10
class Varint {
public:
  template<typename int_t>
  inline static int EncodeVarint(int_t value,
                                 std::uint8_t* target) {
    auto base = target;
    while (value >= 0x80) {
      *target = static_cast<std::uint8_t>(value | 0x80);
      value >>= 7;
      ++target;
    }
    *target = static_cast<std::uint8_t>(value);
    target++;
    return static_cast<int>(target - base);
  }

  template<typename int_t>
  inline static int DecodeVarint(std::uint8_t* input, int_t* result) {
    int count = 0;
    std::uint8_t b;
    *result = 0;

    // Fast path for smaller numbers (upto 2 bytes)
    b = *input;
    *result |= static_cast<int_t>(b & 0x7F) << (7 * count);
    ++input;
    ++count;
    if (!(b & 0x80))
      return count;

    b = *input;
    *result |= static_cast<int_t>(b & 0x7F) << (7 * count);
    ++input;
    ++count;
    if (!(b & 0x80))
      return count;

    do {
      if (count == kMaxVarintBytes) {
        return -1;
      }

      b = *input;
      *result |= static_cast<int_t>(b & 0x7F) << (7 * count);
      ++input;
      ++count;
    } while (b & 0x80);

    return count;
  }

  inline std::uint32_t ZigZagEncode32(std::int32_t n) {
    // Note:  the right-shift must be arithmetic
    return (static_cast<std::uint32_t>(n) << 1) ^ (n >> 31);
  }

  inline std::int32_t ZigZagDecode32(std::uint32_t n) {
    return (n >> 1) ^ -static_cast<std::int32_t>(n & 1);
  }

  inline std::uint64_t ZigZagEncode64(std::int64_t n) {
    // Note:  the right-shift must be arithmetic
    return (static_cast<std::uint64_t>(n) << 1) ^ (n >> 63);
  }

  inline std::int64_t ZigZagDecode64(std::uint64_t n) {
    return (n >> 1) ^ -static_cast<std::int64_t>(n & 1);
  }

  inline static bool OnLittleEndianMachine() {
    int a = 1;
    std::int8_t val = *((std::int8_t*) &a);
    return val != 0;
  }
};
} // namespace jonoondb_utils