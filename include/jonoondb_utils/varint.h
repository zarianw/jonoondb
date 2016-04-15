#pragma once

#include <cstdint>
#include <jonoondb_exceptions.h>

namespace jonoondb_utils {
#define kMaxVarintBytes 10
class Varint {
public:
  template<typename int_t>
  static int64_t EncodeVarint(int_t value,
                   std::uint8_t* target) {
    auto base = target;
    while (value >= 0x80) {
      *target = static_cast<std::uint8_t>(value | 0x80);
      value >>= 7;
      ++target;
    }
    *target = static_cast<std::uint8_t>(value);
    return target - base;
  }

  template<typename int_t>
  static int_t DecodeVarint(std::uint8_t* input) {
    int_t result = 0;
    int count = 0;
    std::uint32_t b;

    do {
      if (count == kMaxVarintBytes) {
        std::string msg = "input size is more than max varint bytes i.e. ";
        msg.append(std::to_string(kMaxVarintBytes)).append(".");
        throw jonoondb_api::JonoonDBException(msg, __FILE__, __func__, __LINE__);
      }

      b = *input;
      result |= static_cast<int_t>(b & 0x7F) << (7 * count);
      ++input;
      ++count;
    } while (b & 0x80);

    return result;
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
};
} // namespace jonoondb_utils