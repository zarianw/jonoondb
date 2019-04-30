#pragma once

#include <boost/endian/conversion.hpp>
#include <cstdint>

namespace jonoondb_api {
class EndianUtils {
 public:
  static bool IsLittleEndianMachine;

  static void HostToLittleEndian(uint64_t& x);

  static void LittleEndianToHost(uint64_t& x);
};

}  // namespace jonoondb_api
