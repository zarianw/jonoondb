#pragma once

#include <boost/endian/conversion.hpp>
#include <cstdint>

namespace jonoondb_api {
class EndianUtils {
 public:
  static bool IsLittleEndianMachine;

  static void MakeLittleEndianInPlace(uint64_t& x);
};

}  // namespace jonoondb_api
