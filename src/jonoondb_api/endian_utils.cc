#include "jonoondb_api/endian_utils.h"

using namespace jonoondb_api;

static bool OnLittleEndianMachine() {
  int a = 1;
  std::int8_t val = *((std::int8_t*)&a);
  return val != 0;
}

bool jonoondb_api::EndianUtils::IsLittleEndianMachine =
    OnLittleEndianMachine();

void EndianUtils::HostToLittleEndian(uint64_t& x) {
  if (!IsLittleEndianMachine)
		boost::endian::endian_reverse_inplace(x);
}

void EndianUtils::LittleEndianToHost(uint64_t& x) {
  if (!IsLittleEndianMachine)
    boost::endian::endian_reverse_inplace(x);
}
