#pragma once

#include <memory>
#include <cstdint>
#include <vector>
#include "gsl/gsl.h"
#include "mama_jennies_bitmap.h"

namespace jonoondb_api {
class IDSequence final {
 public:
  IDSequence(std::shared_ptr<MamaJenniesBitmap> bitmap, int vecSize);
  const gsl::span<std::uint64_t>& Current();
  bool Next();
 private:
  std::shared_ptr<MamaJenniesBitmap> m_bitmap;
  std::unique_ptr<MamaJenniesBitmap::const_iterator> m_iter;
  std::unique_ptr<MamaJenniesBitmap::const_iterator> m_end;
  std::vector<std::uint64_t> m_currentVector;
  gsl::span<std::uint64_t> m_currentSpan;
};
} // namespace jonoondb_api
