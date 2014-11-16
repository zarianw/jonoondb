#pragma once

#include <memory>
#include "ewah_boolarray/ewah.h"

namespace jonoondb_api {
class MamaJenniesBitmap {
public:  
  MamaJenniesBitmap();
  MamaJenniesBitmap(MamaJenniesBitmap&& other);
  void Add(size_t x);
  static MamaJenniesBitmap And(const MamaJenniesBitmap& b1, const MamaJenniesBitmap& b2);
  static MamaJenniesBitmap Or(const MamaJenniesBitmap& b1, const MamaJenniesBitmap& b2);
private:
  MamaJenniesBitmap(EWAHBoolArray<size_t>* ewahBoolArray);
  std::unique_ptr<EWAHBoolArray<size_t>> m_ewahBoolArray;
};
}