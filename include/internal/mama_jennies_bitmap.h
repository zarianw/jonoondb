#pragma once

#include <memory>
#include "ewah_boolarray/ewah.h"

namespace jonoondb_api {
class MamaJenniesBitmap {
 public:
  MamaJenniesBitmap();  
  MamaJenniesBitmap(MamaJenniesBitmap&& other);
  void Add(std::size_t x);
  bool IsEmpty();
  std::size_t GetSizeInBits();
  void LogicalAND(const MamaJenniesBitmap& other, MamaJenniesBitmap& output);
  static MamaJenniesBitmap LogicalAND(const MamaJenniesBitmap& b1,
                               const MamaJenniesBitmap& b2);
  static MamaJenniesBitmap LogicalOR(const MamaJenniesBitmap& b1,
                              const MamaJenniesBitmap& b2);
 private:
   MamaJenniesBitmap(std::unique_ptr<EWAHBoolArray<size_t>> ewahBoolArray);
  std::unique_ptr<EWAHBoolArray<size_t>> m_ewahBoolArray;
};
} // namesapce jonoondb_api
