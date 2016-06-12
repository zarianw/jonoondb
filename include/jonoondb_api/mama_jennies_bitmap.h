#pragma once

#include <memory>
#include <cstdint>
#include "ewah_boolarray/ewah.h"

namespace jonoondb_api {

class MamaJenniesBitmapConstIterator {
 public:
  MamaJenniesBitmapConstIterator
      (EWAHBoolArray<std::uint64_t>::const_iterator& iter);
  std::size_t operator*() const;
  MamaJenniesBitmapConstIterator& operator++();
  bool operator==(const MamaJenniesBitmapConstIterator& other);
  bool operator!=(const MamaJenniesBitmapConstIterator& other);
  bool operator<(const MamaJenniesBitmapConstIterator& other);
  bool operator<=(const MamaJenniesBitmapConstIterator& other);
  bool operator>(const MamaJenniesBitmapConstIterator& other);
  bool operator>=(const MamaJenniesBitmapConstIterator& other);
 private:
  EWAHBoolArray<std::uint64_t>::const_iterator m_iter;
};

class MamaJenniesBitmap {
 public:
  MamaJenniesBitmap();
  MamaJenniesBitmap(MamaJenniesBitmap&& other);
  MamaJenniesBitmap(const MamaJenniesBitmap& other);
  MamaJenniesBitmap& operator=(const MamaJenniesBitmap& other);
  MamaJenniesBitmap& operator=(MamaJenniesBitmap&& other);
  void Add(std::uint64_t x);
  void LogicalAND(const MamaJenniesBitmap& other, MamaJenniesBitmap& output);
  void LogicalOR(const MamaJenniesBitmap& other, MamaJenniesBitmap& output);


  static std::shared_ptr<MamaJenniesBitmap>
      LogicalAND(std::vector<std::shared_ptr<MamaJenniesBitmap>>& bitmaps);
  static std::shared_ptr<MamaJenniesBitmap>
      LogicalOR(std::vector<std::shared_ptr<MamaJenniesBitmap>>& bitmaps);

  typedef MamaJenniesBitmapConstIterator const_iterator;
  const_iterator begin();
  const_iterator end();
  std::unique_ptr<const_iterator> begin_pointer();
  std::unique_ptr<const_iterator> end_pointer();

 private:
  bool IsEmpty();
  std::uint64_t GetSizeInBits();
  MamaJenniesBitmap
      (std::unique_ptr<EWAHBoolArray<std::uint64_t>> ewahBoolArray);
  std::unique_ptr<EWAHBoolArray<std::uint64_t>> m_ewahBoolArray;
};
} // namesapce jonoondb_api
