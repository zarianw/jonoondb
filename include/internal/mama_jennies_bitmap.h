#pragma once

#include <memory>
#include "ewah_boolarray/ewah.h"

namespace jonoondb_api {

  class MamaJenniesBitmapConstIterator {
  public:
    MamaJenniesBitmapConstIterator(EWAHBoolArray<size_t>::const_iterator& iter);
    std::size_t MamaJenniesBitmapConstIterator::operator*() const;
    MamaJenniesBitmapConstIterator & operator++();
    bool operator==(const MamaJenniesBitmapConstIterator& other);
    bool operator!=(const MamaJenniesBitmapConstIterator& other);
    bool operator<(const MamaJenniesBitmapConstIterator& other);
    bool operator<=(const MamaJenniesBitmapConstIterator& other);
    bool operator>(const MamaJenniesBitmapConstIterator& other);
    bool operator>=(const MamaJenniesBitmapConstIterator& other);   
  private:    
    EWAHBoolArray<size_t>::const_iterator m_iter;
  };

class MamaJenniesBitmap {
 public:
  MamaJenniesBitmap();  
  MamaJenniesBitmap(MamaJenniesBitmap&& other);
  MamaJenniesBitmap(const MamaJenniesBitmap& other);
  MamaJenniesBitmap& operator=(const MamaJenniesBitmap& other);
  MamaJenniesBitmap& operator=(MamaJenniesBitmap&& other);
  void Add(std::size_t x);
  bool IsEmpty();
  std::size_t GetSizeInBits();
  void LogicalAND(const MamaJenniesBitmap& other, MamaJenniesBitmap& output);

  static MamaJenniesBitmap LogicalAND(const MamaJenniesBitmap& b1,
                               const MamaJenniesBitmap& b2);
  static MamaJenniesBitmap LogicalOR(const MamaJenniesBitmap& b1,
                              const MamaJenniesBitmap& b2);  
  static std::shared_ptr<MamaJenniesBitmap> LogicalAnd(std::vector<std::shared_ptr<MamaJenniesBitmap>>& bitmaps);

  typedef MamaJenniesBitmapConstIterator const_iterator;
  const_iterator begin();  
  const_iterator end();
  std::unique_ptr<const_iterator> begin_pointer();
  std::unique_ptr<const_iterator> end_pointer();

 private:
  MamaJenniesBitmap(std::unique_ptr<EWAHBoolArray<size_t>> ewahBoolArray);
  std::unique_ptr<EWAHBoolArray<size_t>> m_ewahBoolArray;
};
} // namesapce jonoondb_api
