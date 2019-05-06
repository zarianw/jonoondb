#pragma once

#include <cstdint>
#include <memory>
#include "ewah_boolarray/ewah.h"
#include "gsl/span.h"

namespace jonoondb_api {
// forward declarations
class BufferImpl;

enum class BitmapType : std::int32_t { EWAH_COMPRESSED_BITMAP = 1 };

class MamaJenniesBitmapConstIterator {
 public:
  MamaJenniesBitmapConstIterator(
      EWAHBoolArray<std::uint64_t>::const_iterator& iter);
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

typedef std::uint64_t mama_jennies_bitmap_uword;

class MamaJenniesBitmap {
 public:
  MamaJenniesBitmap();
  MamaJenniesBitmap(MamaJenniesBitmap&& other);
  MamaJenniesBitmap(const MamaJenniesBitmap& other);
  MamaJenniesBitmap& operator=(const MamaJenniesBitmap& other);
  MamaJenniesBitmap& operator=(MamaJenniesBitmap&& other);
  void Add(std::uint64_t x);
  void LogicalAND(const MamaJenniesBitmap& other,
                  MamaJenniesBitmap& output) const;
  void LogicalOR(const MamaJenniesBitmap& other,
                 MamaJenniesBitmap& output) const;
  void LogicalXOR(const MamaJenniesBitmap& other,
                  MamaJenniesBitmap& output) const;

  static std::shared_ptr<MamaJenniesBitmap> LogicalAND(
      std::vector<std::shared_ptr<MamaJenniesBitmap>>& bitmaps);
  static std::shared_ptr<MamaJenniesBitmap> LogicalOR(
      std::vector<std::shared_ptr<MamaJenniesBitmap>>& bitmaps);

  void LogicalNOT(MamaJenniesBitmap& output) const;
  void InPlaceLogicalNOT();

  typedef MamaJenniesBitmapConstIterator const_iterator;
  const_iterator begin() const;
  const_iterator end() const;
  std::unique_ptr<const_iterator> begin_pointer();
  std::unique_ptr<const_iterator> end_pointer();

  void Reset();

  void Serialize(BufferImpl& buffer) const;
  void Deserialize(BitmapType type, int version, gsl::span<const char> buffer);
  BitmapType GetType() const;
  bool Empty() const;

 private:
  std::uint64_t GetSizeInBits() const;
  MamaJenniesBitmap(
      std::unique_ptr<EWAHBoolArray<std::uint64_t>> ewahBoolArray);
  std::unique_ptr<EWAHBoolArray<std::uint64_t>> m_ewahBoolArray;
};
}  // namespace jonoondb_api
