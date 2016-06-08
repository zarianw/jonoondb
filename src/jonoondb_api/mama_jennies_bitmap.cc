#include <memory>
#include <sstream>
#include "mama_jennies_bitmap.h"
#include "jonoondb_exceptions.h"

using namespace jonoondb_api;

MamaJenniesBitmapConstIterator::MamaJenniesBitmapConstIterator(EWAHBoolArray<std::uint64_t>::const_iterator& iter)
    : m_iter(iter) {
}

std::size_t MamaJenniesBitmapConstIterator::operator*() const {
  return m_iter.operator*();
}

MamaJenniesBitmapConstIterator& MamaJenniesBitmapConstIterator::operator++() {
  ++m_iter;
  return *this;
}

bool MamaJenniesBitmapConstIterator::operator==(const MamaJenniesBitmapConstIterator& other) {
  return this->m_iter == other.m_iter;
}

bool MamaJenniesBitmapConstIterator::operator!=(const MamaJenniesBitmapConstIterator& other) {
  return this->m_iter != other.m_iter;
}

bool MamaJenniesBitmapConstIterator::operator<(const MamaJenniesBitmapConstIterator& other) {
  return m_iter.operator<(other.m_iter);
}

bool MamaJenniesBitmapConstIterator::operator<=(const MamaJenniesBitmapConstIterator& other) {
  return m_iter.operator<=(other.m_iter);
}

bool MamaJenniesBitmapConstIterator::operator>(const MamaJenniesBitmapConstIterator& other) {
  return m_iter.operator>(other.m_iter);
}

bool MamaJenniesBitmapConstIterator::operator>=(const MamaJenniesBitmapConstIterator& other) {
  return m_iter.operator>=(other.m_iter);
}

MamaJenniesBitmap::MamaJenniesBitmap()
    : m_ewahBoolArray(std::make_unique<EWAHBoolArray<std::uint64_t>>()) {
}

MamaJenniesBitmap::MamaJenniesBitmap(MamaJenniesBitmap&& other) {
  if (this != &other) {
    m_ewahBoolArray.reset(other.m_ewahBoolArray.release());
  }
}

MamaJenniesBitmap::MamaJenniesBitmap(const MamaJenniesBitmap& other) {
  if (this != &other) {
    // Lets call copy ctor of EWAHBoolArray
    m_ewahBoolArray =
        std::make_unique<EWAHBoolArray<std::uint64_t>>(*other.m_ewahBoolArray);
  }
}

MamaJenniesBitmap& MamaJenniesBitmap::operator=(const MamaJenniesBitmap& other) {
  if (this != &other) {
    // Lets call copy ctor of EWAHBoolArray
    m_ewahBoolArray =
        std::make_unique<EWAHBoolArray<std::uint64_t>>(*other.m_ewahBoolArray);
  }
  return *this;
}

MamaJenniesBitmap& MamaJenniesBitmap::operator=(MamaJenniesBitmap&& other) {
  if (this != &other) {
    m_ewahBoolArray.reset(other.m_ewahBoolArray.release());
  }
  return *this;
}

void MamaJenniesBitmap::Add(std::uint64_t x) {
  if (!m_ewahBoolArray->set(x)) {
    throw JonoonDBException(
        "Add to bitmap failed. Most probably the entries were not added in increasing order.",
        __FILE__,
        __func__,
        __LINE__);
  }
}

bool MamaJenniesBitmap::IsEmpty() {
  // Todo: Need to find a faster way to check for empty bitmap
  bool isEmpty = true;
  if (m_ewahBoolArray->sizeInBits() > 0) {
    for (auto iter = m_ewahBoolArray->begin(); iter != m_ewahBoolArray->end();
         iter++) {
      isEmpty = false;
      break;
    }
  }

  return isEmpty;
}

std::uint64_t MamaJenniesBitmap::GetSizeInBits() {
  return m_ewahBoolArray->sizeInBits();
}

void MamaJenniesBitmap::LogicalAND(const MamaJenniesBitmap& other,
                                   MamaJenniesBitmap& output) {
  m_ewahBoolArray->logicaland(*other.m_ewahBoolArray, *output.m_ewahBoolArray);
}

void MamaJenniesBitmap::LogicalOR(const MamaJenniesBitmap& other,
                                  MamaJenniesBitmap& output) {
  m_ewahBoolArray->logicalor(*other.m_ewahBoolArray, *output.m_ewahBoolArray);
}

std::shared_ptr<MamaJenniesBitmap> MamaJenniesBitmap::LogicalAND(std::vector<std::shared_ptr<
    MamaJenniesBitmap>>& bitmaps) {
  if (bitmaps.size() == 0) {
    return std::make_unique<MamaJenniesBitmap>();
  } else if (bitmaps.size() == 1) {
    return bitmaps[0];
  }
  // ok we have more than 1 bitmap
  std::shared_ptr<MamaJenniesBitmap> b1 = std::make_shared<MamaJenniesBitmap>();
  std::shared_ptr<MamaJenniesBitmap> b2 = std::make_shared<MamaJenniesBitmap>();

  MamaJenniesBitmap* combinedBitmap = bitmaps[0].get();
  MamaJenniesBitmap* outputBitmap = b2.get();
  bool flipper = true;

  for (int i = 1; i < bitmaps.size(); i++) {
    bitmaps[i]->LogicalAND(*combinedBitmap, *outputBitmap);
    flipper = !flipper; // will turn to false on 1st iteration
    combinedBitmap = flipper ? b1.get() : b2.get();
    outputBitmap = flipper ? b2.get() : b1.get();

    if (combinedBitmap->GetSizeInBits() == 0) {
      // No need to proceed further, the AND result will be an empty bitmap
      break;
    }
  }

  return flipper ? b1 : b2;
}

std::shared_ptr<MamaJenniesBitmap> MamaJenniesBitmap::LogicalOR(
    std::vector<std::shared_ptr<MamaJenniesBitmap>>& bitmaps) {
  if (bitmaps.size() == 0) {
    return std::make_unique<MamaJenniesBitmap>();
  } else if (bitmaps.size() == 1) {
    return bitmaps[0];
  }
  // ok we have more than 1 bitmap
  std::shared_ptr<MamaJenniesBitmap> b1 = std::make_shared<MamaJenniesBitmap>();
  std::shared_ptr<MamaJenniesBitmap> b2 = std::make_shared<MamaJenniesBitmap>();

  MamaJenniesBitmap* combinedBitmap = bitmaps[0].get();
  MamaJenniesBitmap* outputBitmap = b2.get();
  bool flipper = true;

  for (int i = 1; i < bitmaps.size(); i++) {
    bitmaps[i]->LogicalOR(*combinedBitmap, *outputBitmap);
    flipper = !flipper; // will turn to false on 1st iteration
    combinedBitmap = flipper ? b1.get() : b2.get();
    outputBitmap = flipper ? b2.get() : b1.get();
  }

  return flipper ? b1 : b2;
}

MamaJenniesBitmap::const_iterator MamaJenniesBitmap::begin() {
  auto iter = m_ewahBoolArray->begin();
  return MamaJenniesBitmapConstIterator(iter);
}

MamaJenniesBitmap::const_iterator MamaJenniesBitmap::end() {
  auto iter = m_ewahBoolArray->end();
  return MamaJenniesBitmapConstIterator(iter);
}

std::unique_ptr<MamaJenniesBitmap::const_iterator> MamaJenniesBitmap::begin_pointer() {
  auto iter = m_ewahBoolArray->begin();
  return std::make_unique<const_iterator>(iter);
}

std::unique_ptr<MamaJenniesBitmap::const_iterator> MamaJenniesBitmap::end_pointer() {
  auto iter = m_ewahBoolArray->end();
  return std::make_unique<const_iterator>(iter);
}

MamaJenniesBitmap::MamaJenniesBitmap(std::unique_ptr<EWAHBoolArray<std::uint64_t>> ewahBoolArray)
    : m_ewahBoolArray(std::move(ewahBoolArray)) {
}