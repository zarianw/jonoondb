#include <memory>
#include <sstream>
#include "mama_jennies_bitmap.h"
#include "jonoondb_exceptions.h"

using namespace jonoondb_api;

MamaJenniesBitmap::MamaJenniesBitmap()
    : m_ewahBoolArray(std::make_unique<EWAHBoolArray<size_t>>()) {
}

MamaJenniesBitmap::MamaJenniesBitmap(MamaJenniesBitmap&& other) {
  if (this != &other) {
    m_ewahBoolArray.reset(other.m_ewahBoolArray.release());
  }
}

MamaJenniesBitmap::MamaJenniesBitmap(const MamaJenniesBitmap& other) {
  if (this != &other) {
    // Lets call copy ctor of EWAHBoolArray
    m_ewahBoolArray = std::make_unique<EWAHBoolArray<size_t>>(*other.m_ewahBoolArray);
  }
}

MamaJenniesBitmap& MamaJenniesBitmap::operator=(const MamaJenniesBitmap& other) {
  if (this != &other) {
    // Lets call copy ctor of EWAHBoolArray
    m_ewahBoolArray = std::make_unique<EWAHBoolArray<size_t>>(*other.m_ewahBoolArray);
  }
  return *this;
}

MamaJenniesBitmap& MamaJenniesBitmap::operator=(MamaJenniesBitmap&& other) {
  if (this != &other) {
    m_ewahBoolArray.reset(other.m_ewahBoolArray.release());
  }
  return *this;
}

void MamaJenniesBitmap::Add(std::size_t x) {
  if (!m_ewahBoolArray->set(x)) {
    throw JonoonDBException("Add to bitmap failed. Most probably the entries were not added in increasing order.",
      __FILE__, "", __LINE__);
  }
}

bool MamaJenniesBitmap::IsEmpty() {
  // Todo: Need to find a faster way to check for empty bitmap
  bool isEmpty = true;
  if (m_ewahBoolArray->sizeInBits() > 0) {    
    for (auto iter = m_ewahBoolArray->begin(); iter != m_ewahBoolArray->end(); iter++) {
      isEmpty = false;
      break;
    }     
  }

  return isEmpty;
}

std::size_t MamaJenniesBitmap::GetSizeInBits() {
  return m_ewahBoolArray->sizeInBits();
}

void MamaJenniesBitmap::LogicalAND(const MamaJenniesBitmap& other, MamaJenniesBitmap& output) {
  m_ewahBoolArray->logicaland(*other.m_ewahBoolArray, *output.m_ewahBoolArray);  
 
}

MamaJenniesBitmap MamaJenniesBitmap::LogicalAND(const MamaJenniesBitmap& b1,
                                         const MamaJenniesBitmap& b2) {
  auto container = std::make_unique<EWAHBoolArray<std::size_t>>();
  b1.m_ewahBoolArray->logicaland(*b2.m_ewahBoolArray, *container);
  return MamaJenniesBitmap(std::move(container));  // move ctor should be called here
}

MamaJenniesBitmap MamaJenniesBitmap::LogicalOR(const MamaJenniesBitmap& b1,
                                        const MamaJenniesBitmap& b2) {
  auto container = std::make_unique<EWAHBoolArray<std::size_t>>();
  b1.m_ewahBoolArray->logicalor(*b2.m_ewahBoolArray, *container);
  return MamaJenniesBitmap(std::move(container));  // move ctor should be called here
}

MamaJenniesBitmap::MamaJenniesBitmap(std::unique_ptr<EWAHBoolArray<std::size_t>> ewahBoolArray)
    : m_ewahBoolArray(std::move(ewahBoolArray)) {
}

std::shared_ptr<MamaJenniesBitmap> MamaJenniesBitmap::LogicalAnd(std::vector<std::shared_ptr<MamaJenniesBitmap>>& bitmaps) {
  if (bitmaps.size() == 0) {
    return std::make_unique<MamaJenniesBitmap>();
  } else if (bitmaps.size() == 1) {
    return bitmaps[0];
  }
  // ok we have more than 1 bitmap
  std::shared_ptr<MamaJenniesBitmap> b1 = std::make_shared<MamaJenniesBitmap>();
  std::shared_ptr<MamaJenniesBitmap> b2 = std::make_shared<MamaJenniesBitmap>();

  MamaJenniesBitmap& combinedBitmap = *bitmaps[0];
  MamaJenniesBitmap& outputBitmap = *b2;
  bool flipper = true;

  for (int i = 1; i < bitmaps.size(); i++) {
    bitmaps[i]->LogicalAND(combinedBitmap, outputBitmap);
    flipper = !flipper; // will turn to false on 1st iteration
    combinedBitmap = flipper ? *b1 : *b2;
    outputBitmap = flipper ? *b2 : *b1;

    if (combinedBitmap.GetSizeInBits() == 0) {
      // No need to proceed further, the AND result will be an empty bitmap
      break;
    }
  }

  return flipper ? b1 : b2;  
}