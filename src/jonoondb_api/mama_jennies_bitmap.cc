#include <memory>
#include "mama_jennies_bitmap.h"

using namespace jonoondb_api;

MamaJenniesBitmap::MamaJenniesBitmap()
    : m_ewahBoolArray(std::make_unique<EWAHBoolArray<size_t>>()) {
}

MamaJenniesBitmap::MamaJenniesBitmap(MamaJenniesBitmap&& other) {
  if (this != &other) {
    m_ewahBoolArray.reset(other.m_ewahBoolArray.release());
  }
}

void MamaJenniesBitmap::Add(std::size_t x) {
  m_ewahBoolArray->set(x);  
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

