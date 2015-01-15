#include <memory>
#include "mama_jennies_bitmap.h"

using namespace std;
using namespace jonoondb_api;

MamaJenniesBitmap::MamaJenniesBitmap()
    : m_ewahBoolArray(new EWAHBoolArray<size_t>()) {
}

MamaJenniesBitmap::MamaJenniesBitmap(MamaJenniesBitmap&& other) {
  if (this != &other) {
    m_ewahBoolArray.reset(other.m_ewahBoolArray.release());
  }
}

void MamaJenniesBitmap::Add(size_t x) {
  m_ewahBoolArray->set(x);
}

MamaJenniesBitmap MamaJenniesBitmap::And(const MamaJenniesBitmap& b1,
                                         const MamaJenniesBitmap& b2) {
  unique_ptr<EWAHBoolArray<size_t>> container(new EWAHBoolArray<size_t>());
  b1.m_ewahBoolArray->logicaland(*b2.m_ewahBoolArray, *container);
  return MamaJenniesBitmap(container.release());  // move ctor should be called here
}

MamaJenniesBitmap MamaJenniesBitmap::Or(const MamaJenniesBitmap& b1,
                                        const MamaJenniesBitmap& b2) {
  unique_ptr<EWAHBoolArray<size_t>> container(new EWAHBoolArray<size_t>());
  b1.m_ewahBoolArray->logicalor(*b2.m_ewahBoolArray, *container);
  return MamaJenniesBitmap(container.release());  // move ctor should be called here
}

MamaJenniesBitmap::MamaJenniesBitmap(EWAHBoolArray<size_t>* ewahBoolArray)
    : m_ewahBoolArray(ewahBoolArray) {
}

