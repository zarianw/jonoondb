#include "id_seq.h"

using namespace jonoondb_api;
using namespace gsl;

IDSequence::IDSequence(std::shared_ptr<MamaJenniesBitmap> bitmap, int vecSize) : 
  m_bitmap(move(bitmap)) {
  m_currentVector.resize(vecSize);
  m_currentSpan = span<std::uint64_t>(m_currentVector.data(), 0);
  m_iter = bitmap->begin_pointer();
  m_end = bitmap->end_pointer();
}

const span<std::uint64_t>& IDSequence::Current() {
  return m_currentSpan;
}

bool IDSequence::Next() {
  if (*(m_iter) >= *(m_end)) {
    m_currentSpan = span<std::uint64_t>(m_currentVector.data(), 0);
    return false;
  }

  int index = 0;
  while (index < m_currentVector.size() && *(m_iter) < *(m_end)) {
    m_currentVector[index] = m_iter->operator*();
    ++(*m_iter);
    index++;
  }

  m_currentSpan = span<std::uint64_t>(m_currentVector.data(), index);
  return true;
}
