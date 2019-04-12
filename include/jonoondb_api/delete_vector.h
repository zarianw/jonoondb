#pragma once

#include <set>
#include <cstdint>
#include "mama_jennies_bitmap.h"

namespace jonoondb_api {
class DocumentCollection;

class DeleteVector {
 public:
  DeleteVector(const DocumentCollection& parentCollection);
  void Add(uint64_t);
  const MamaJenniesBitmap& GetDeleteVectorBitmap();
  bool Empty();
 private:
  void MaybeReconstructBitmap();
  std::set<std::uint64_t> m_deletedDocIds;
  MamaJenniesBitmap m_deleteVecBitmap;
  bool m_isDirty;
  const DocumentCollection& m_parentCollection;
};
} // namespace jonoondb_api
