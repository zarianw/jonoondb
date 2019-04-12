#pragma once

#include <set>
#include <cstdint>
#include "mama_jennies_bitmap.h"

namespace jonoondb_api {
class DocumentCollection;

class DeleteVector final {
 public:
  DeleteVector(const DocumentCollection& parentCollection);
  DeleteVector(const DeleteVector&) = delete;
  DeleteVector(DeleteVector&&) = delete;
  DeleteVector& operator=(const DeleteVector&) = delete;
  DeleteVector& operator=(DeleteVector&&) = delete;
  void Add(uint64_t);
  const MamaJenniesBitmap& GetDeleteVectorBitmap();
  bool Empty();
 private:
  void MaybeReconstructBitmap();
  std::set<std::uint64_t> m_deletedDocIds;
  MamaJenniesBitmap m_deleteVecBitmap;
  bool m_isDirty;
  // m_parentCollection reference is ok because DeleteVector is a child of
  // parent collection. DeleteVector should not be used in other way where
  // the life of m_parentCollection reference is less than DeleteVector
  const DocumentCollection& m_parentCollection;
};
} // namespace jonoondb_api
