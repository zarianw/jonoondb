#include "jonoondb_api/delete_vector.h"
#include "jonoondb_api/document_collection.h"

using namespace jonoondb_api;

DeleteVector::DeleteVector(const DocumentCollection& parentCollection)
    : m_isDirty(false), m_parentCollection(parentCollection) {
}

void DeleteVector::Add(uint64_t docId) {
  m_deletedDocIds.insert(docId);
  m_isDirty = true;
}

const MamaJenniesBitmap& DeleteVector::GetDeleteVectorBitmap() {
  MaybeReconstructBitmap();
  return m_deleteVecBitmap;
}

bool DeleteVector::Empty() {
  return m_deletedDocIds.empty();
}

void DeleteVector::MaybeReconstructBitmap() {
  if (m_isDirty) {
    m_deleteVecBitmap.Reset();
    for (auto& id : m_deletedDocIds) {
      m_deleteVecBitmap.Add(id);
    }
    // Its important to add a id that is last_doc_id + 1 at the end of the bitmap
    // so that the size of the total length of bitmap is correct
    m_deleteVecBitmap.Add(m_parentCollection.GetDocumentCount());
    m_deleteVecBitmap.InPlaceLogicalNOT();
    m_isDirty = false;
  }
}
