#include "ewah_compressed_bitmap_indexer.h"
#include "status.h"
#include "document.h"

using namespace jonoondb_api;

EWAHCompressedBitmapIndexer::EWAHCompressedBitmapIndexer(const IndexInfo& indexInfo)
    : m_indexInfo (indexInfo) {
}
EWAHCompressedBitmapIndexer::~EWAHCompressedBitmapIndexer() {
}

Status EWAHCompressedBitmapIndexer::ValidateForInsert(const Document& document) {
  return Status();
}

void EWAHCompressedBitmapIndexer::Insert(const Document& document) {
}



