#include "indexer_factory.h"
#include "status.h"
#include "index_info.h"
#include "enums.h"
#include "ewah_compressed_bitmap_indexer.h"

using namespace jonoondb_api;

Status IndexerFactory::CreateIndexer(const IndexInfo& indexInfo, Indexer*& indexer) {
  switch (indexInfo.GetType()) {
  case IndexType::EWAHCompressedBitmap:
    indexer = new EWAHCompressedBitmapIndexer(indexInfo);
    break;
  default:
    break;
  }

  return Status();
}