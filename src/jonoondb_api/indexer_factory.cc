#include "indexer_factory.h"
#include "status.h"
#include "index_info.h"
#include "enums.h"

using namespace jonoondb_api;

Status IndexerFactory::CreateIndexer(const IndexInfo& indexInfo, Indexer*& indexer) {
  switch (indexInfo.GetType()) {
  case IndexType::WAHCompressedBitmap:
    break;
  default:
    break;
  }

  return Status();
}