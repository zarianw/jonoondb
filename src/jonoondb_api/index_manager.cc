#include <memory>
#include "index_manager.h"
#include "status.h"
#include "document.h"
#include "indexer.h"

using namespace std;
using namespace jonoondb_api;

IndexManager::IndexManager(const IndexInfo indexes[], int indexesLength) {
}

Status IndexManager::Construct(const IndexInfo indexes[], size_t indexesLength, IndexManager*& indexManager) {
  indexManager = new IndexManager(indexes, indexesLength);
  return Status();
}

Status IndexManager::CreateIndex(const IndexInfo& indexInfo) {
  return Status();
}

Status IndexManager::IndexDocument(const Document& document) {
  for (auto& indexer : m_indexers) {
    Status sts = indexer->ValidateForInsert(document);
    if (!sts.OK()) {
      return sts;
    }
  }

  for (auto& indexer : m_indexers) {
    indexer->Insert(document);
  }

  return Status();
}