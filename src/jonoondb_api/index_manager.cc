#include "index_manager.h"
#include "status.h"

using namespace jonoondb_api;

Status IndexManager::Construct(const IndexInfo indexes[], int indexesLength, IndexManager*& indexManager) {
  return Status();
}

Status IndexManager::CreateIndex(const IndexInfo& indexInfo) {
  return Status();
}

Status IndexManager::IndexDocument(const document& document) {
  return Status();
}