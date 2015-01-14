#include <memory>
#include "index_manager.h"
#include "status.h"
#include "document.h"
#include "indexer.h"
#include "indexer_factory.h"
#include "index_info.h"

using namespace std;
using namespace jonoondb_api;

IndexManager::IndexManager(unique_ptr<vector<unique_ptr<Indexer>>> indexers) 
    : m_indexers (move(indexers)) {
}

Status IndexManager::Construct(const IndexInfo indexes[], size_t indexesLength,
                               std::unordered_map<std::string, ColumnType>& columnTypes,
                               IndexManager*& indexManager) {
  unique_ptr<vector<unique_ptr<Indexer>>> indexers;

  for (size_t i = 0; i < indexesLength; i++) {
    Indexer* indexer;
    auto sts = IndexerFactory::CreateIndexer(indexes[i], columnTypes, indexer);
    if (!sts.OK()) {
      return sts;
    }
    indexers->push_back(unique_ptr<Indexer>(indexer));
  }

  indexManager = new IndexManager(move(indexers));

  return Status();
}

Status IndexManager::CreateIndex(const IndexInfo& indexInfo,
  std::unordered_map<std::string, ColumnType>& columnTypes) {
  Indexer* indexer;
  auto sts = IndexerFactory::CreateIndexer(indexInfo, columnTypes, indexer);
  if (!sts.OK()) {
    return sts;
  }

  m_indexers->push_back(unique_ptr<Indexer>(indexer));
  return sts;
}

Status IndexManager::IndexDocument(const Document& document) {
  for (auto& indexer : *m_indexers) {
    Status sts = indexer->ValidateForInsert(document);
    if (!sts.OK()) {
      return sts;
    }
  }

  for (auto& indexer : *m_indexers) {
    indexer->Insert(document);
  }

  return Status();
}