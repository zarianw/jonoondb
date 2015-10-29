#include <memory>
#include <assert.h>
#include "index_manager.h"
#include "status.h"
#include "document.h"
#include "indexer.h"
#include "indexer_factory.h"
#include "index_info.h"
#include "index_stat.h"

using namespace std;
using namespace jonoondb_api;

IndexManager::IndexManager(std::unique_ptr<ColumnIndexderMap> columnIndexderMap)
  : m_columnIndexerMap(move(columnIndexderMap)) {
}

Status IndexManager::Construct(
    const IndexInfo indexes[], size_t indexesLength,
    std::unordered_map<std::string, FieldType>& columnTypes,
    IndexManager*& indexManager) {
  std::unique_ptr<ColumnIndexderMap> columnIndexerMap(new ColumnIndexderMap());

  for (size_t i = 0; i < indexesLength; i++) {    
    Indexer* indexer;
    auto sts = IndexerFactory::CreateIndexer(indexes[i], columnTypes, indexer);
    if (!sts.OK()) {
      return sts;
    }
    
    std::string columnName = indexes[i].GetColumnName();
    (*columnIndexerMap)[columnName].push_back(unique_ptr<Indexer>(indexer));
  }

  indexManager = new IndexManager(move(columnIndexerMap));

  return Status();
}

Status IndexManager::CreateIndex(
    const IndexInfo& indexInfo,
    std::unordered_map<std::string, FieldType>& columnTypes) {
  Indexer* indexer;
  auto sts = IndexerFactory::CreateIndexer(indexInfo, columnTypes, indexer);
  if (!sts.OK()) {
    return sts;
  }

  std::string columnName = indexInfo.GetColumnName();
  (*m_columnIndexerMap)[columnName].push_back(unique_ptr<Indexer>(indexer));

  return sts;
}

Status IndexManager::IndexDocument(uint64_t documentID,
                                   const Document& document) {
  for (const auto& columnIndexerMapPair : *m_columnIndexerMap) {
    for (const auto& indexer : columnIndexerMapPair.second) {
      Status sts = indexer->ValidateForInsert(document);
      if (!sts.OK()) {
        return sts;
      }
    }
  }

  for (const auto& columnIndexerMapPair : *m_columnIndexerMap) {
    for (const auto& indexer : columnIndexerMapPair.second) {
      indexer->Insert(documentID, document);
    }
  }

  return Status();
}

bool IndexManager::TryGetBestIndex(const std::string& columnName, IndexConstraintOperator op,
  IndexStat& indexStat) {
  auto columnIndexerIter = m_columnIndexerMap->find(columnName);
  if (columnIndexerIter == m_columnIndexerMap->end()) {
    return false;
  }

  assert(columnIndexerIter->second.size() > 0);
  // Todo: When we have different kinds of indexes, 
  // Add the logic to select the best index for the column  
  indexStat = columnIndexerIter->second[0]->GetIndexStats();
  return true;
}
