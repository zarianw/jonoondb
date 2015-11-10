#include <memory>
#include <assert.h>
#include "index_manager.h"
#include "status.h"
#include "document.h"
#include "indexer.h"
#include "indexer_factory.h"
#include "index_info_impl.h"
#include "index_stat.h"

using namespace std;
using namespace jonoondb_api;

IndexManager::IndexManager(const std::vector<IndexInfoImpl*>& indexes, const std::unordered_map<std::string, FieldType>& columnTypes) :
    m_columnIndexerMap(new ColumnIndexderMap()) {
  for (size_t i = 0; i < indexes.size(); i++) {    
    unique_ptr<Indexer> indexer(IndexerFactory::CreateIndexer(*indexes[i], columnTypes));
    (*m_columnIndexerMap)[indexes[i]->GetColumnName()].push_back(move(indexer));
  }
}

Status IndexManager::CreateIndex(const IndexInfoImpl& indexInfo,
                                 std::unordered_map<std::string, FieldType>& columnTypes) {
  unique_ptr<Indexer> indexer(IndexerFactory::CreateIndexer(indexInfo, columnTypes));  
  (*m_columnIndexerMap)[indexInfo.GetColumnName()].push_back(move(indexer));
  return Status();
}

Status IndexManager::IndexDocument(uint64_t documentID,
                                   const Document& document) {
  for (const auto& columnIndexerMapPair : *m_columnIndexerMap) {
    for (const auto& indexer : columnIndexerMapPair.second) {
      indexer->ValidateForInsert(document);      
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
