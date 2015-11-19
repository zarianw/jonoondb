#include <memory>
#include <assert.h>
#include <sstream>
#include "index_manager.h"
#include "status.h"
#include "document.h"
#include "indexer.h"
#include "indexer_factory.h"
#include "index_info_impl.h"
#include "index_stat.h"
#include "constraint.h"
#include "mama_jennies_bitmap.h"

using namespace std;
using namespace jonoondb_api;

IndexManager::IndexManager(const std::vector<IndexInfoImpl*>& indexes,
  const std::unordered_map<std::string, FieldType>& columnTypes) :
  m_columnIndexerMap(new ColumnIndexderMap()) {
  for (size_t i = 0; i < indexes.size(); i++) {
    auto it = columnTypes.find(indexes[i]->GetColumnName());
    if (it == columnTypes.end()) {
      ostringstream ss;
      ss << "The field type for " << indexes[i]->GetColumnName()
        << " could not be determined.";
      throw JonoonDBException(ss.str(), __FILE__, "", __LINE__);
    }
    unique_ptr<Indexer> indexer(IndexerFactory::CreateIndexer(*indexes[i], it->second));
    (*m_columnIndexerMap)[indexes[i]->GetColumnName()].push_back(move(indexer));
  }
}

Status IndexManager::CreateIndex(const IndexInfoImpl& indexInfo,
  std::unordered_map<std::string, FieldType>& columnTypes) {
  auto it = columnTypes.find(indexInfo.GetColumnName());
  if (it == columnTypes.end()) {
    ostringstream ss;
    ss << "The field type for " << indexInfo.GetColumnName()
      << " could not be determined.";
    throw JonoonDBException(ss.str(), __FILE__, "", __LINE__);
  }
  unique_ptr<Indexer> indexer(IndexerFactory::CreateIndexer(indexInfo, it->second));  
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

std::shared_ptr<MamaJenniesBitmap> IndexManager::Filter(const std::vector<Constraint>& constraints) {
  std::shared_ptr<MamaJenniesBitmap> combinedBitmap;  
  for (auto& constraint : constraints) {
    auto columnIndexerIter = m_columnIndexerMap->find(constraint.columnName);
    if (columnIndexerIter == m_columnIndexerMap->end()) {
      std::ostringstream ss;
      ss << "Cannot apply filter operation on field " << constraint.columnName
        << " because no indexes exist on this field.";
      throw JonoonDBException(ss.str(), __FILE__, "", __LINE__);
    }
    // Todo: When we have different kinds of indexes, 
    // Add the logic to select the best index for the column  
    auto currBitmap = columnIndexerIter->second[0]->Filter(constraint);
    if (combinedBitmap == nullptr) {      
      combinedBitmap = currBitmap;
    } else {
      auto outputBitmap = std::make_shared<MamaJenniesBitmap>();
      currBitmap->LogicalAND(*combinedBitmap, *outputBitmap);
      combinedBitmap = outputBitmap;
    }

    if (combinedBitmap->GetSizeInBits() == 0) {
      break;
    }
  }

  return combinedBitmap;
}
