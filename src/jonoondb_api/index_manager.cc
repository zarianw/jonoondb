#include <memory>
#include <assert.h>
#include <sstream>
#include "index_manager.h"
#include "document.h"
#include "indexer.h"
#include "indexer_factory.h"
#include "index_info_impl.h"
#include "index_stat.h"
#include "constraint.h"
#include "mama_jennies_bitmap.h"
#include "document_id_generator.h"

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
      throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
    }
    unique_ptr<Indexer> indexer(IndexerFactory::CreateIndexer(*indexes[i], it->second));
    (*m_columnIndexerMap)[indexes[i]->GetColumnName()].push_back(move(indexer));
  }
}

void IndexManager::CreateIndex(const IndexInfoImpl& indexInfo,
  const std::unordered_map<std::string, FieldType>& columnTypes) {
  auto it = columnTypes.find(indexInfo.GetColumnName());
  if (it == columnTypes.end()) {
    ostringstream ss;
    ss << "The field type for " << indexInfo.GetColumnName()
      << " could not be determined.";
    throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }
  unique_ptr<Indexer> indexer(IndexerFactory::CreateIndexer(indexInfo, it->second));  
  (*m_columnIndexerMap)[indexInfo.GetColumnName()].push_back(move(indexer));  
}

void IndexManager::ValidateForIndexing(const std::vector<std::unique_ptr<Document>>& documents) {
  for (const auto& doc : documents) {
    for (const auto& columnIndexerMapPair : *m_columnIndexerMap) {
      for (const auto& indexer : columnIndexerMapPair.second) {
        indexer->ValidateForInsert(*doc);
      }
    }
  }
}

std::uint64_t IndexManager::IndexDocuments(DocumentIDGenerator& documentIDGenerator,
                                           const std::vector<std::unique_ptr<Document>>& documents) {
  std::uint64_t startID = 0;
  {
    std::unique_lock<std::mutex> lock(m_mutex);
    startID = documentIDGenerator.ReserveID(documents.size());
    auto documentID = startID;
    for (const auto& doc : documents) {
      for (const auto& columnIndexerMapPair : *m_columnIndexerMap) {
        for (const auto& indexer : columnIndexerMapPair.second) {
          indexer->Insert(documentID, *doc);
        }
      }
      ++documentID;
    }
  }

  return startID;
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
  std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
  for (std::size_t i = 0; i < constraints.size(); i++) {
    auto columnIndexerIter = m_columnIndexerMap->find(constraints[i].columnName);
    if (columnIndexerIter == m_columnIndexerMap->end()) {
      std::ostringstream ss;
      ss << "Cannot apply filter operation on field " << constraints[i].columnName
        << " because no indexes exist on this field.";
      throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
    }
    // Todo: When we have different kinds of indexes, 
    // Add the logic to select the best index for the column  
    
    // First lets see if we have range condition e.g. val > 10 AND val < 20
    // We look for adjacent constraints if they are on the same column and are
    // representing a range then we use FilterRange func instead which is more
    // optimized.    
    if (i + 1 < constraints.size() && constraints[i].columnName == constraints[i+1].columnName && 
        (constraints[i].op == IndexConstraintOperator::GREATER_THAN || constraints[i].op == IndexConstraintOperator::GREATER_THAN_EQUAL) &&
        (constraints[i+1].op == IndexConstraintOperator::LESS_THAN || constraints[i+1].op == IndexConstraintOperator::LESS_THAN_EQUAL)) {
      auto bm = columnIndexerIter->second[0]->FilterRange(constraints[i], constraints[i + 1]);
      bitmaps.push_back(bm);
      i++; // advance i because we have processed 2 constraints
    } else {
      // Todo: Uncomment the code below which is an optimization.
      // But first we need a fast way to check if bitmap is empty.
      /*if (bm->IsEmpty()) {
      // no need to proceed further as the AND opaeration will yield
      // an empty bitmap in the end
      bitmaps.clear();
      break;
      }*/
      auto bm = columnIndexerIter->second[0]->Filter(constraints[i]);
      bitmaps.push_back(bm);
    }     
  }

  return MamaJenniesBitmap::LogicalAND(bitmaps);  
}

bool IndexManager::TryGetIntegerValue(std::uint64_t documentID,
                                      const std::string& columnName,
                                      std::int64_t& val) {
  auto columnIndexerIter = m_columnIndexerMap->find(columnName);
  if (columnIndexerIter != m_columnIndexerMap->end()) {
    for (auto& indexer : columnIndexerIter->second) {
      if (indexer->GetIndexStats().GetIndexInfo().GetType() == IndexType::VECTOR) {
        return indexer->TryGetIntegerValue(documentID, val);        
      }
    }
  }

  return false;
}

bool IndexManager::TryGetIntegerVector(
    const gsl::span<std::uint64_t>& documentIDs,
    const std::string & columnName,
    std::vector<std::int64_t>& values) {
  auto columnIndexerIter = m_columnIndexerMap->find(columnName);
  if (columnIndexerIter != m_columnIndexerMap->end()) {
    for (auto& indexer : columnIndexerIter->second) {
      if (indexer->GetIndexStats().GetIndexInfo().GetType() == IndexType::VECTOR) {
        return indexer->TryGetIntegerVector(documentIDs, values);
      }
    }
  }

  return false;
}

bool IndexManager::TryGetDoubleValue(std::uint64_t documentID,
                                     const std::string& columnName,
                                     double& val) {
  auto columnIndexerIter = m_columnIndexerMap->find(columnName);
  if (columnIndexerIter != m_columnIndexerMap->end()) {
    for (auto& indexer : columnIndexerIter->second) {
      if (indexer->GetIndexStats().GetIndexInfo().GetType() == IndexType::VECTOR) {
        return indexer->TryGetDoubleValue(documentID, val);        
      }
    }
  }

  return false;
}

bool IndexManager::TryGetStringValue(
    std::uint64_t documentID, const std::string& columnName,
    std::string& val) {
  auto columnIndexerIter = m_columnIndexerMap->find(columnName);
  if (columnIndexerIter != m_columnIndexerMap->end()) {
    for (auto& indexer : columnIndexerIter->second) {
      if (indexer->GetIndexStats().GetIndexInfo().GetType() == IndexType::VECTOR) {
        return indexer->TryGetStringValue(documentID, val);
      }
    }
  }

  return false;
}
