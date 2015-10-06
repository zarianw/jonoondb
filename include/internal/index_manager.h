#pragma once

#include <vector>
#include <memory>
#include <unordered_map>
#include <string>
#include <cstdint>
#include "indexer.h"

namespace jonoondb_api {
// Forward declarations
class Status;
class IndexInfo;
class Document;
class IndexStat;
enum class FieldType
: std::int32_t;
enum class IndexConstraintOperator
  : std::int32_t;

class IndexManager {
 public:
   typedef std::unordered_map<std::string, std::vector<std::unique_ptr<Indexer>>> ColumnIndexderMap;
  
  IndexManager(std::unique_ptr<ColumnIndexderMap> indexers);
  static Status Construct(const IndexInfo indexes[], size_t indexesLength,
  std::unordered_map<std::string, FieldType>& columnTypes,
  IndexManager*& indexManager);
  Status CreateIndex(const IndexInfo& indexInfo,
  std::unordered_map<std::string, FieldType>& columnTypes);
  Status IndexDocument(std::uint64_t documentID, const Document& document);
  bool TryGetBestIndex(const std::string& columnName, IndexConstraintOperator op,
    IndexStat& indexStat);
private:
  std::unique_ptr<ColumnIndexderMap> m_columnIndexerMap;
};
}
  // namespace jonoondb_api
