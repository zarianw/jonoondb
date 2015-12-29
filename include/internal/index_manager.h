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
class IndexInfoImpl;
class Document;
class IndexStat;
enum class FieldType
: std::int8_t;
enum class IndexConstraintOperator
  : std::int8_t;
struct Constraint;

class IndexManager {
 public:
  typedef std::unordered_map<std::string, std::vector<std::unique_ptr<Indexer>>> ColumnIndexderMap;  
  
  IndexManager(const std::vector<IndexInfoImpl*>& indexes, const std::unordered_map<std::string, FieldType>& columnTypes);
  Status CreateIndex(const IndexInfoImpl& indexInfo, std::unordered_map<std::string, FieldType>& columnTypes);
  void IndexDocument(std::uint64_t documentID, const Document& document);
  bool TryGetBestIndex(const std::string& columnName, IndexConstraintOperator op,
    IndexStat& indexStat);
  std::shared_ptr<MamaJenniesBitmap> Filter(const std::vector<Constraint>& constraints);
private:
  std::unique_ptr<ColumnIndexderMap> m_columnIndexerMap;
};
}
  // namespace jonoondb_api
