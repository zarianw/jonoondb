#pragma once

#include <vector>
#include <memory>
#include <unordered_map>
#include <string>
#include <cstdint>
#include <mutex>
#include "indexer.h"

namespace jonoondb_api {
// Forward declarations
class IndexInfoImpl;
class Document;
class IndexStat;
enum class FieldType
: std::int8_t;
enum class IndexConstraintOperator
  : std::int8_t;
struct Constraint;
class DocumentIDGenerator;

class IndexManager {
 public:
  typedef std::unordered_map<std::string, std::vector<std::unique_ptr<Indexer>>> ColumnIndexderMap;  
  
  IndexManager(const std::vector<IndexInfoImpl*>& indexes, const std::unordered_map<std::string, FieldType>& columnTypes);
  void CreateIndex(const IndexInfoImpl& indexInfo, const std::unordered_map<std::string, FieldType>& columnTypes);
  std::uint64_t IndexDocument(DocumentIDGenerator& documentIDGenerator, const Document& document);
  // performValidation flag must only be set to false on startup because if we run into exception during startup
  // then we will not create Collection and consequently Database object. After startup we must set it to true for every call.
  std::uint64_t IndexDocuments(DocumentIDGenerator& documentIDGenerator,
                               const std::vector<std::unique_ptr<Document>>& documents,
                               bool performValidation = true);
  bool TryGetBestIndex(const std::string& columnName, IndexConstraintOperator op,
    IndexStat& indexStat);
  std::shared_ptr<MamaJenniesBitmap> Filter(const std::vector<Constraint>& constraints);
  bool TryGetIntegerValue(std::uint64_t documentID, const std::string& columnName, std::int64_t& val);
private:
  std::unique_ptr<ColumnIndexderMap> m_columnIndexerMap;
  std::mutex m_mutex;
};
}
  // namespace jonoondb_api
