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
class BufferImpl;

class IndexManager {
 public:
  typedef std::unordered_map<std::string, std::vector<std::unique_ptr<Indexer>>>
      ColumnIndexderMap;

  IndexManager(const std::vector<IndexInfoImpl*>& indexes,
               const std::unordered_map<std::string, FieldType>& columnTypes);
  void CreateIndex(const IndexInfoImpl& indexInfo,
                   const std::unordered_map<std::string,
                                            FieldType>& columnTypes);
  std::uint64_t IndexDocuments(DocumentIDGenerator& documentIDGenerator,
                               const std::vector<std::unique_ptr<Document>>& documents);
  bool
      TryGetBestIndex(const std::string& columnName, IndexConstraintOperator op,
                      IndexStat& indexStat);
  std::shared_ptr<MamaJenniesBitmap>
      Filter(const std::vector<Constraint>& constraints);
  bool TryGetIntegerValue(std::uint64_t documentID,
                          const std::string& columnName,
                          std::int64_t& val);
  bool TryGetDoubleValue
      (std::uint64_t documentID, const std::string& columnName, double& val);
  bool TryGetStringValue(std::uint64_t documentID,
                         const std::string& columnName,
                         std::string& val);

  bool TryGetBlobValue(std::uint64_t documentID,
                       const std::string& columnName,
                       BufferImpl& val);

  bool TryGetIntegerVector(const gsl::span<std::uint64_t>& documentIDs,
                           const std::string& columnName,
                           std::vector<std::int64_t>& values);
  bool TryGetDoubleVector(const gsl::span<std::uint64_t>& documentIDs,
                          const std::string& columnName,
                          std::vector<double>& values);

 private:
  std::unique_ptr<ColumnIndexderMap> m_columnIndexerMap;
  std::mutex m_mutex;
};
}
// namespace jonoondb_api
