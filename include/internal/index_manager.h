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
enum class FieldType
: std::int32_t;

class IndexManager {
 public:
  IndexManager(std::unique_ptr<std::vector<std::unique_ptr<Indexer>>>indexers);
  static Status Construct(const IndexInfo indexes[], size_t indexesLength,
  std::unordered_map<std::string, FieldType>& columnTypes,
  IndexManager*& indexManager);
  Status CreateIndex(const IndexInfo& indexInfo,
  std::unordered_map<std::string, FieldType>& columnTypes);
  Status IndexDocument(std::uint64_t documentID, const Document& document);
private:
  std::unique_ptr<std::vector<std::unique_ptr<Indexer>>> m_indexers;
};
}
  // namespace jonoondb_api
