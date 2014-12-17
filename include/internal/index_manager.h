#pragma once

#include <vector>
#include <memory>
#include "indexer.h"

namespace jonoondb_api {
// Forward declarations
class Status;
class IndexInfo;
class Document;

class IndexManager {
public:
  IndexManager(std::unique_ptr<std::vector<std::unique_ptr<Indexer>>> indexers);
  static Status Construct(const IndexInfo indexes[], size_t indexesLength, IndexManager*& indexManager);
  Status CreateIndex(const IndexInfo& indexInfo);
  Status IndexDocument(const Document& document);
private:
  std::unique_ptr<std::vector<std::unique_ptr<Indexer>>> m_indexers;
};
} // namespace jonoondb_api
