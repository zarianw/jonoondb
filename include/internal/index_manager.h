#pragma once

#include <vector>
#include <memory>

namespace jonoondb_api {

// Foward declarations
class Status;
class IndexInfo;
class Indexer;
class document;

class IndexManager {
public:
  Status Construct(const IndexInfo indexes[], int indexesLength, IndexManager*& indexManager);
  Status CreateIndex(const IndexInfo& indexInfo);
  Status IndexDocument(const document& document);
private:
  std::vector<std::unique_ptr<Indexer>> m_indexers;
};
} // namespace jonoondb_api
