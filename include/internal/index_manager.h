#pragma once

#include <vector>
#include <memory>
#include <Indexer.h>

namespace jonoondb_api {

// Foward declarations
class Status;
class IndexInfo;
class Indexer;
class Document;

class IndexManager {
public:
  IndexManager(const IndexInfo indexes[], int indexesLength);
  static Status Construct(const IndexInfo indexes[], int indexesLength, IndexManager*& indexManager);
  Status CreateIndex(const IndexInfo& indexInfo);
  Status IndexDocument(const Document& document);
private:
  std::vector<std::unique_ptr<Indexer>> m_indexers;
};
} // namespace jonoondb_api
