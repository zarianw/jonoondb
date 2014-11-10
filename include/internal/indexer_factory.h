#pragma once
namespace jonoondb_api {
  // Forward declarations
  class Status;
  class IndexInfo;
  class Indexer;

class IndexerFactory
{
public:
  static Status CreateIndexer(const IndexInfo& indexInfo, Indexer*& indexer);
private:
  IndexerFactory() = delete;
  IndexerFactory(const IndexerFactory&) = delete;
  IndexerFactory(IndexerFactory&&) = delete;
};
}

