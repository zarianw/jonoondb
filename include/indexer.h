#pragma once

namespace jonoondb_api {

// Forward declarations
class Status;
class IndexInfo;
class Document;

class Indexer {
public:
  Status Insert(const Document& document);
private:
  Indexer(const IndexInfo& indexInfo);  
};
}