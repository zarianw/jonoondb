#pragma once

#include <vector>
#include "Indexer.h"

// Forward declaration
struct sqlite3;

namespace jonoondb_api {
// Forward Declaration
class Status;
class IndexInfo;
class Buffer;
class IndexManager;

class DocumentCollection {
 public:
  ~DocumentCollection();
  static Status Construct(const char* databaseMetadataFilePath, const char* name, int schemaType, const char* schema,
                          const IndexInfo indexes[], int indexesLength, DocumentCollection*& documentCollection);
  Status Insert(const Buffer& documentData);

 private:
  explicit DocumentCollection(sqlite3* dbConnection);
  sqlite3* m_dbConnection;
  std::unique_ptr<IndexManager> m_indexManager;
};
}  // namespace jonoondb_api

