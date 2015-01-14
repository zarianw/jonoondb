#pragma once

#include <memory>
#include <unordered_map>
#include <string>
#include "index_manager.h"

// Forward declaration
struct sqlite3;

namespace jonoondb_api {
// Forward Declaration
class Status;
class IndexInfo;
class Buffer;
enum class ColumnType : std::int16_t;

class DocumentCollection {
 public:
  ~DocumentCollection();
  static Status Construct(const char* databaseMetadataFilePath, const char* name, int schemaType, const char* schema,
                          const IndexInfo indexes[], size_t indexesLength, DocumentCollection*& documentCollection);
  Status Insert(const Buffer& documentData);

 private:
  explicit DocumentCollection(sqlite3* dbConnection, std::unique_ptr<IndexManager> indexManager);
  static Status PopulateColumnTypes(const IndexInfo indexes[], size_t indexesLength,
    std::unordered_map<std::string, ColumnType>& columnTypes);
  sqlite3* m_dbConnection;
  std::unique_ptr<IndexManager> m_indexManager;
};
}  // namespace jonoondb_api

