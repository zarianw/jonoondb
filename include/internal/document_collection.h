#pragma once

#include <memory>
#include <unordered_map>
#include <string>
#include <cstdint>
#include "index_manager.h"

// Forward declaration
struct sqlite3;

namespace jonoondb_api {
// Forward Declaration
class Status;
class IndexInfo;
class Buffer;
class DocumentSchema;
enum class ColumnType
: std::int32_t;
enum class SchemaType
: std::int32_t;

class DocumentCollection {
 public:
  ~DocumentCollection();
  static Status Construct(const char* databaseMetadataFilePath,
                          const char* name, SchemaType schemaType,
                          const char* schema, const IndexInfo indexes[],
                          size_t indexesLength,
                          DocumentCollection*& documentCollection);

  Status Insert(const Buffer& documentData);

 private:
  explicit DocumentCollection(sqlite3* dbConnection,
                              std::unique_ptr<IndexManager> indexManager,
                              std::unique_ptr<DocumentSchema> documentSchema);
  static Status PopulateColumnTypes(
      const IndexInfo indexes[], size_t indexesLength,
      const DocumentSchema& documentSchema,
      std::unordered_map<std::string, ColumnType>& columnTypes);
  sqlite3* m_dbConnection;
  std::unique_ptr<IndexManager> m_indexManager;
  std::unique_ptr<DocumentSchema> m_documentSchema;
};
}  // namespace jonoondb_api

