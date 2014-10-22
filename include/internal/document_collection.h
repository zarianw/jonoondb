#pragma once

// Forward declaration
struct sqlite3;

namespace jonoondb_api {
// Forward Declaration
class Status;
class IndexInfo;
class Buffer;

class DocumentCollection {
 public:
  ~DocumentCollection();
  static Status Construct(const char* databaseMetadataFilePath, const char* name, int schemaType, const char* schema,
                          const IndexInfo indexes[], int indexesLength, DocumentCollection*& documentCollection);
  Status Insert(const Buffer& documentData);

 private:
  explicit DocumentCollection(sqlite3* dbConnection);
  sqlite3* m_dbConnection;
};
}  // namespace jonoondb_api

