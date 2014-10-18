#pragma once

#include <string>

//Forward declarations
struct sqlite3;
struct sqlite3_stmt;

namespace jonoondb_api {
//Forward declaration
class Status;
class IndexInfo;

class DatabaseMetadataManager {
 public:
  static Status Open(const char* dbPath, const char* dbName,
                     bool createDBIfMissing,
                     DatabaseMetadataManager*& databaseMetadataManager);

  Status AddCollection(const char* name, int schemaType, const char* schema,
                       const IndexInfo indexes[], int indexesLength);

  ~DatabaseMetadataManager();
 private:
  DatabaseMetadataManager(const char* dbPath, const char* dbName,
                          bool createDBIfMissing);
  Status Initialize();
  Status CreateTables();
  Status PrepareStatements();
  Status CreateIndex(const char* collectionName, const IndexInfo& indexInfo);

  std::string m_dbName;
  std::string m_dbPath;
  sqlite3* m_metadataDBConnection;
  sqlite3_stmt* m_insertCollectionSchemaStmt;
  sqlite3_stmt* m_insertCollectionIndexStmt;
  bool m_createDBIfMissing;
};
}

