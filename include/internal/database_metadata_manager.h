#pragma once

#include <string>
#include <cstdint>

//Forward declarations
struct sqlite3;
struct sqlite3_stmt;

namespace jonoondb_api {
//Forward declaration
class Status;
class IndexInfo;
enum class SchemaType
: std::int32_t;

class DatabaseMetadataManager final {
 public:
   DatabaseMetadataManager(const DatabaseMetadataManager&) = delete;
   DatabaseMetadataManager(DatabaseMetadataManager&&) = delete;
   DatabaseMetadataManager& operator=(const DatabaseMetadataManager&) = delete;
   DatabaseMetadataManager(const std::string& dbPath,
                           const std::string& dbName,
                           bool createDBIfMissing);
  ~DatabaseMetadataManager();
  void AddCollection(const std::string& name, SchemaType schemaType,
                       const std::string& schema, const std::vector<IndexInfo*>& indexes);
  const char* GetFullDBPath() const;

 private:
  
  void Initialize(bool createDBIfMissing);
  void CreateTables();
  void PrepareStatements();
  Status CreateIndex(const char* collectionName, const IndexInfo& indexInfo);

  std::string m_dbName;
  std::string m_dbPath;
  std::string m_fullDbPath;
  sqlite3* m_metadataDBConnection;
  sqlite3_stmt* m_insertCollectionSchemaStmt;
  sqlite3_stmt* m_insertCollectionIndexStmt;
};
}

