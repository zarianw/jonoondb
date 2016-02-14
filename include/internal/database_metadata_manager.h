#pragma once

#include <string>
#include <cstdint>
#include <vector>

//Forward declarations
struct sqlite3;
struct sqlite3_stmt;

namespace jonoondb_api {
//Forward declaration
class IndexInfoImpl;
class FileInfo;
enum class SchemaType
: std::int32_t;

struct CollectionMetadata {
  std::string name;
  std::string schema;
  SchemaType schemaType;
  std::vector<IndexInfoImpl> indexes;
  std::vector<FileInfo> dataFiles;
};

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
                     const std::string& schema, const std::vector<IndexInfoImpl*>& indexes);
  const std::string& GetFullDBPath() const;
  const std::string& GetDBPath() const;
  const std::string& GetDBName() const;
  void GetExistingCollections(std::vector<CollectionMetadata>& collections);

 private:
  void CreateTables();
  void PrepareStatements();
  void CreateIndex(const std::string& collectionName, const IndexInfoImpl& indexInfo);
  void FinalizeStatements();

  std::string m_dbName;
  std::string m_dbPath;
  std::string m_fullDbPath;
  std::unique_ptr<sqlite3, void(*)(sqlite3*)> m_metadataDBConnection;
  sqlite3_stmt* m_insertCollectionSchemaStmt;
  sqlite3_stmt* m_insertCollectionIndexStmt;
};
}
