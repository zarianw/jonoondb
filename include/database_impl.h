#pragma once
#include <memory>
#include <unordered_map>
#include <string>
#include <cstdint>
#include "database_metadata_manager.h"
#include "document_collection.h"
#include "query_processor.h"

namespace jonoondb_api {
//Forward Declarations
class Status;
class Options;
class Buffer;
class IndexInfo;
class ResultSetImpl;
enum class SchemaType
: std::int32_t;

class DatabaseImpl final {
 public:
  DatabaseImpl(const DatabaseImpl&) = delete;
  DatabaseImpl(DatabaseImpl&&) = delete;
  DatabaseImpl& operator=(const DatabaseImpl&) = delete;
  static DatabaseImpl* Open(const std::string& dbPath, const std::string& dbName,
                      const Options& options);
  void Close();
  void CreateCollection(const std::string& name, SchemaType schemaType,
                          const std::string& schema, const std::vector<IndexInfo*>& indexes);
  Status Insert(const char* collectionName, const Buffer& documentData);
  ResultSetImpl ExecuteSelect(const std::string& selectStatement);

 private:
  DatabaseImpl(
      std::unique_ptr<DatabaseMetadataManager> databaseMetadataManager,
      std::unique_ptr<QueryProcessor> queryProcessor);
  std::unique_ptr<DatabaseMetadataManager> m_dbMetadataMgrImpl;
  std::unordered_map<std::string, std::shared_ptr<DocumentCollection>> m_collectionContainer;
  std::unique_ptr<QueryProcessor> m_queryProcessor;
};

}  // jonoondb_api
