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
class ResultSet;
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
  Status CreateCollection(const char* name, SchemaType schemaType,
                          const char* schema, const IndexInfo indexes[],
                          size_t indexesLength);
  Status Insert(const char* collectionName, const Buffer& documentData);
  Status ExecuteSelect(const char* selectStatement, ResultSet*& resultSet);

 private:
  DatabaseImpl(
      std::unique_ptr<DatabaseMetadataManager> databaseMetadataManager,
      std::unique_ptr<QueryProcessor> queryProcessor);
  std::unique_ptr<DatabaseMetadataManager> m_dbMetadataMgrImpl;
  std::unordered_map<std::string, std::shared_ptr<DocumentCollection>> m_collectionContainer;
  std::unique_ptr<QueryProcessor> m_queryProcessor;
};

}  // jonoondb_api
