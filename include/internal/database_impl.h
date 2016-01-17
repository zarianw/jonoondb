#pragma once
#include <memory>
#include <unordered_map>
#include <string>
#include <cstdint>
#include "database_metadata_manager.h"
#include "document_collection.h"
#include "query_processor.h"
#include "options_impl.h"

namespace jonoondb_api {
//Forward Declarations
class BufferImpl;
class IndexInfoImpl;
class ResultSetImpl;
enum class SchemaType
: std::int32_t;

class DatabaseImpl final {
 public:
  DatabaseImpl(const std::string& dbPath, const std::string& dbName,
     const OptionsImpl& options);
  ~DatabaseImpl();
  DatabaseImpl(const DatabaseImpl&) = delete;
  DatabaseImpl(DatabaseImpl&&) = delete;
  DatabaseImpl& operator=(const DatabaseImpl&) = delete;
  
  void CreateCollection(const std::string& name, SchemaType schemaType,
                          const std::string& schema, const std::vector<IndexInfoImpl*>& indexes);
  void Insert(const char* collectionName, const BufferImpl& documentData);
  ResultSetImpl ExecuteSelect(const std::string& selectStatement);

 private:  
  std::unique_ptr<DatabaseMetadataManager> m_dbMetadataMgrImpl;
  std::unordered_map<std::string, std::shared_ptr<DocumentCollection>> m_collectionContainer;
  std::unique_ptr<QueryProcessor> m_queryProcessor;
  OptionsImpl m_options;
};

}  // jonoondb_api
