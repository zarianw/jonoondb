#pragma once

#include <memory>
#include <unordered_map>
#include <string>
#include "database_metadata_manager.h"
#include "document_collection.h"

namespace jonoondb_api {
//Forward Declarations
class Status;
class Options;
class Buffer;
class IndexInfo;


class DatabaseImpl {
 public:
  static Status Open(const char* dbPath, const char* dbName,
                     const Options& options, DatabaseImpl*& db);
  Status Close();
  Status CreateCollection(const char* name, int schemaType, const char* schema, const IndexInfo indexes[],
    size_t indexesLength);
  Status Insert(const char* collectionName, const Buffer& documentData);

 private:
  DatabaseImpl(std::unique_ptr<DatabaseMetadataManager> databaseMetadataManager);
  std::unique_ptr<DatabaseMetadataManager> m_dbMetadataMgrImpl;
  std::unordered_map<std::string, std::unique_ptr<DocumentCollection>> m_collectionContainer;

};

}  // jonoondb_api
