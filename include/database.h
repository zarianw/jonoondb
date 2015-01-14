#pragma once

#include <memory>
#include <cstdint>

namespace jonoondb_api {
//Forward Declarations
class DatabaseImpl;
class Status;
class Options;
class IndexInfo;
class Buffer;
enum class SchemaType : std::int32_t;

class Database {
public:
  static Status Open(const char* dbPath, const char* dbName,
                     const Options& options, Database*& db);
  Status Close();
  Status CreateCollection(const char* name, SchemaType schemaType, const char* schema,
                          const IndexInfo indexes[], int indexesLength);
  Status Insert(const char* collectionName, Buffer& documentData);  

private:
  Database(DatabaseImpl* databaseImpl);
  std::unique_ptr<DatabaseImpl> m_databaseImpl;
};

}  // jonoondb_api
