#pragma once

#include <memory>
#include <cstdint>
#include <string>
#include <cdatabase.h>

namespace jonoondb_api {
//Forward Declarations
class DatabaseImpl;
class Status;
class Options;
class IndexInfo;
class Buffer;
class ResultSet;
enum class SchemaType
: std::int32_t;

class Database {
 public:
  static void Open(const std::string& dbPath, const std::string& dbName,
                     const Options& options, Database*& db);  
  void Close();
  Status CreateCollection(const char* name, SchemaType schemaType,
                          const char* schema, const IndexInfo indexes[],
                          int indexesLength);
  Status Insert(const char* collectionName, const Buffer& documentData);
  Status ExecuteSelect(const char* selectStatement, ResultSet*& resultSet);

 private:
  Database(DatabaseImpl* databaseImpl);
  std::unique_ptr<DatabaseImpl> m_databaseImpl;
};

}  // jonoondb_api
