#pragma once

namespace jonoondb_api {
//Forward Declarations
class Status;
class Options;
class DatabaseMetadataManager;
class Buffer;

class DatabaseImpl {
 public:
  ~DatabaseImpl();

  static Status Open(const char* dbPath, const char* dbName,
                     const Options& options, DatabaseImpl** db);
  Status CreateCollection(const char* name, int schemaType, const char* schema);
  Status Insert(const char* collectionName, Buffer& documentData);

 private:
  DatabaseImpl();
  DatabaseMetadataManager* m_databaseImpl;
};

}  // jonoondb_api
