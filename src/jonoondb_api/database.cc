#include "database.h"
#include "status.h"
#include "database_impl.h"
#include "buffer.h"
#include "enums.h"

using namespace jonoondb_api;

Database::Database(DatabaseImpl* databaseImpl)
    : m_databaseImpl(databaseImpl) {
}

void Database::Open(const std::string& dbPath, const std::string& dbName,
                      const Options& options, Database*& db) {
  DatabaseImpl* dbImpl = DatabaseImpl::Open(dbPath, dbName, options);
  
  db = new Database(dbImpl); 
}

Status Database::Close() {
  Status status = m_databaseImpl->Close();
  delete this;
  return status;
}

Status Database::CreateCollection(const char* name, SchemaType schemaType,
                                  const char* schema, const IndexInfo indexes[],
                                  int indexesLength) {
  return m_databaseImpl->CreateCollection(name, schemaType, schema, indexes,
                                          indexesLength);
}

Status Database::Insert(const char* collectionName,
                        const Buffer& documentData) {
  return m_databaseImpl->Insert(collectionName, documentData);
}

Status Database::ExecuteSelect(const char* selectStatement, ResultSet*& resultSet) {
  return m_databaseImpl->ExecuteSelect(selectStatement, resultSet);
}

