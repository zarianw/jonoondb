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

void Database::Close() {
  m_databaseImpl->Close();
  delete this;
}

Status Database::Insert(const char* collectionName,
                        const Buffer& documentData) {
  return m_databaseImpl->Insert(collectionName, documentData);
}

Status Database::ExecuteSelect(const char* selectStatement, ResultSet*& resultSet) {
  return m_databaseImpl->ExecuteSelect(selectStatement, resultSet);
}

