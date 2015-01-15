#include <string>
#include <stdio.h>
#include <sstream>
#include "database_impl.h"
#include "status.h"
#include "database_metadata_manager.h"
#include "options.h"
#include "string_utils.h"
#include "index_manager.h"
#include "document_collection.h"
#include "enums.h"

using namespace std;
using namespace jonoondb_api;

DatabaseImpl::DatabaseImpl(
    unique_ptr<DatabaseMetadataManager> databaseMetadataManager)
    : m_dbMetadataMgrImpl(move(databaseMetadataManager)) {
}

Status DatabaseImpl::Open(const char* dbPath, const char* dbName,
                          const Options& options, DatabaseImpl*& db) {
  // Initialize DatabaseMetadataManager
  DatabaseMetadataManager* databaseMetadataManagerPtr;
  Status status = DatabaseMetadataManager::Open(dbPath, dbName,
                                                options.GetCreateDBIfMissing(),
                                                databaseMetadataManagerPtr);
  if (!status.OK()) {
    return status;
  }
  unique_ptr<DatabaseMetadataManager> databaseMetadataManager(
      databaseMetadataManagerPtr);

  db = new DatabaseImpl(move(databaseMetadataManager));

  return status;
}

Status DatabaseImpl::Close() {
  // Todo (zarian): Close all sub components and report any issues in status
  return Status();
}

Status DatabaseImpl::CreateCollection(const char* name, SchemaType schemaType,
                                      const char* schema,
                                      const IndexInfo indexes[],
                                      size_t indexesLength) {
  DocumentCollection* documentCollectionPtr;
  Status status = DocumentCollection::Construct(
      m_dbMetadataMgrImpl->GetFullDBPath(), name, schemaType, schema, indexes,
      indexesLength, documentCollectionPtr);
  if (!status.OK()) {
    return status;
  }
  unique_ptr<DocumentCollection> documentCollection(documentCollectionPtr);

  status = m_dbMetadataMgrImpl->AddCollection(name, schemaType, schema, indexes,
                                              indexesLength);
  if (!status.OK()) {
    return status;
  }
  m_collectionContainer.insert(
      make_pair<string, unique_ptr<DocumentCollection>>(
          string(name), move(documentCollection)));

  return Status();
}

Status DatabaseImpl::Insert(const char* collectionName,
                            const Buffer& documentData) {
  // Todo (zarian): Check what is a clean way to avoid the string copy from char * to string
  auto item = m_collectionContainer.find(collectionName);
  if (item == m_collectionContainer.end()) {
    ostringstream ss;
    ss << "Collection \"" << collectionName << "\" not found.";
    string errorMsg = ss.str();
    return Status(kStatusCollectionNotFound, errorMsg.c_str(),
                  errorMsg.length());
  }

  // Add data in collection
  Status status = item->second->Insert(documentData);
  if (!status.OK()) {
    return status;
  }

  return status;
}

