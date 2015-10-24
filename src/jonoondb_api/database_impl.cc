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
#include "query_processor.h"
#include "resultset.h"

using namespace std;
using namespace jonoondb_api;

DatabaseImpl::DatabaseImpl(
    unique_ptr<DatabaseMetadataManager> databaseMetadataManager,
    unique_ptr<QueryProcessor> queryProcessor)
    : m_dbMetadataMgrImpl(move(databaseMetadataManager)),
      m_queryProcessor(move(queryProcessor)) {
}

DatabaseImpl* DatabaseImpl::Open(const std::string& dbPath, const std::string& dbName,
                          const Options& options) {
  // Initialize query processor
  unique_ptr<QueryProcessor> qp = std::make_unique<QueryProcessor>();
  
  // Initialize DatabaseMetadataManager
  std::unique_ptr<DatabaseMetadataManager> databaseMetadataManager =
    std::make_unique<DatabaseMetadataManager>(dbPath, dbName, options.GetCreateDBIfMissing()); 

  return new DatabaseImpl(move(databaseMetadataManager), move(qp));
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
  auto sts = DocumentCollection::Construct(
      m_dbMetadataMgrImpl->GetFullDBPath(), name, schemaType, schema, indexes,
      indexesLength, documentCollectionPtr);
  if (!sts) return sts;
  shared_ptr<DocumentCollection> documentCollection(documentCollectionPtr);

  //check if collection already exists
  string colName = name;
  if (m_collectionContainer.find(colName) != m_collectionContainer.end()) {
    ostringstream ss;
    ss << "Collection with name \"" << name << "\" already exists.";
    std::string errorMsg = ss.str();
    return Status(kStatusCollectionAlreadyExistCode, errorMsg.c_str(),
      __FILE__, "", __LINE__);
  }
  
  sts = m_queryProcessor->AddCollection(documentCollection);
  if (!sts) return sts;
  
  sts = m_dbMetadataMgrImpl->AddCollection(name, schemaType, schema, indexes,
                                           indexesLength);
  if (!sts) {
    // TODO: we need to call m_queryProcessor->RemoveCollection here.
    return sts;
  }

  m_collectionContainer[colName] = documentCollection;

  return sts;
}

Status DatabaseImpl::Insert(const char* collectionName,
                            const Buffer& documentData) {
  // Todo (zarian): Check what is a clean way to avoid the string copy from char * to string
  auto item = m_collectionContainer.find(collectionName);
  if (item == m_collectionContainer.end()) {
    ostringstream ss;
    ss << "Collection \"" << collectionName << "\" not found.";
    string errorMsg = ss.str();
    return Status(kStatusCollectionNotFoundCode, errorMsg.c_str(),
                  __FILE__, "", __LINE__);
  }

  // Add data in collection
  Status status = item->second->Insert(documentData);
  if (!status.OK()) {
    return status;
  }

  return status;
}

Status DatabaseImpl::ExecuteSelect(const char* selectStatement, ResultSet*& resultSet) {
  return m_queryProcessor->ExecuteSelect(selectStatement, resultSet);
}
