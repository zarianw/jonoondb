#include <string>
#include <stdio.h>
#include <sstream>
#include <memory>
#include "database_impl.h"
#include "database_metadata_manager.h"
#include "options_impl.h"
#include "string_utils.h"
#include "index_manager.h"
#include "document_collection.h"
#include "enums.h"
#include "query_processor.h"
#include "resultset_impl.h"
#include "filename_manager.h"
#include "blob_manager.h"
#include "document_collection_dictionary.h"
#include "index_info_impl.h"

using namespace jonoondb_api;

DatabaseImpl::DatabaseImpl(const std::string& dbPath, const std::string& dbName,
  const OptionsImpl& options) : m_options(options) {
  // Initialize DatabaseMetadataManager
  m_dbMetadataMgrImpl = std::make_unique<DatabaseMetadataManager>(
    dbPath, dbName, options.GetCreateDBIfMissing());

  std::vector<CollectionMetadata> collectionsInfo;
  m_dbMetadataMgrImpl->GetExistingCollections(collectionsInfo);
  
  for (auto& colInfo : collectionsInfo) {
    std::vector<IndexInfoImpl*> indexes;
    auto documentCollection = CreateCollectionInternal(colInfo.name,
                                                       colInfo.schemaType, colInfo.schema,
                                                       indexes);

    m_collectionNameStore.push_back(std::make_unique<std::string>(colInfo.name));
    m_collectionContainer[*m_collectionNameStore.back()] = documentCollection;
  }

  // Initialize query processor
  m_queryProcessor = std::make_unique<QueryProcessor>(dbPath, dbName);  
}

DatabaseImpl::~DatabaseImpl() {
  // Todo (zarian): Close all sub components and log any issues
  // Only clear the collections for this database and not all.
  DocumentCollectionDictionary::Instance()->Clear();
}

void DatabaseImpl::CreateCollection(const std::string& name, SchemaType schemaType,
  const std::string& schema, const std::vector<IndexInfoImpl*>& indexes) {

  auto documentCollection = CreateCollectionInternal(name, schemaType, schema, indexes);

  //check if collection already exists
  std::string colName = name;
  if (m_collectionContainer.find(colName) != m_collectionContainer.end()) {
    std::ostringstream ss;
    ss << "Collection with name \"" << name << "\" already exists.";
    throw CollectionAlreadyExistException(ss.str(), __FILE__, __func__, __LINE__);
  }
  
  m_queryProcessor->AddCollection(documentCollection);  
  
  try {
    m_dbMetadataMgrImpl->AddCollection(name, schemaType, schema, indexes);
  } catch (...) {
    try {
      m_queryProcessor->RemoveCollection(name);
    } catch (...) {
      // This is bad and we should log the error and terminate the program
      // Todo: Log the std::current_exception(), also mark the collection for cleanup on startup
      std::terminate();
    }
    throw;
  }

  m_collectionNameStore.push_back(std::make_unique<std::string>(colName));
  m_collectionContainer[*m_collectionNameStore.back()] = documentCollection;
}

void DatabaseImpl::Insert(const char* collectionName,
                            const BufferImpl& documentData) {
  // Todo (zarian): Check what is a clean way to avoid the string copy from char * to string
  auto item = m_collectionContainer.find(collectionName);
  if (item == m_collectionContainer.end()) {
    std::ostringstream ss;
    ss << "Collection \"" << collectionName << "\" not found.";    
    throw CollectionNotFoundException(ss.str(), __FILE__, __func__, __LINE__);
  }

  // Add data in collection
  item->second->Insert(documentData); 
}

void DatabaseImpl::MultiInsert(const boost::string_ref& collectionName,
                               gsl::span<const BufferImpl*>& documents) {
  auto item = m_collectionContainer.find(collectionName);
  if (item == m_collectionContainer.end()) {
    std::ostringstream ss;
    ss << "Collection \"" << collectionName << "\" not found.";
    throw CollectionNotFoundException(ss.str(), __FILE__, __func__, __LINE__);
  }

  // Add data in collection
  item->second->MultiInsert(documents);
}

ResultSetImpl DatabaseImpl::ExecuteSelect(const std::string& selectStatement) {
  return m_queryProcessor->ExecuteSelect(selectStatement);
}

std::shared_ptr<DocumentCollection> DatabaseImpl::CreateCollectionInternal(
  const std::string& name, SchemaType schemaType, const std::string& schema,
  const std::vector<IndexInfoImpl*>& indexes) {

  //First create FileNameManager and BlobManager
  auto fnm = std::make_unique<FileNameManager>(m_dbMetadataMgrImpl->GetDBPath(),
                                               m_dbMetadataMgrImpl->GetDBName(),
                                               name, false);

  auto bm = std::make_unique<BlobManager>(move(fnm),
                                          m_options.GetCompressionEnabled(),
                                          m_options.GetMaxDataFileSize(),
                                          m_options.GetSynchronous());

  return std::make_shared<DocumentCollection>(m_dbMetadataMgrImpl->GetFullDBPath(),
                                              name, schemaType, schema,
                                              indexes, move(bm));
}
