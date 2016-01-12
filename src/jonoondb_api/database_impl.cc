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

using namespace std;
using namespace jonoondb_api;

DatabaseImpl::DatabaseImpl(const std::string& dbPath, const std::string& dbName,
  const OptionsImpl& options) : m_options(options) {
  // Initialize DatabaseMetadataManager
  m_dbMetadataMgrImpl = std::make_unique<DatabaseMetadataManager>(dbPath,
    dbName, options.GetCreateDBIfMissing());

  // Initialize query processor
  m_queryProcessor = std::make_unique<QueryProcessor>(dbPath, dbName);  
}

void DatabaseImpl::Close() {
  // Todo (zarian): Close all sub components and log any issues
  DocumentCollectionDictionary::Instance()->Clear();
}

void DatabaseImpl::CreateCollection(const std::string& name, SchemaType schemaType,
  const std::string& schema, const std::vector<IndexInfoImpl*>& indexes) {

  //First create FileNameManager and BlobManager
  auto fnm = std::make_unique<FileNameManager>(m_dbMetadataMgrImpl->GetDBPath(),
    m_dbMetadataMgrImpl->GetDBName(), false);
  auto bm = std::make_unique<BlobManager>(move(fnm), m_options.GetCompressionEnabled(), m_options.GetMaxDataFileSize(), m_options.GetSynchronous());

  shared_ptr<DocumentCollection> documentCollection =
    make_shared<DocumentCollection>(m_dbMetadataMgrImpl->GetFullDBPath(), name, schemaType, schema, indexes, move(bm));

  //check if collection already exists
  string colName = name;
  if (m_collectionContainer.find(colName) != m_collectionContainer.end()) {
    ostringstream ss;
    ss << "Collection with name \"" << name << "\" already exists.";
    throw CollectionAlreadyExistException(ss.str(), __FILE__, "", __LINE__);
  }
  
  m_queryProcessor->AddCollection(documentCollection);  
  // TODO: we need to call m_queryProcessor->RemoveCollection if the below call fails
  m_dbMetadataMgrImpl->AddCollection(name, schemaType, schema, indexes);  

  m_collectionContainer[colName] = documentCollection;
}

void DatabaseImpl::Insert(const char* collectionName,
                            const BufferImpl& documentData) {
  // Todo (zarian): Check what is a clean way to avoid the string copy from char * to string
  auto item = m_collectionContainer.find(collectionName);
  if (item == m_collectionContainer.end()) {
    ostringstream ss;
    ss << "Collection \"" << collectionName << "\" not found.";    
    throw CollectionNotFoundException(ss.str(), __FILE__, "", __LINE__);
  }

  // Add data in collection
  item->second->Insert(documentData); 
}

ResultSetImpl DatabaseImpl::ExecuteSelect(const std::string& selectStatement) {
  return m_queryProcessor->ExecuteSelect(selectStatement);
}
