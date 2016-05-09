#include <string>
#include <stdio.h>
#include <sstream>
#include <memory>
#include <chrono>
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
#include "proc_utils.h"

using namespace jonoondb_api;

void DatabaseImpl::MemoryWatcherFunc() {
  ProcessMemStat stat;
  int lastPos = -1;
  while (true) {
    try {
      std::unique_lock<std::mutex> lock(m_memWatcherMutex);
      if (m_shutdownMemWatcher) {
        break;
      }

      // Check whether we need to cleanup memory every 5 secs
      m_memWatcherCV.wait_for(lock, std::chrono::seconds(5));

      // conditional variable can also be signaled on shutdown
      if (m_shutdownMemWatcher) {
        break;
      }

      if (m_collectionContainer.size() == 0) {
        continue;
      }     

      ProcessUtils::GetProcessMemoryStats(stat);
      if (stat.MemoryUsedInBytes > m_options.GetMemoryCleanupThreshold()) {
        // lastPos and currPos ensures that we don't keep hitting the
        // same collections. We will visit the collections in a round
        // robin fashion
        if (lastPos >= m_collectionContainer.size() - 1) {
          lastPos = -1;
        }

        int currPos = 0;
        for (auto& entry : m_collectionContainer) {
          if (currPos <= lastPos) {
            currPos++;
            continue;
          }
          entry.second->UnmapLRUDataFiles();
          ProcessUtils::GetProcessMemoryStats(stat);
          if (stat.MemoryUsedInBytes < m_options.GetMemoryCleanupThreshold()) {
            break;
          }
          currPos++;
        }
        lastPos = currPos;
      }
    } catch (std::exception&) {
      // Todo: Log exception
      assert(false);
    }
  }
}

DatabaseImpl::DatabaseImpl(const std::string& dbPath, const std::string& dbName,
  const OptionsImpl& options) : m_options(options) {
  // Initialize DatabaseMetadataManager
  m_dbMetadataMgrImpl = std::make_unique<DatabaseMetadataManager>(
    dbPath, dbName, options.GetCreateDBIfMissing());

  // Initialize query processor
  m_queryProcessor = std::make_unique<QueryProcessor>(dbPath, dbName);

  std::vector<CollectionMetadata> collectionsInfo;
  m_dbMetadataMgrImpl->GetExistingCollections(collectionsInfo);
  
  for (auto& colInfo : collectionsInfo) {
    std::vector<IndexInfoImpl*> indexes;
    // Todo: make this conversion cleaner
    for (auto& index : colInfo.indexes) {
      indexes.push_back(&index);
    }

    auto documentCollection = CreateCollectionInternal(colInfo.name,
                                                       colInfo.schemaType, colInfo.schema,
                                                       indexes, colInfo.dataFiles);    

    m_queryProcessor->AddExistingCollection(documentCollection);

    m_collectionNameStore.push_back(std::make_unique<std::string>(colInfo.name));
    m_collectionContainer[*m_collectionNameStore.back()] = documentCollection;
  }  

  m_memWatcherThread = std::thread(&DatabaseImpl::MemoryWatcherFunc, this);
}

DatabaseImpl::~DatabaseImpl() {
  {
    std::unique_lock<std::mutex> lock(m_memWatcherMutex);
    m_shutdownMemWatcher = true;    
  }
  m_memWatcherCV.notify_one();
  // Todo (zarian): Close all sub components and log any issues
  // Only clear the collections for this database and not all.
  DocumentCollectionDictionary::Instance()->Clear();
  
  m_memWatcherThread.join();
}

void DatabaseImpl::CreateCollection(const std::string& name, SchemaType schemaType,
  const std::string& schema, const std::vector<IndexInfoImpl*>& indexes) {
  // check if collection already exists
  if (m_collectionContainer.find(name) != m_collectionContainer.end()) {
    std::ostringstream ss;
    ss << "Collection with name \"" << name << "\" already exists.";
    throw CollectionAlreadyExistException(ss.str(), __FILE__, __func__, __LINE__);
  }

  auto documentCollection = CreateCollectionInternal(name, schemaType, schema, indexes,
                                                     std::vector<FileInfo>());  
  
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

  m_collectionNameStore.push_back(std::make_unique<std::string>(name));
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
  const std::vector<IndexInfoImpl*>& indexes, const std::vector<FileInfo>& dataFilesToLoad) {

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
                                              indexes, move(bm), dataFilesToLoad);
}