#pragma once
#include <memory>
#include <map>
#include <string>
#include <cstdint>
#include <boost/utility/string_ref.hpp>
#include <thread>
#include <condition_variable>
#include "gsl/span.h"
#include "database_metadata_manager.h"
#include "document_collection.h"
#include "query_processor.h"
#include "options_impl.h"

namespace jonoondb_api {
//Forward Declarations
class BufferImpl;
class IndexInfoImpl;
class ResultSetImpl;
struct WriteOptionsImpl;
enum class SchemaType
    : std::int32_t;

class DatabaseImpl final {
 public:
  DatabaseImpl(const std::string& dbPath, const std::string& dbName,
               const OptionsImpl& options);
  ~DatabaseImpl();
  DatabaseImpl(const DatabaseImpl&) = delete;
  DatabaseImpl(DatabaseImpl&&) = delete;
  DatabaseImpl& operator=(const DatabaseImpl&) = delete;

  void CreateCollection(const std::string& name,
                        SchemaType schemaType,
                        const std::string& schema,
                        const std::vector<IndexInfoImpl*>& indexes);
  void Insert(const char* collectionName, const BufferImpl& documentData,
              const WriteOptionsImpl& wo);
  void MultiInsert(const boost::string_ref& collectionName,
                   gsl::span<const BufferImpl*>& documents,
                   const WriteOptionsImpl& wo);
  ResultSetImpl ExecuteSelect(const std::string& selectStatement);

 private:
  std::shared_ptr<DocumentCollection> CreateCollectionInternal(
      const std::string& name,
      SchemaType schemaType,
      const std::string& schema,
      const std::vector<IndexInfoImpl*>& indexes,
      const std::vector<FileInfo>& dataFilesToLoad);
  std::unique_ptr<DatabaseMetadataManager> m_dbMetadataMgrImpl;
  void MemoryWatcherFunc();
  // m_collectionNameStore stores the collection name as string, m_collectionContainer just uses
  // string_ref as the key. m_collectionNameStore should be declared before m_collectionContainer.
  // This insures that they get destroyed in reverse order i.e. m_collectionContainer first and then
  // m_collectionNameStore.
  std::vector<std::unique_ptr<std::string>> m_collectionNameStore;
  std::map<boost::string_ref, std::shared_ptr<DocumentCollection>>
      m_collectionContainer;
  std::unique_ptr<QueryProcessor> m_queryProcessor;
  OptionsImpl m_options;
  std::thread m_memWatcherThread;
  bool m_shutdownMemWatcher = false;
  std::mutex m_memWatcherMutex;
  std::condition_variable m_memWatcherCV;
};

}  // jonoondb_api
