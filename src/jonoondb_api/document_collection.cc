#include <string>
#include <boost/filesystem.hpp>
#include <unordered_map>
#include <string>
#include "sqlite3.h"
#include "document_collection.h"
#include "status.h"
#include "string_utils.h"
#include "exception_utils.h"
#include "sqlite_utils.h"
#include "document.h"
#include "document_factory.h"
#include "index_manager.h"

using namespace jonoondb_api;
using namespace std;

DocumentCollection::DocumentCollection(sqlite3* dbConnection, unique_ptr<IndexManager> indexManager)
    : m_dbConnection(dbConnection), m_indexManager(move(indexManager)) {
}

DocumentCollection::~DocumentCollection() {
  if (m_dbConnection != nullptr) {
    if (sqlite3_close(m_dbConnection) != SQLITE_OK) {
      //Todo: Handle SQLITE_BUSY response here
    }
    m_dbConnection = nullptr;
  }
}

Status DocumentCollection::Construct(const char* databaseMetadataFilePath,
                                     const char* name, int schemaType,
                                     const char* schema,
                                     const IndexInfo indexes[],
                                     size_t indexesLength,
                                     DocumentCollection*& documentCollection) {
  string errorMessage;
  // Validate function arguments
  if (StringUtils::IsNullOrEmpty(databaseMetadataFilePath)) {
    errorMessage = "Argument databaseMetadataFilePath is null or empty.";
    return Status(kStatusInvalidArgumentCode, errorMessage.c_str(),
                  (int32_t) errorMessage.length());
  }

  // databaseMetadataFile should exist and all the tables should exist in it
  if (!boost::filesystem::exists(databaseMetadataFilePath)) {
    return Status(kStatusMissingDatabaseFileCode, errorMessage.c_str(),
                  (int32_t) errorMessage.length());
  }

  sqlite3* dbConnection = nullptr;
  int sqliteCode = sqlite3_open(databaseMetadataFilePath, &dbConnection);  //, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr);
  
  std::unique_ptr<sqlite3, int(*)(sqlite3*)> dbConnectionUniquePtr(dbConnection, sqlite3_close);

  if (sqliteCode != SQLITE_OK) {
    errorMessage = ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(
        sqliteCode);
    SQLiteUtils::CloseSQLiteConnection(dbConnection);
    return Status(kStatusFailedToOpenMetadataDatabaseFileCode,
                  errorMessage.c_str(), errorMessage.length());
  }

  IndexManager* indexManager;
  unordered_map<string, ColumnType> columnTypes;
  auto sts = PopulateColumnTypes(indexes, indexesLength, columnTypes);
  if (!sts.OK()) {
    return sts;
  }
  sts = IndexManager::Construct(indexes, indexesLength, columnTypes, indexManager);
  if (!sts.OK()) {
    return sts;
  }
  unique_ptr<IndexManager> indexManagerUniquePtr(indexManager);
  documentCollection = new DocumentCollection(dbConnectionUniquePtr.release(), move(indexManagerUniquePtr));

  return sts;
}

Status DocumentCollection::Insert(const Buffer& documentData) {
  Document* docPtr;
  Status sts = DocumentFactory::CreateDocument(documentData, false, SchemaType::FLAT_BUFFERS, docPtr);
  if (!sts.OK()) {
    return sts;
  }
  
  // unique_ptr will ensure that we will release memory even incase of exception.
  unique_ptr<Document> doc(docPtr);
  sts = m_indexManager->IndexDocument(*doc.get());
  if (!sts.OK()) {
    return sts;
  }

  return sts;
}

Status DocumentCollection::PopulateColumnTypes(const IndexInfo indexes[], size_t indexesLength,
                                               std::unordered_map<std::string, ColumnType>& columnTypes) {
  return Status();
}
