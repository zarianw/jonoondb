#include <string>
#include <boost/filesystem.hpp>
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

DocumentCollection::DocumentCollection(sqlite3* dbConnection)
    : m_dbConnection(dbConnection) {
}

DocumentCollection::~DocumentCollection() {
}

Status DocumentCollection::Construct(const char* databaseMetadataFilePath,
                                     const char* name, int schemaType,
                                     const char* schema,
                                     const IndexInfo indexes[],
                                     int indexesLength,
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

  sqlite3* dbConnection;
  int sqliteCode = sqlite3_open(databaseMetadataFilePath, &dbConnection);  //, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr);

  if (sqliteCode != SQLITE_OK) {
    errorMessage = ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(
        sqliteCode);
    SQLiteUtils::CloseSQLiteConnection(dbConnection);
    return Status(kStatusFailedToOpenMetadataDatabaseFileCode,
                  errorMessage.c_str(), errorMessage.length());
  }

  documentCollection = new DocumentCollection(dbConnection);

  return Status();
}

Status DocumentCollection::Insert(const Buffer& documentData) {
  Document* docPtr;
  Status sts = DocumentFactory::CreateDocument(documentData, false, SchemaType::FLAT_BUFFERS, docPtr);
  if (!sts.OK()) {
    return sts;
  }
  
  // unique_ptr will ensure that we will release memory even incase of exception.
  unique_ptr<Document> doc(docPtr);
  m_indexManager->IndexDocument(*doc.get());

  return Status();
}
