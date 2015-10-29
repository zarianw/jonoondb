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
#include "index_info.h"
#include "document_schema.h"
#include "document_schema_factory.h"
#include "enums.h"

using namespace jonoondb_api;
using namespace std;

DocumentCollection::DocumentCollection(const char* name,
    sqlite3* dbConnection, unique_ptr<IndexManager> indexManager,
    shared_ptr<DocumentSchema> documentSchema)
    : m_name(name),
      m_dbConnection(dbConnection),
      m_indexManager(move(indexManager)),
      m_documentSchema(documentSchema) {
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
                                     const char* name, SchemaType schemaType,
                                     const char* schema,
                                     const IndexInfo indexes[],
                                     size_t indexesLength,
                                     DocumentCollection*& documentCollection) {
  string errorMessage;
  // Validate function arguments
  if (StringUtils::IsNullOrEmpty(databaseMetadataFilePath)) {
    errorMessage = "Argument databaseMetadataFilePath is null or empty.";
    return Status(kStatusInvalidArgumentCode, errorMessage.c_str(),
                  __FILE__, "", __LINE__);
  }

  if (StringUtils::IsNullOrEmpty(name)) {
    errorMessage = "Argument name is null or empty.";
    return Status(kStatusInvalidArgumentCode, errorMessage.c_str(),
                  __FILE__, "", __LINE__);
  }

  // databaseMetadataFile should exist and all the tables should exist in it
  if (!boost::filesystem::exists(databaseMetadataFilePath)) {
    return Status(kStatusMissingDatabaseFileCode, errorMessage.c_str(),
                  __FILE__, "", __LINE__);
  }

  sqlite3* dbConnection = nullptr;
  int sqliteCode = sqlite3_open(databaseMetadataFilePath, &dbConnection);  //, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr);

  std::unique_ptr<sqlite3, int (*)(sqlite3*)> dbConnectionUniquePtr(
      dbConnection, sqlite3_close);

  if (sqliteCode != SQLITE_OK)
    return ExceptionUtils::GetSQLiteErrorStatusFromSQLiteErrorCode(sqliteCode);

  IndexManager* indexManager;
  unordered_map<string, FieldType> columnTypes;

  DocumentSchema* documentSchema;
  auto sts = DocumentSchemaFactory::CreateDocumentSchema(schema, schemaType,
                                                         documentSchema);
  if (!sts.OK()) {
    return sts;
  }

  std::shared_ptr<DocumentSchema> documentSchemaPtr(documentSchema);
  sts = PopulateColumnTypes(indexes, indexesLength, *documentSchemaPtr.get(),
                            columnTypes);
  if (!sts.OK()) {
    return sts;
  }

  sts = IndexManager::Construct(indexes, indexesLength, columnTypes,
                                indexManager);
  if (!sts.OK()) {
    return sts;
  }

  unique_ptr<IndexManager> indexManagerUniquePtr(indexManager);
  documentCollection = new DocumentCollection(name,
                                              dbConnectionUniquePtr.release(),
                                              move(indexManagerUniquePtr),
                                              move(documentSchemaPtr));
  return sts;
}

Status DocumentCollection::Insert(const Buffer& documentData) {
  Document* docPtr;
  auto sts = DocumentFactory::CreateDocument(m_documentSchema, documentData,
                                               docPtr);
  if (!sts) return sts;
  // unique_ptr will ensure that memory does not leak.
  unique_ptr<Document> doc(docPtr);
  
  // Index the document
  auto id = m_documentIDGenerator.ReserveID(1);
  sts = m_indexManager->IndexDocument(id, *doc.get());
  if (!sts) return sts;

  // Add it in the documentID map
  // Todo: Once we have the blobManager then use the real blobMetadata.
  BlobMetadata blobMetadata;
  blobMetadata.fileKey = 1;
  blobMetadata.offset = 0;
  m_documentIDMap[id] = blobMetadata;

  return sts;
}

const std::string& DocumentCollection::GetName() {
  return m_name;
}

const std::shared_ptr<DocumentSchema>& DocumentCollection::GetDocumentSchema() {
  return m_documentSchema;
}

bool DocumentCollection::TryGetBestIndex(const std::string& columnName, IndexConstraintOperator op,
  IndexStat& indexStat) {
  return m_indexManager->TryGetBestIndex(columnName, op, indexStat);
}

Status DocumentCollection::PopulateColumnTypes(
  const IndexInfo indexes[], size_t indexesLength,
  const DocumentSchema& documentSchema,
  std::unordered_map<string, FieldType>& columnTypes) {
  FieldType colType;
  for (size_t i = 0; i < indexesLength; i++) {
    auto sts = documentSchema.GetFieldType(indexes[i].GetColumnName().c_str(), colType);
    if (!sts.OK()) {
      return sts;
    }
    columnTypes.insert(
      pair<string, FieldType>(indexes[i].GetColumnName(), colType));
  }
  return Status();
}
