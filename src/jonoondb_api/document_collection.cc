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
#include "index_info_impl.h"
#include "document_schema.h"
#include "document_schema_factory.h"
#include "enums.h"
#include "jonoondb_exceptions.h"

using namespace jonoondb_api;
using namespace std;

DocumentCollection::DocumentCollection(const std::string& databaseMetadataFilePath,
  const std::string& name, SchemaType schemaType,
  const std::string& schema, const std::vector<IndexInfoImpl*>& indexes) :
    m_name(name) {
  // Validate function arguments
  if (StringUtils::IsNullOrEmpty(databaseMetadataFilePath)) {
    throw InvalidArgumentException("Argument databaseMetadataFilePath is null or empty.", __FILE__, "", __LINE__);
  }

  if (StringUtils::IsNullOrEmpty(name)) {
    throw InvalidArgumentException("Argument name is null or empty.", __FILE__, "", __LINE__);
  }

  // databaseMetadataFile should exist and all the tables should exist in it
  if (!boost::filesystem::exists(databaseMetadataFilePath)) {
    std::ostringstream ss;
    ss << "Database file " << databaseMetadataFilePath << " does not exist.";
    throw MissingDatabaseFileException(ss.str(), __FILE__, "", __LINE__);
  }

  sqlite3* dbConnection = nullptr;
  int sqliteCode = sqlite3_open(databaseMetadataFilePath.c_str(), &dbConnection);  //, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr);

  std::unique_ptr<sqlite3, int (*)(sqlite3*)> dbConnectionUniquePtr(
      dbConnection, sqlite3_close);

  if (sqliteCode != SQLITE_OK) {
    std::string msg = sqlite3_errstr(sqliteCode);
    throw SQLException(msg, __FILE__, "", __LINE__);
  }

  m_documentSchema.reset(DocumentSchemaFactory::CreateDocumentSchema(schema, schemaType));
  
  unordered_map<string, FieldType> columnTypes;
  PopulateColumnTypes(indexes, *m_documentSchema.get(), columnTypes);
  m_indexManager.reset(new IndexManager(indexes, columnTypes));  
  
  m_dbConnection = dbConnectionUniquePtr.release();
}

DocumentCollection::~DocumentCollection() {
  if (m_dbConnection != nullptr) {
    if (sqlite3_close(m_dbConnection) != SQLITE_OK) {
      //Todo: Handle SQLITE_BUSY response here
    }
    m_dbConnection = nullptr;
  }
}

Status DocumentCollection::Insert(const BufferImpl& documentData) {
  auto doc = DocumentFactory::CreateDocument(m_documentSchema, documentData);
  
  // Index the document
  auto id = m_documentIDGenerator.ReserveID(1);
  auto sts = m_indexManager->IndexDocument(id, *doc.get());
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

void DocumentCollection::PopulateColumnTypes(
  const std::vector<IndexInfoImpl*>& indexes,
  const DocumentSchema& documentSchema,
  std::unordered_map<string, FieldType>& columnTypes) {
  FieldType colType;
  for (size_t i = 0; i < indexes.size(); i++) {
    auto sts = documentSchema.GetFieldType(indexes[i]->GetColumnName().c_str(), colType);    
    columnTypes.insert(
      pair<string, FieldType>(indexes[i]->GetColumnName(), colType));
  }
}
