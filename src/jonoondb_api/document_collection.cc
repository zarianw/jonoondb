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
#include "index_stat.h"
#include "mama_jennies_bitmap.h"
#include "blob_manager.h"

using namespace jonoondb_api;
using namespace std;

DocumentCollection::DocumentCollection(const std::string& databaseMetadataFilePath,
  const std::string& name, SchemaType schemaType,
  const std::string& schema, const std::vector<IndexInfoImpl*>& indexes,
  std::unique_ptr<BlobManager> blobManager) : m_blobManager(move(blobManager)),
  m_dbConnection(nullptr, SQLiteUtils::CloseSQLiteConnection) {
  // Validate function arguments
  if (StringUtils::IsNullOrEmpty(databaseMetadataFilePath)) {
    throw InvalidArgumentException("Argument databaseMetadataFilePath is null or empty.", __FILE__, "", __LINE__);
  }

  if (StringUtils::IsNullOrEmpty(name)) {
    throw InvalidArgumentException("Argument name is null or empty.", __FILE__, "", __LINE__);
  }
  m_name = name;

  // databaseMetadataFile should exist and all the tables should exist in it
  if (!boost::filesystem::exists(databaseMetadataFilePath)) {
    std::ostringstream ss;
    ss << "Database file " << databaseMetadataFilePath << " does not exist.";
    throw MissingDatabaseFileException(ss.str(), __FILE__, "", __LINE__);
  } 

  sqlite3* dbConnection = nullptr;
  int sqliteCode = sqlite3_open(databaseMetadataFilePath.c_str(), &dbConnection);  //, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr);
  m_dbConnection.reset(dbConnection);

  if (sqliteCode != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, "", __LINE__);
  }

  m_documentSchema.reset(DocumentSchemaFactory::CreateDocumentSchema(schema, schemaType));
  
  unordered_map<string, FieldType> columnTypes;
  PopulateColumnTypes(indexes, *m_documentSchema.get(), columnTypes);
  m_indexManager.reset(new IndexManager(indexes, columnTypes));      
}

void DocumentCollection::Insert(const BufferImpl& documentData) {
  auto doc = DocumentFactory::CreateDocument(m_documentSchema, documentData);
  
  // Index the document
  auto id = m_documentIDGenerator.ReserveID(1);
  m_indexManager->IndexDocument(id, *doc.get());  

  // Add it in the documentID map
  // Todo: Once we have the blobManager then use the real blobMetadata.
  BlobMetadata blobMetadata;
  m_blobManager->Put(documentData, blobMetadata);  
 
  m_documentIDMap[id] = blobMetadata;
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

std::shared_ptr<MamaJenniesBitmap> DocumentCollection::Filter(const std::vector<Constraint>& constraints) {
  return m_indexManager->Filter(constraints);
}
