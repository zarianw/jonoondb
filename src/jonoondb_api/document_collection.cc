#include <string>
#include <boost/filesystem.hpp>
#include <unordered_map>
#include <string>
#include "sqlite3.h"
#include "document_collection.h"
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
#include "constraint.h"
#include "buffer_impl.h"

using namespace jonoondb_api;

DocumentCollection::DocumentCollection(const std::string& databaseMetadataFilePath,
  const std::string& name, SchemaType schemaType,
  const std::string& schema, const std::vector<IndexInfoImpl*>& indexes,
  std::unique_ptr<BlobManager> blobManager) : m_blobManager(move(blobManager)),
  m_dbConnection(nullptr, SQLiteUtils::CloseSQLiteConnection) {
  // Validate function arguments
  if (StringUtils::IsNullOrEmpty(databaseMetadataFilePath)) {
    throw InvalidArgumentException("Argument databaseMetadataFilePath is null or empty.", __FILE__, __func__, __LINE__);
  }

  if (StringUtils::IsNullOrEmpty(name)) {
    throw InvalidArgumentException("Argument name is null or empty.", __FILE__, __func__, __LINE__);
  }
  m_name = name;

  // databaseMetadataFile should exist and all the tables should exist in it
  if (!boost::filesystem::exists(databaseMetadataFilePath)) {
    std::ostringstream ss;
    ss << "Database file " << databaseMetadataFilePath << " does not exist.";
    throw MissingDatabaseFileException(ss.str(), __FILE__, __func__, __LINE__);
  } 

  sqlite3* dbConnection = nullptr;
  int sqliteCode = sqlite3_open(databaseMetadataFilePath.c_str(), &dbConnection);  //, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr);
  m_dbConnection.reset(dbConnection);

  if (sqliteCode != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__, __LINE__);
  }

  m_documentSchema.reset(DocumentSchemaFactory::CreateDocumentSchema(schema, schemaType));
  
  unordered_map<string, FieldType> columnTypes;
  PopulateColumnTypes(indexes, *m_documentSchema.get(), columnTypes);
  m_indexManager.reset(new IndexManager(indexes, columnTypes));      
}

void DocumentCollection::Insert(const BufferImpl& documentData) {
  auto doc = DocumentFactory::CreateDocument(m_documentSchema, documentData);
  BlobMetadata blobMetadata;
  m_blobManager->Put(documentData, blobMetadata);
  
  // Index the document
  auto id = m_documentIDGenerator.ReserveID(1);
  m_indexManager->IndexDocument(id, *doc.get());  
 
  m_documentIDMap.push_back(blobMetadata);
  assert(m_documentIDMap.size()-1 == id);
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
  for (std::size_t i = 0; i < indexes.size(); i++) {
    columnTypes.insert(
      pair<string, FieldType>(indexes[i]->GetColumnName(),
      documentSchema.GetFieldType(indexes[i]->GetColumnName())));
  }
}

std::shared_ptr<MamaJenniesBitmap> DocumentCollection::Filter(const std::vector<Constraint>& constraints) {
  if (constraints.size() > 0) {
    return m_indexManager->Filter(constraints);
  } else {
    // Return all the ids
    auto lastID = m_documentIDMap.size();
    auto bm = std::make_shared<MamaJenniesBitmap>();
    for (std::size_t i = 0; i < lastID; i++) {
      bm->Add(i);
    }

    return bm;
  }
}

//Todo: Need to avoid the string creation/copy cost
std::string DocumentCollection::GetDocumentFieldAsString(std::uint64_t docID,
  const std::string& fieldName) const {
  if (fieldName.size() == 0) {
    throw InvalidArgumentException("Argument fieldName is empty.", __FILE__,
      "", __LINE__);
  }

  if (docID >= m_documentIDMap.size()) {
    ostringstream ss;
    ss << "Document with ID '" << docID << "' does exist in collection " << m_name << ".";
    throw MissingDocumentException(ss.str(), __FILE__, __func__, __LINE__);
  }

  // TODO: buffer should come from object pool
  BufferImpl buffer;
  m_blobManager->Get(m_documentIDMap.at(docID), buffer);

  // TODO: tokens should be cached in collection class
  std::vector<std::string> tokens = StringUtils::Split(fieldName, ".");
  // TODO: Document should be cached
  auto doc = DocumentFactory::CreateDocument(m_documentSchema, buffer);
  if (tokens.size() > 1) {
    auto subDoc = DocumentUtils::GetSubDocumentRecursively(*doc, tokens);
    return subDoc->GetStringValue(tokens.back());
  } else {
    return doc->GetStringValue(fieldName);
  }  
}

std::int64_t DocumentCollection::GetDocumentFieldAsInteger(std::uint64_t docID,
  const std::string& fieldName) const {
  if (fieldName.size() == 0) {
    throw InvalidArgumentException("Argument fieldName is empty.", __FILE__,
      "", __LINE__);
  }

  if (docID >= m_documentIDMap.size()) {
    ostringstream ss;
    ss << "Document with ID '" << docID << "' does exist in collection " << m_name << ".";
    throw MissingDocumentException(ss.str(), __FILE__, __func__, __LINE__);
  }

  // TODO: buffer should come from object pool
  BufferImpl buffer;
  m_blobManager->Get(m_documentIDMap.at(docID), buffer);

  // TODO: tokens should be cached in collection class
  std::vector<std::string> tokens = StringUtils::Split(fieldName, ".");
  // TODO: Document should be cached  
  auto doc = DocumentFactory::CreateDocument(m_documentSchema, buffer);
  if (tokens.size() > 1) {
    auto subDoc = DocumentUtils::GetSubDocumentRecursively(*doc, tokens);
    return subDoc->GetIntegerValueAsInt64(tokens.back());
  } else {
    return doc->GetIntegerValueAsInt64(fieldName);
  }
}

double DocumentCollection::GetDocumentFieldAsDouble(std::uint64_t docID,
  const std::string& fieldName) const {
  if (fieldName.size() == 0) {
    throw InvalidArgumentException("Argument fieldName is empty.", __FILE__,
      "", __LINE__);
  }

  if (docID >= m_documentIDMap.size()) {
    ostringstream ss;
    ss << "Document with ID '" << docID << "' does exist in collection " << m_name << ".";
    throw MissingDocumentException(ss.str(), __FILE__, __func__, __LINE__);
  }

  // TODO: buffer should come from object pool
  BufferImpl buffer;
  m_blobManager->Get(m_documentIDMap.at(docID), buffer);

  // TODO: tokens should be cached in collection class
  std::vector<std::string> tokens = StringUtils::Split(fieldName, ".");
  // TODO: Document should be cached  
  auto doc = DocumentFactory::CreateDocument(m_documentSchema, buffer);
  if (tokens.size() > 1) {
    auto subDoc = DocumentUtils::GetSubDocumentRecursively(*doc, tokens);
    return subDoc->GetFloatingValueAsDouble(tokens.back());
  } else {
    return doc->GetFloatingValueAsDouble(fieldName);
  }
}
