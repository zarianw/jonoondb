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
#include "file_info.h"

using namespace jonoondb_api;

DocumentCollection::DocumentCollection(const std::string& databaseMetadataFilePath,
  const std::string& name, SchemaType schemaType,
  const std::string& schema, const std::vector<IndexInfoImpl*>& indexes,
  std::unique_ptr<BlobManager> blobManager, const std::vector<FileInfo>& dataFilesToLoad) :
  m_blobManager(move(blobManager)), m_dbConnection(nullptr, SQLiteUtils::CloseSQLiteConnection) {
  // Validate function arguments
  if (databaseMetadataFilePath.size() == 0) {
    throw InvalidArgumentException("Argument databaseMetadataFilePath is empty.", __FILE__, __func__, __LINE__);
  }

  if (name.size() == 0) {
    throw InvalidArgumentException("Argument name is empty.", __FILE__, __func__, __LINE__);
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

  // Load the data files
  for (auto& file : dataFilesToLoad) {
    BlobIterator iter(file);
    const std::size_t desiredBatchSize = 10000;
    std::vector<BufferImpl> blobs(desiredBatchSize);
    std::vector<BlobMetadata> blobMetadataVec(desiredBatchSize);
    std::size_t actualBatchSize = 0;

    while ((actualBatchSize = iter.GetNextBatch(blobs, blobMetadataVec)) > 0) {
      std::vector<std::unique_ptr<Document>> docs;
      for (size_t i = 0; i < actualBatchSize; i++) {
        docs.push_back(DocumentFactory::CreateDocument(m_documentSchema, blobs[i]));
      }      

      auto startID = m_indexManager->IndexDocuments(m_documentIDGenerator, docs, false);
      assert(startID == m_documentIDMap.size());
      m_documentIDMap.insert(m_documentIDMap.end(), blobMetadataVec.begin(),
                             blobMetadataVec.begin() + actualBatchSize);
    }
  }
}

void DocumentCollection::Insert(const BufferImpl& documentData) {
  auto doc = DocumentFactory::CreateDocument(m_documentSchema, documentData);
  BlobMetadata blobMetadata;
  m_blobManager->Put(documentData, blobMetadata);
  
  // Index the document
  auto id = m_indexManager->IndexDocument(m_documentIDGenerator, *doc.get());
 
  m_documentIDMap.push_back(blobMetadata);
  assert(m_documentIDMap.size()-1 == id);
}

void jonoondb_api::DocumentCollection::MultiInsert(gsl::span<const BufferImpl*>& documents) {
  std::vector<BlobMetadata> blobMetadataVec(documents.size());
  m_blobManager->MultiPut(documents, blobMetadataVec);

  std::vector<std::unique_ptr<Document>> docs;
  for (auto documentData : documents) {
    docs.push_back(DocumentFactory::CreateDocument(m_documentSchema, *documentData));    
  }

  auto startID = m_indexManager->IndexDocuments(m_documentIDGenerator, docs);
  assert(startID == m_documentIDMap.size());
  m_documentIDMap.insert(m_documentIDMap.end(), blobMetadataVec.begin(), blobMetadataVec.end());  
}

const std::string& DocumentCollection::GetName() {
  return m_name;
}

const std::shared_ptr<DocumentSchema>& DocumentCollection::GetDocumentSchema() {
  return m_documentSchema;
}

bool DocumentCollection::TryGetBestIndex(const std::string& columnName,
                                         IndexConstraintOperator op,
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

std::int64_t DocumentCollection::GetDocumentFieldAsInteger(
  std::uint64_t docID, std::vector<std::string>& tokens,
  BufferImpl& buffer, std::unique_ptr<Document>& document) const {
  if (tokens.size() == 0) {
    throw InvalidArgumentException("Argument tokens is empty.", __FILE__,
                                   "", __LINE__);
  }

  if (docID >= m_documentIDMap.size()) {
    ostringstream ss;
    ss << "Document with ID '" << docID << "' does exist in collection " << m_name << ".";
    throw MissingDocumentException(ss.str(), __FILE__, __func__, __LINE__);
  }

  m_blobManager->Get(m_documentIDMap.at(docID), buffer);
    
  document = DocumentFactory::CreateDocument(m_documentSchema, buffer);
  if (tokens.size() > 1) {
    auto subDoc = DocumentUtils::GetSubDocumentRecursively(*document, tokens);
    return subDoc->GetIntegerValueAsInt64(tokens.back());
  } else {
    return document->GetIntegerValueAsInt64(tokens.front());
  }
}

double DocumentCollection::GetDocumentFieldAsDouble(
  std::uint64_t docID, std::vector<std::string>& tokens,
  BufferImpl& buffer, std::unique_ptr<Document>& document) const {
  if (tokens.size() == 0) {
    throw InvalidArgumentException("Argument tokens is empty.", __FILE__,
                                   "", __LINE__);
  }

  if (docID >= m_documentIDMap.size()) {
    ostringstream ss;
    ss << "Document with ID '" << docID << "' does exist in collection " << m_name << ".";
    throw MissingDocumentException(ss.str(), __FILE__, __func__, __LINE__);
  }

  m_blobManager->Get(m_documentIDMap.at(docID), buffer);

  document = DocumentFactory::CreateDocument(m_documentSchema, buffer);
  if (tokens.size() > 1) {
    auto subDoc = DocumentUtils::GetSubDocumentRecursively(*document, tokens);
    return subDoc->GetFloatingValueAsDouble(tokens.back());
  } else {
    return document->GetFloatingValueAsDouble(tokens.front());
  }
}

// Todo: Need to avoid the string creation/copy cost
std::string DocumentCollection::GetDocumentFieldAsString(
  std::uint64_t docID, std::vector<std::string>& tokens,
  BufferImpl& buffer, std::unique_ptr<Document>& document) const {
  if (tokens.size() == 0) {
    throw InvalidArgumentException("Argument tokens is empty.", __FILE__,
                                   "", __LINE__);
  }

  if (docID >= m_documentIDMap.size()) {
    ostringstream ss;
    ss << "Document with ID '" << docID << "' does exist in collection " << m_name << ".";
    throw MissingDocumentException(ss.str(), __FILE__, __func__, __LINE__);
  }

  m_blobManager->Get(m_documentIDMap.at(docID), buffer);

  document = DocumentFactory::CreateDocument(m_documentSchema, buffer);
  if (tokens.size() > 1) {
    auto subDoc = DocumentUtils::GetSubDocumentRecursively(*document, tokens);
    return subDoc->GetStringValue(tokens.back());
  } else {
    return document->GetStringValue(tokens.front());
  }
}
