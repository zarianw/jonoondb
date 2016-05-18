#pragma once

#include <memory>
#include <unordered_map>
#include <string>
#include <cstdint>
#include "gsl/span.h"
#include "index_manager.h"
#include "document_id_generator.h"
#include "blob_metadata.h"

// Forward declaration
struct sqlite3;

namespace jonoondb_api {
// Forward Declaration
class IndexInfoImpl;
class BufferImpl;
class DocumentSchema;
class IndexStat;
enum class FieldType
: std::int8_t;
enum class SchemaType
: std::int32_t;
struct Constraint;
class MamaJenniesBitmap;
class BlobManager;
struct FileInfo;

class DocumentCollection final {
 public:
   DocumentCollection(const std::string& databaseMetadataFilePath,
     const std::string& name, SchemaType schemaType,
     const std::string& schema, const std::vector<IndexInfoImpl*>& indexes,
     std::unique_ptr<BlobManager> blobManager, const std::vector<FileInfo>& dataFilesToLoad);

  void Insert(const BufferImpl& documentData);
  void MultiInsert(gsl::span<const BufferImpl*>& documents);
  const std::string& GetName();
  const std::shared_ptr<DocumentSchema>& GetDocumentSchema();
  bool TryGetBestIndex(const std::string& columnName, IndexConstraintOperator op,
                       IndexStat& indexStat);
  std::shared_ptr<MamaJenniesBitmap> Filter(const std::vector<Constraint>& constraints);

  //Document Access Functions
  std::int64_t GetDocumentFieldAsInteger(std::uint64_t docID, const std::string& columnName,
                                         std::vector<std::string>& tokens, BufferImpl& buffer,
                                         std::unique_ptr<Document>& document) const;
  double GetDocumentFieldAsDouble(std::uint64_t docID, const std::string& columnName,
                                  std::vector<std::string>& tokens, BufferImpl& buffer,
                                  std::unique_ptr<Document>& document) const;
  std::string GetDocumentFieldAsString(std::uint64_t docID, const std::string& columnName,
                                       std::vector<std::string>& tokens, BufferImpl& buffer,
                                       std::unique_ptr<Document>& document) const;
  void GetDocumentFieldsAsIntegerVector(
      const gsl::span<std::uint64_t>& docIDs, const std::string& columnName,
      std::vector<std::string>& tokens, std::vector<std::int64_t>& values) const;
  void GetDocumentFieldsAsDoubleVector(
      const gsl::span<std::uint64_t>& docIDs, const std::string& columnName,
      std::vector<std::string>& tokens, std::vector<double>& values) const;
  void UnmapLRUDataFiles();

 private:
  void PopulateColumnTypes(
      const std::vector<IndexInfoImpl*>& indexes,
      const DocumentSchema& documentSchema,
      std::unordered_map<std::string, FieldType>& columnTypes);
  std::unique_ptr<sqlite3, void(*)(sqlite3*)> m_dbConnection;  
  std::unique_ptr<IndexManager> m_indexManager;
  std::shared_ptr<DocumentSchema> m_documentSchema;
  DocumentIDGenerator m_documentIDGenerator;
  std::vector<BlobMetadata> m_documentIDMap;
  std::string m_name;
  std::unique_ptr<BlobManager> m_blobManager;
};
}  // namespace jonoondb_api

