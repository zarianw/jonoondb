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
class BlobManager;
struct FileInfo;
struct WriteOptionsImpl;
class DeleteVector;

class DocumentCollection final {
 public:
  DocumentCollection(const std::string& dbPath, const std::string& dbName,
                     const std::string& name, SchemaType schemaType,
                     const std::string& schema,
                     const std::vector<IndexInfoImpl*>& indexes,
                     std::unique_ptr<BlobManager> blobManager,
                     const std::vector<FileInfo>& dataFilesToLoad);

  void Insert(const BufferImpl& documentData, const WriteOptionsImpl& wo);
  void MultiInsert(gsl::span<const BufferImpl*>& documents,
                   const WriteOptionsImpl& wo);
  const std::string& GetName();
  const std::shared_ptr<DocumentSchema>& GetDocumentSchema();
  bool
      TryGetBestIndex(const std::string& columnName, IndexConstraintOperator op,
                      IndexStat& indexStat);
  std::shared_ptr<MamaJenniesBitmap>
      Filter(const std::vector<Constraint>& constraints);

  //Document Access Functions
  void GetDocumentAndBuffer(std::uint64_t docID,
                            std::unique_ptr<Document>& document,
                            BufferImpl & buffer) const;

  bool TryGetBlobFieldFromIndexer(std::uint64_t docID,
                                  const std::string& columnName,
                                  BufferImpl& val) const;
  bool TryGetIntegerFieldFromIndexer(std::uint64_t docID,
                                     const std::string& columnName,
                                     std::int64_t& val) const;
  bool TryGetFloatFieldFromIndexer(std::uint64_t docID,
                                   const std::string& columnName,
                                   double& val) const;
  bool TryGetStringFieldFromIndexer(std::uint64_t docID,
                                    const std::string& columnName,
                                    std::string& val) const;    
  void GetDocumentFieldsAsIntegerVector(
      const gsl::span<std::uint64_t>& docIDs, const std::string& columnName,
      const std::vector<std::string>& tokens, std::vector<std::int64_t>& values)
      const;
  void GetDocumentFieldsAsDoubleVector(
      const gsl::span<std::uint64_t>& docIDs, const std::string& columnName,
      const std::vector<std::string>& tokens, std::vector<double>& values) const;
  void UnmapLRUDataFiles();
  void AddToDeleteVector(std::uint64_t id);

 private:
  void PopulateColumnTypes(
      const std::vector<IndexInfoImpl*>& indexes,
      const DocumentSchema& documentSchema,
      std::unordered_map<std::string, FieldType>& columnTypes);
  std::unique_ptr<sqlite3, void (*)(sqlite3*)> m_dbConnection;
  std::unique_ptr<IndexManager> m_indexManager;
  std::shared_ptr<DocumentSchema> m_documentSchema;
  DocumentIDGenerator m_documentIDGenerator;
  std::vector<BlobMetadata> m_documentIDMap;
  std::string m_name;
  std::unique_ptr<BlobManager> m_blobManager;
  std::unique_ptr<DeleteVector> m_deleteVector;
};
}  // namespace jonoondb_api

