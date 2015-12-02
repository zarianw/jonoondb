#pragma once

#include <memory>
#include <unordered_map>
#include <string>
#include <cstdint>
#include "index_manager.h"
#include "document_id_generator.h"
#include "blob_metadata.h"

// Forward declaration
struct sqlite3;

namespace jonoondb_api {
// Forward Declaration
class Status;
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

class DocumentCollection {
 public:
   DocumentCollection(const std::string& databaseMetadataFilePath,
     const std::string& name, SchemaType schemaType,
     const std::string& schema, const std::vector<IndexInfoImpl*>& indexes);
  ~DocumentCollection();

  Status Insert(const BufferImpl& documentData);
  const std::string& GetName();
  const std::shared_ptr<DocumentSchema>& GetDocumentSchema();
  bool TryGetBestIndex(const std::string& columnName, IndexConstraintOperator op,
                       IndexStat& indexStat);
  std::shared_ptr<MamaJenniesBitmap> Filter(const std::vector<Constraint>& constraints);

 private:
  void PopulateColumnTypes(
      const std::vector<IndexInfoImpl*>& indexes,
      const DocumentSchema& documentSchema,
      std::unordered_map<std::string, FieldType>& columnTypes);
  sqlite3* m_dbConnection;
  std::unique_ptr<IndexManager> m_indexManager;
  std::shared_ptr<DocumentSchema> m_documentSchema;
  DocumentIDGenerator m_documentIDGenerator;
  std::unordered_map<std::uint64_t, BlobMetadata> m_documentIDMap;
  std::string m_name;
};
}  // namespace jonoondb_api

