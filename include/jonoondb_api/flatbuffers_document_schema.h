#pragma once

#include <cstdint>
#include <string>
#include "flatbuffers/idl.h"
#include "document_schema.h"
#include "jonoondb_api_export.h"

namespace jonoondb_api {
//Forward declaration
enum class FieldType
    : std::int8_t;
enum class SchemaType
    : std::int32_t;

class JONOONDB_API_EXPORT FlatbuffersDocumentSchema final: public DocumentSchema {
 public:
  FlatbuffersDocumentSchema(const FlatbuffersDocumentSchema&) = delete;
  FlatbuffersDocumentSchema(FlatbuffersDocumentSchema&&) = delete;
  static FieldType MapFlatbuffersToJonoonDBType(
      reflection::BaseType flatbuffersType);
  ~FlatbuffersDocumentSchema() override;
  FlatbuffersDocumentSchema(std::string binarySchema);
  const std::string& GetSchemaText() const override;
  SchemaType GetSchemaType() const override;
  FieldType GetFieldType(const std::string& fieldName) const override;
  std::size_t GetRootFieldCount() const override;
  void GetRootField(size_t index, Field*& field) const override;
  Field* AllocateField() const override;
 private:
  std::string m_binarySchema;
  reflection::Schema* m_schema;
};
}  // jonoondb_api
