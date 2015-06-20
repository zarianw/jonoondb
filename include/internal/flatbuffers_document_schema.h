#pragma once

#include <cstdint>
#include <string>
#include "flatbuffers/idl.h"
#include "document_schema.h"

namespace jonoondb_api {
//Forward declaration
class Status;
enum class FieldType
: std::int32_t;
enum class SchemaType
  : std::int32_t;

class FlatbuffersDocumentSchema final : public DocumentSchema {
 public:
  ~FlatbuffersDocumentSchema() override;
  static Status Construct(const char* schemaText, SchemaType schemaType,
                          FlatbuffersDocumentSchema*& documentSchema);
  const char* GetSchemaText() const override;  
  SchemaType GetSchemaType() const override;
  Status GetFieldType(const char* fieldName,
    FieldType& fieldType) const override;
  const flatbuffers::StructDef* GetRootStruct() const;
  const flatbuffers::SymbolTable<flatbuffers::StructDef>* GetChildStructs() const;
 private:
  FlatbuffersDocumentSchema(const char* schemaText, SchemaType schemaType,
                            std::unique_ptr<flatbuffers::Parser> parser);
  FlatbuffersDocumentSchema(const FlatbuffersDocumentSchema&) = delete;
  FlatbuffersDocumentSchema(FlatbuffersDocumentSchema&&) = delete;

  FieldType MapFlatbuffersToJonoonDBType(flatbuffers::BaseType flatbuffersType) const;
  Status GetMissingFieldErrorStatus(const char* fieldName) const;

  std::string m_schemaText;
  std::unique_ptr<flatbuffers::Parser> m_parser;
  SchemaType m_schemaType;
};
}  // jonoondb_api
