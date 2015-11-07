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
  static FieldType MapFlatbuffersToJonoonDBType(
      flatbuffers::BaseType flatbuffersType);
  ~FlatbuffersDocumentSchema() override;
  FlatbuffersDocumentSchema(const std::string& schemaText, SchemaType schemaType);
  const char* GetSchemaText() const override;
  SchemaType GetSchemaType() const override;
  Status GetFieldType(const char* fieldName, FieldType& fieldType) const
      override;
  std::size_t GetRootFieldCount() const override;
  Status GetRootField(size_t index, Field*& field) const override;
  Status AllocateField(Field*& field) const override;
  const flatbuffers::StructDef* GetRootStruct() const;
  const flatbuffers::SymbolTable<flatbuffers::StructDef>* GetChildStructs() const;
 private:  
  FlatbuffersDocumentSchema(const FlatbuffersDocumentSchema&) = delete;
  FlatbuffersDocumentSchema(FlatbuffersDocumentSchema&&) = delete;

  Status GetMissingFieldErrorStatus(const char* fieldName) const;
  Status GetInvalidStructFieldErrorStatus(const char* fieldName,
                                          const char* fullName) const;

  std::string m_schemaText;
  std::unique_ptr<flatbuffers::Parser> m_parser;
  SchemaType m_schemaType;
};
}  // jonoondb_api
