#pragma once

#include <cstdint>
#include <string>
#include "flatbuffers/idl.h"
#include "document_schema.h"

namespace jonoondb_api {
//Forward declaration
enum class FieldType
: std::int8_t;
enum class SchemaType
: std::int32_t;

class FlatbuffersDocumentSchema final : public DocumentSchema {
 public:
  static FieldType MapFlatbuffersToJonoonDBType(
      flatbuffers::BaseType flatbuffersType);
  ~FlatbuffersDocumentSchema() override;
  FlatbuffersDocumentSchema(const std::string& schemaText, SchemaType schemaType);
  const std::string& GetSchemaText() const override;
  SchemaType GetSchemaType() const override;
  FieldType GetFieldType(const std::string& fieldName) const override;
  std::size_t GetRootFieldCount() const override;
  void GetRootField(size_t index, Field*& field) const override;
  Field* AllocateField() const override;
  const flatbuffers::StructDef* GetRootStruct() const;
  const flatbuffers::SymbolTable<flatbuffers::StructDef>* GetChildStructs() const;
 private:  
  FlatbuffersDocumentSchema(const FlatbuffersDocumentSchema&) = delete;
  FlatbuffersDocumentSchema(FlatbuffersDocumentSchema&&) = delete;

  std::string m_schemaText;
  std::unique_ptr<flatbuffers::Parser> m_parser;
  SchemaType m_schemaType;
};
}  // jonoondb_api
