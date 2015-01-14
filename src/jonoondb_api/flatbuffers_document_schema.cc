#include "flatbuffers_document_schema.h"
#include "enums.h"
#include "status.h"

using namespace jonoondb_api;

FlatbuffersDocumentSchema::FlatbuffersDocumentSchema(const char* schemaText) {
}

FlatbuffersDocumentSchema::~FlatbuffersDocumentSchema() {
}

Status FlatbuffersDocumentSchema::Construct(const char* schemaText,
                                            FlatbuffersDocumentSchema*& documentSchema) {
  documentSchema = new FlatbuffersDocumentSchema(schemaText);
  return Status();
}

const char* FlatbuffersDocumentSchema::GetSchemaText() const {
  return "";
}

Status FlatbuffersDocumentSchema::GetColumnType(const char* columnName, ColumnType& columnType) const {
  return Status();
}

