#include "flatbuffers_document_schema.h"
#include "enums.h"
#include "status.h"

using namespace jonoondb_api;

Status FlatbuffersDocumentSchema::Construct(const char* schemaText) {
  return Status();
}

const char* FlatbuffersDocumentSchema::GetSchemaText() {
  return "";
}

Status FlatbuffersDocumentSchema::GetColumnType(const char* columnName, ColumnType& columnType) {
  return Status();
}

