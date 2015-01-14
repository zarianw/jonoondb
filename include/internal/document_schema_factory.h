#pragma once

#include "enums.h"

namespace jonoondb_api {
// Forward Declaration
class Status;
class DocumentSchema;

class DocumentSchemaFactory {
public:
  static Status CreateDocumentSchema(const char* schemaText, SchemaType schemaType, DocumentSchema*& document);
private:
  DocumentSchemaFactory() = delete;
  DocumentSchemaFactory(const DocumentSchemaFactory&) = delete;
  DocumentSchemaFactory(DocumentSchemaFactory&&) = delete;
};
}