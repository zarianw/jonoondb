#pragma once

#include "enums.h"

namespace jonoondb_api {
// Forward Declaration
class Status;
class DocumentSchema;

class DocumentSchemaFactory {
 public:
   static DocumentSchema* DocumentSchemaFactory::CreateDocumentSchema(
     const std::string& schemaText, SchemaType schemaType);
 private:
  DocumentSchemaFactory() = delete;
  DocumentSchemaFactory(const DocumentSchemaFactory&) = delete;
  DocumentSchemaFactory(DocumentSchemaFactory&&) = delete;
};
}
