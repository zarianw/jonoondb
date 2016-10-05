#pragma once

#include "enums.h"
#include "jonoondb_api_export.h"

namespace jonoondb_api {
// Forward Declaration
class DocumentSchema;

class DocumentSchemaFactory {
 public:
  static DocumentSchema* CreateDocumentSchema(
      const std::string& schemaText, SchemaType schemaType);
 private:
  DocumentSchemaFactory() = delete;
  DocumentSchemaFactory(const DocumentSchemaFactory&) = delete;
  DocumentSchemaFactory(DocumentSchemaFactory&&) = delete;
};
}
