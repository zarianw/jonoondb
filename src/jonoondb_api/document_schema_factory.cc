#include <string>
#include "document_schema_factory.h"
#include "status.h"
#include "flatbuffers_document_schema.h"

using namespace jonoondb_api;

DocumentSchema* DocumentSchemaFactory::CreateDocumentSchema(
    const std::string& schemaText, SchemaType schemaType) {
  DocumentSchema* documentSchema;
  switch (schemaType) {
    case SchemaType::FLAT_BUFFERS: {
      FlatbuffersDocumentSchema* fbDocSchema;
      return new FlatbuffersDocumentSchema(schemaText, schemaType);      
    }
    default:
      break;
  }  
}
