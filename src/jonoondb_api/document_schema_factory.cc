#include "document_schema_factory.h"
#include "status.h"
#include "flatbuffers_document_schema.h"

using namespace jonoondb_api;

Status DocumentSchemaFactory::CreateDocumentSchema(
    const char* schemaText, SchemaType schemaType,
    DocumentSchema*& documentSchema) {
  switch (schemaType) {
    case SchemaType::FLAT_BUFFERS: {
      FlatbuffersDocumentSchema* fbDocSchema;
      auto sts = FlatbuffersDocumentSchema::Construct(schemaText, fbDocSchema);
      if (!sts.OK()) {
        return sts;
      }

      documentSchema = reinterpret_cast<DocumentSchema*>(fbDocSchema);
      break;
    }
    default:
      break;
  }

  return Status();
}
