#include "document_schema_factory.h"
#include "status.h"

using namespace jonoondb_api;

Status DocumentSchemaFactory::CreateDocumentSchema(const char* schemaText,
                                                   SchemaType schemaType,
                                                   DocumentSchema*& document) {
  switch (schemaType)
  {
  case jonoondb_api::SchemaType::FLAT_BUFFERS:
    break;
  default:
    break;
  }

  return Status();
}