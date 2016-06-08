#include <string>
#include <sstream>
#include "document_schema_factory.h"
#include "flatbuffers_document_schema.h"

using namespace jonoondb_api;

DocumentSchema* DocumentSchemaFactory::CreateDocumentSchema(
    const std::string& schemaText, SchemaType schemaType) {
  switch (schemaType) {
    case SchemaType::FLAT_BUFFERS: {
      return new FlatbuffersDocumentSchema(schemaText);
    }
    default:
      std::ostringstream ss;
      ss << "Cannot create DocumentSchema. Schema type '"
          << static_cast<int32_t>(schemaType) << "' is unknown.";
      throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }
}
