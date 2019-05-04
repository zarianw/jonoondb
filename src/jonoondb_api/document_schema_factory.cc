#include "jonoondb_api/document_schema_factory.h"
#include <sstream>
#include <string>
#include "jonoondb_api/flatbuffers_document_schema.h"
#include "jonoondb_api/jonoondb_exceptions.h"

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
