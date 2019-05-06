#include "jonoondb_api/document_factory.h"
#include <string>
#include "jonoondb_api/document_schema.h"
#include "jonoondb_api/flatbuffers_document.h"
#include "jonoondb_api/flatbuffers_document_schema.h"
#include "jonoondb_api/jonoondb_exceptions.h"

using namespace std;
using namespace jonoondb_api;

std::unique_ptr<Document> DocumentFactory::CreateDocument(
    const DocumentSchema& documentSchema, const BufferImpl& buffer) {
  switch (documentSchema.GetSchemaType()) {
    case SchemaType::FLAT_BUFFERS: {
      try {
        auto& fbDocSchema =
            dynamic_cast<const FlatbuffersDocumentSchema&>(documentSchema);
        return std::unique_ptr<Document>(new FlatbuffersDocument(
            const_cast<FlatbuffersDocumentSchema*>(&fbDocSchema),
            const_cast<BufferImpl*>(&buffer)));
      } catch (std::bad_cast) {
        // This means that the passed in doc schema cannot be casted to
        // FlatbuffersDocumentSchema
        string errorMsg =
            "Argument documentSchema cannot be casted to "
            "underlying DocumentSchema implementation i.e. "
            "FlatbuffersDocumentSchema";
        throw InvalidArgumentException(errorMsg, __FILE__, __func__, __LINE__);
      }
    }
    default:
      std::ostringstream ss;
      ss << "Cannot create Document. Schema type '"
         << static_cast<int32_t>(documentSchema.GetSchemaType())
         << "' is unknown.";
      throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }
}
