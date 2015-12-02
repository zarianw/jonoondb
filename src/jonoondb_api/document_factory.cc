#include <string>
#include "document_factory.h"
#include "flatbuffers_document.h"
#include "status.h"
#include "document_schema.h"
#include "flatbuffers_document_schema.h"

using namespace std;
using namespace jonoondb_api;

std::unique_ptr<Document> DocumentFactory::CreateDocument(
    const shared_ptr<DocumentSchema>& documentSchema, const BufferImpl& buffer) {
  switch (documentSchema->GetSchemaType()) {
    case SchemaType::FLAT_BUFFERS: {
      auto fbDocSchema = dynamic_pointer_cast
          < FlatbuffersDocumentSchema > (documentSchema);
      if (!fbDocSchema) {
        // This means that the passed in doc cannot be casted to FlatbuffersDocument    
        string errorMsg = "Argument documentSchema cannot be casted to "
            "underlying DocumentSchema implementation i.e. "
            "FlatbuffersDocumentSchema";
        throw InvalidArgumentException(errorMsg, __FILE__, "", __LINE__);
      }

      return std::unique_ptr<Document>(new FlatbuffersDocument(fbDocSchema, buffer));
    }
    default:
      std::ostringstream ss;
      ss << "Cannot create Document. Schema type '" << static_cast<int32_t>(documentSchema->GetSchemaType()) << "' is unknown.";
      throw JonoonDBException(ss.str(), __FILE__, "", __LINE__);
  }  
}
