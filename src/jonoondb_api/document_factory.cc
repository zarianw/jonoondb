#include <string>
#include "document_factory.h"
#include "flatbuffers_document.h"
#include "status.h"
#include "document_schema.h"
#include "flatbuffers_document_schema.h"

using namespace std;
using namespace jonoondb_api;

Status DocumentFactory::CreateDocument(
    const shared_ptr<DocumentSchema> documentSchema, const Buffer& buffer,
    Document*& document) {
  Status sts;
  switch (documentSchema->GetSchemaType()) {
    case SchemaType::FLAT_BUFFERS: {
      shared_ptr<FlatbuffersDocumentSchema> fbDocSchema = dynamic_pointer_cast
          < FlatbuffersDocumentSchema > (documentSchema);
      if (!fbDocSchema) {
        // This means that the passed in doc cannot be casted to FlatbuffersDocument    
        string errorMsg = "Argument documentSchema cannot be casted to "
            "underlying DocumentSchema implementation i.e. "
            "FlatbuffersDocumentSchema";
        return Status(kStatusInvalidArgumentCode, errorMsg.c_str(),
                      errorMsg.length());
      }
      FlatbuffersDocument* fbDoc;
      sts = FlatbuffersDocument::Construct(fbDocSchema, buffer, fbDoc);
      if (!sts.OK()) {
        return sts;
      }
      document = fbDoc;
      break;
    }
    default:
      string errorMsg = "Unknown schema.";
      return Status(kStatusGenericErrorCode, errorMsg.c_str(),
                    errorMsg.length());
  }

  return sts;
}
