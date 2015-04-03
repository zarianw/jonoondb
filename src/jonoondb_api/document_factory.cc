#include <string>
#include "document_factory.h"
#include "flatbuffers_document.h"
#include "status.h"

using namespace std;
using namespace jonoondb_api;

Status DocumentFactory::CreateDocument(const char* schema,
                                       int schemaID,
                                       const Buffer& buffer,
                                       SchemaType schemaType,
                                       Document*& document) {
  Status sts;
  switch (schemaType) {
    case SchemaType::FLAT_BUFFERS: {      
      FlatbuffersDocument* fbDoc;
      sts = FlatbuffersDocument::Construct(schema, schemaID, buffer, fbDoc);
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