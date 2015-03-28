#include <string>
#include "document_factory.h"
#include "status.h"

using namespace std;
using namespace jonoondb_api;

Status DocumentFactory::CreateDocument(const Buffer& buffer, bool deepCopy,
                                       SchemaType schemaType,
                                       Document*& document) {
  switch (schemaType) {
    case SchemaType::FLAT_BUFFERS:
      document = nullptr;
      break;
    default:
      string errorMsg = "Unknown schema";
      return Status(kStatusGenericErrorCode, errorMsg.c_str(),
                    errorMsg.length());
  }
  return Status();
}
