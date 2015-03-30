#pragma once

#include "enums.h"

namespace jonoondb_api {
// Forward Declaration
class Status;
class Buffer;
class Document;

class DocumentFactory {
 public:
  static Status CreateDocument(const char* schema, int schemaID, const Buffer& buffer,
                               SchemaType schemaType, Document*& document);
 private:
  DocumentFactory() = delete;
  DocumentFactory(const DocumentFactory&) = delete;
  DocumentFactory(DocumentFactory&&) = delete;
};
}
