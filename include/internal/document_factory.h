#pragma once

#include <memory>
#include "enums.h"

namespace jonoondb_api {
// Forward Declaration
class Status;
class Buffer;
class Document;
class DocumentSchema;

class DocumentFactory {
 public:
  static Status CreateDocument(
      const std::shared_ptr<DocumentSchema>& documentSchema,
      const Buffer& buffer, Document*& document);
 private:
  DocumentFactory() = delete;
  DocumentFactory(const DocumentFactory&) = delete;
  DocumentFactory(DocumentFactory&&) = delete;
};
}
