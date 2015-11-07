#pragma once

#include <memory>
#include "enums.h"

namespace jonoondb_api {
// Forward Declaration
class Status;
class BufferImpl;
class Document;
class DocumentSchema;

class DocumentFactory {
 public:
  static Document* CreateDocument(
      const std::shared_ptr<DocumentSchema>& documentSchema,
      const BufferImpl& buffer);
 private:
  DocumentFactory() = delete;
  DocumentFactory(const DocumentFactory&) = delete;
  DocumentFactory(DocumentFactory&&) = delete;
};
}
