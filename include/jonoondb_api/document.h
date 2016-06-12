#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "enums.h"

namespace jonoondb_api {
class Document {
 public:
  virtual ~Document() {
  }

  virtual std::string GetStringValue(const std::string& fieldName) const = 0;
  virtual const char*
      GetStringValue(const std::string& fieldName, std::size_t& size) const = 0;

  virtual std::int64_t
      GetIntegerValueAsInt64(const std::string& fieldName) const = 0;
  virtual double
      GetFloatingValueAsDouble(const std::string& fieldName) const = 0;

  virtual void
      GetDocumentValue(const std::string& fieldName, Document& val) const = 0;
  virtual std::unique_ptr<Document> AllocateSubDocument() const = 0;
  virtual void
      VerifyFieldForRead(const std::string& fieldName, FieldType type) const = 0;
};

class DocumentUtils {
 public:
  static std::unique_ptr<Document> GetSubDocumentRecursively(const Document& parentDoc,
                                                             const std::vector<
                                                                 std::string>& tokens) {
    auto doc = parentDoc.AllocateSubDocument();
    for (size_t i = 0; i < tokens.size() - 1; i++) {
      if (i == 0) {
        parentDoc.GetDocumentValue(tokens[i], *doc.get());
      } else {
        doc->GetDocumentValue(tokens[i], *doc.get());
      }
    }

    return doc;
  }
};
}
