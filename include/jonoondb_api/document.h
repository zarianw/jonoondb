#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "enums.h"
#include "null_helpers.h"
#include "buffer_impl.h"

namespace jonoondb_api {
class Document {
 public:
  virtual ~Document() {
  }

  virtual std::string GetStringValue(const std::string& fieldName) const = 0;
  virtual const char* GetStringValue(const std::string& fieldName,
                                     std::size_t& size) const = 0;
  virtual std::int64_t
      GetIntegerValueAsInt64(const std::string& fieldName) const = 0;
  virtual double
      GetFloatingValueAsDouble(const std::string& fieldName) const = 0;
  virtual bool
      TryGetDocumentValue(const std::string& fieldName, Document& val) const = 0;
  virtual const char* GetBlobValue(const std::string& fieldName,
                                   std::size_t& size) const = 0;
  virtual std::unique_ptr<Document> AllocateSubDocument() const = 0;
  virtual void VerifyFieldForRead(const std::string& fieldName,
                                  FieldType type) const = 0;
  virtual const BufferImpl* GetRawBuffer() const = 0;
  virtual bool Verify() const = 0;
};

class DocumentUtils {
public:
  static bool TryGetSubDocumentRecursively(
    const Document& parentDoc, const std::vector<std::string>& tokens,
    Document& subDoc) {
    for (size_t i = 0; i < tokens.size() - 1; i++) {
      if (i == 0) {
        if (!parentDoc.TryGetDocumentValue(tokens[i], subDoc)) {
          return false;
        }
      } else {
        if (!subDoc.TryGetDocumentValue(tokens[i], subDoc)) {
          return false;
        }
      }
    }

    return tokens.size() > 0;
  }

  static const char* GetBlobValue(const Document& document,
                                  std::unique_ptr<Document>& subDoc,
                                  const std::vector<std::string>& tokens,
                                  std::size_t& size) {
    if (tokens.size() > 1) {
      if (!subDoc) {
        subDoc = document.AllocateSubDocument();
      }
      if (!DocumentUtils::TryGetSubDocumentRecursively(document,
                                                       tokens,
                                                       *subDoc.get())) {
        size = 0;
        return nullptr;
      }

      return subDoc->GetBlobValue(tokens.back(), size);
    } else {
      if (tokens.back() == "_document") {
        auto buffer = document.GetRawBuffer();
        size = buffer->GetLength();
        return buffer->GetData();
      } else {
        return document.GetBlobValue(tokens.back(), size);
      }
    }
  }

  static int64_t GetIntegerValue(const Document& document,
                                 std::unique_ptr<Document>& subDoc,
                                 const std::vector<std::string>& tokens) {
    if (tokens.size() > 1) {
      if (!subDoc) {
        subDoc = document.AllocateSubDocument();
      }
      if (!DocumentUtils::TryGetSubDocumentRecursively(document,
                                                       tokens,
                                                       *subDoc.get())) {
        return JONOONDB_NULL_INT64;
      }
      return subDoc->GetIntegerValueAsInt64(tokens.back());
    } else {
      return document.GetIntegerValueAsInt64(tokens.back());
    }
  }

  static double GetFloatValue(const Document& document,
                              std::unique_ptr<Document>& subDoc,
                              const std::vector<std::string>& tokens) {
    if (tokens.size() > 1) {
      if (!subDoc) {
        subDoc = document.AllocateSubDocument();
      }
      if (!DocumentUtils::TryGetSubDocumentRecursively(document,
                                                       tokens,
                                                       *subDoc.get())) {
        return JONOONDB_NULL_DOUBLE;
      }
      return subDoc->GetFloatingValueAsDouble(tokens.back());
    } else {
      return document.GetFloatingValueAsDouble(tokens.back());
    }
  }

  // Todo: Need to avoid the string creation/copy cost
  static std::string GetStringValue(const Document& document,
                                    std::unique_ptr<Document>& subDoc,
                                    const std::vector<std::string>& tokens) {
    if (tokens.size() > 1) {
      if (!subDoc) {
        subDoc = document.AllocateSubDocument();
      }
      if (!DocumentUtils::TryGetSubDocumentRecursively(document,
                                                       tokens,
                                                       *subDoc.get())) {
        return JONOONDB_NULL_STR;
      }
      return subDoc->GetStringValue(tokens.back());
    } else {
      return document.GetStringValue(tokens.back());
    }
  }
};
}
