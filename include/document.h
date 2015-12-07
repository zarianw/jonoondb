#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "buffer_impl.h"
#include "enums.h"

namespace jonoondb_api {
// Forward Declaration
class Status;

class Document {
 public:
  virtual ~Document() {
  }

  virtual std::int8_t GetScalarValueAsInt8(const std::string& fieldName) const = 0;
  virtual std::int16_t GetScalarValueAsInt16(const std::string& fieldName) const = 0;
  virtual std::int32_t GetScalarValueAsInt32(const std::string& fieldName) const = 0;
  virtual std::int64_t GetScalarValueAsInt64(const std::string& fieldName) const = 0;

  virtual std::uint8_t GetScalarValueAsUInt8(const std::string& fieldName) const = 0;
  virtual std::uint16_t GetScalarValueAsUInt16(const std::string& fieldName) const = 0;
  virtual std::uint32_t GetScalarValueAsUInt32(const std::string& fieldName) const = 0;
  virtual std::uint64_t GetScalarValueAsUInt64(const std::string& fieldName) const = 0;

  virtual float GetScalarValueAsFloat(const std::string& fieldName) const = 0;
  virtual double GetScalarValueAsDouble(const std::string& fieldName) const = 0;

  virtual std::string GetStringValue(const std::string& fieldName) const = 0;
  virtual void GetDocumentValue(const std::string& fieldName, Document& val) const = 0;
  virtual std::unique_ptr<Document> AllocateSubDocument() const = 0;  
  virtual void VerifyFieldForRead(const std::string& fieldName, FieldType type) const = 0;
};

class DocumentUtils {
public:
  static std::unique_ptr<Document> GetSubDocumentRecursively(const Document& parentDoc,
                                                      const std::vector<std::string>& tokens) {
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
