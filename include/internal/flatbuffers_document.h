#pragma once

#include <memory>
#include "document.h"
#include "flatbuffers/idl.h"

namespace jonoondb_api {

// Forward declarations
class Status;
class FlatbuffersDocumentSchema;

class FlatbuffersDocument final : public Document {
 public:
  FlatbuffersDocument(
      const std::shared_ptr<FlatbuffersDocumentSchema>& fbDocumentSchema,
      const BufferImpl& buffer);

  int8_t GetScalarValueAsInt8(const std::string& fieldName) const override;
  int16_t GetScalarValueAsInt16(const std::string& fieldName) const override;
  int32_t GetScalarValueAsInt32(const std::string& fieldName) const override;
  int64_t GetScalarValueAsInt64(const std::string& fieldName) const override;

  uint8_t GetScalarValueAsUInt8(const std::string& fieldName) const override;
  uint16_t GetScalarValueAsUInt16(const std::string& fieldName) const override;
  uint32_t GetScalarValueAsUInt32(const std::string& fieldName) const override;
  uint64_t GetScalarValueAsUInt64(const std::string& fieldName) const override;

  float GetScalarValueAsFloat(const std::string& fieldName) const override;
  double GetScalarValueAsDouble(const std::string& fieldName) const override;

  const char* GetStringValue(const std::string& fieldName) const override;
  void GetDocumentValue(const std::string& fieldName, Document& val) const override;
  std::unique_ptr<Document> AllocateSubDocument() const override;  

  std::unique_ptr<flatbuffers::DynamicTableReader> m_dynTableReader;

 private:
  FlatbuffersDocument(
     const std::shared_ptr<FlatbuffersDocumentSchema> m_fbDocumentSchema,
     std::unique_ptr<flatbuffers::DynamicTableReader> dynTableReader);
  std::string GetMissingFieldErrorString(const std::string& fieldName) const;
  const std::shared_ptr<FlatbuffersDocumentSchema> m_fbDcumentSchema;

};

}  // jonoondb_api
