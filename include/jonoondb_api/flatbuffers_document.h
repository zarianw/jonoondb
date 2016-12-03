#pragma once

#include <memory>
#include "flatbuffers/reflection.h"
#include "document.h"

namespace jonoondb_api {

// Forward declarations
class FlatbuffersDocumentSchema;
class BufferImpl;

class FlatbuffersDocument final: public Document {
 public:
  FlatbuffersDocument(
      FlatbuffersDocumentSchema* fbDocumentSchema, BufferImpl* buffer);

  std::string GetStringValue(const std::string& fieldName) const override;
  const char* GetStringValue(const std::string& fieldName,
                             std::size_t& size) const override;

  std::int64_t
      GetIntegerValueAsInt64(const std::string& fieldName) const override;
  double GetFloatingValueAsDouble(const std::string& fieldName) const override;

  bool TryGetDocumentValue(const std::string& fieldName, Document& val) const
      override;

  const char* GetBlobValue(const std::string& fieldName,
                           std::size_t& size) const override;

  std::unique_ptr<Document> AllocateSubDocument() const override;
  void VerifyFieldForRead(const std::string& fieldName, FieldType type) const
      override;

  void SetMembers(FlatbuffersDocumentSchema* schema, BufferImpl* buffer,
                  reflection::Object* obj, flatbuffers::Table* table);
  const BufferImpl* GetRawBuffer() const override;
  bool Verify() const override;

 private:
  FlatbuffersDocument();
  bool VerifyVector(flatbuffers::Verifier& v, const flatbuffers::Table* table,
                    const reflection::Field* vecField) const;
  bool VerifyObject(flatbuffers::Verifier& v, const flatbuffers::Table* table,
                    const reflection::Object* obj, bool isRequired) const;
  std::string GetMissingFieldErrorString(const std::string& fieldName) const;
  FlatbuffersDocumentSchema* m_fbDcumentSchema = nullptr;
  BufferImpl* m_buffer = nullptr;
  reflection::Object* m_obj = nullptr;
  flatbuffers::Table* m_table = nullptr;
};

}  // jonoondb_api
