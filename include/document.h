#pragma once

#include <cstdint>

#include "buffer.h"
#include "enums.h"

namespace jonoondb_api {
// Forward Declaration
class Status;

class Document {
 public:
  virtual void GetDocumentSequenceId() = 0;
  virtual void SetDocumentSequenceId() = 0;

  virtual Status GetInt32(const char* fieldName, int32_t& value) = 0;
  virtual Status SetInt32(const char* fieldName, int32_t& value) = 0;

  virtual Status GetSubDocument(const char* fieldName, int32_t& value) = 0;
  virtual Status SetSubDocument(const char* fieldName, int32_t& value) = 0;

 private:
  Document();
  Document(const Document&) = delete;
};
}
