#pragma once

#include <cstdint>

#include "buffer.h"
#include "enums.h"

namespace jonoondb_api {
// Forward Declaration
class Status;

class Document {
public:
  virtual ~Document() {}
  
  virtual Status GetScalarValueAsInt8(const char* fieldName, int8_t& val) const = 0;
  virtual Status GetScalarValueAsInt16(const char* fieldName, int16_t& val) const = 0;
  virtual Status GetScalarValueAsInt32(const char* fieldName, int32_t& val) const = 0;
  virtual Status GetScalarValueAsInt64(const char* fieldName, int64_t& val) const = 0;

  virtual Status GetScalarValueAsUInt8(const char* fieldName, uint8_t& val) const = 0;
  virtual Status GetScalarValueAsUInt16(const char* fieldName, uint16_t& val) const = 0;
  virtual Status GetScalarValueAsUInt32(const char* fieldName, uint32_t& val) const = 0;
  virtual Status GetScalarValueAsUInt64(const char* fieldName, uint64_t& val) const = 0;

  virtual Status GetScalarValueAsFloat(const char* fieldName, float& val) const = 0;
  virtual Status GetScalarValueAsDouble(const char* fieldName, double& val) const = 0;

  virtual Status GetStringValue(const char* fieldName, char*& val) const = 0;
  virtual Status GetDocumentValue(const char* fieldName,
    Document*& val) const = 0;
  virtual Status AllocateSubDocument(Document*& doc) const = 0;
  virtual void Dispose() = 0;
};
}
