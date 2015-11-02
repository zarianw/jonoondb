#include <sstream>
#include <cstdint>
#include <memory>
#include "flatbuffers_document.h"
#include "flatbuffers_document_schema.h"

using namespace std;
using namespace jonoondb_api;
using namespace flatbuffers;

FlatbuffersDocument::FlatbuffersDocument(
  const std::shared_ptr<FlatbuffersDocumentSchema>& fbDocumentSchema, const Buffer& buffer) :
  m_fbDcumentSchema(fbDocumentSchema) {
  Table* table = const_cast<Table*>(flatbuffers::GetRoot<Table>(buffer.GetData()));
  m_dynTableReader.reset(new DynamicTableReader(table,
                                                m_fbDcumentSchema->GetRootStruct(),
                                                m_fbDcumentSchema->GetChildStructs()));
}

Status FlatbuffersDocument::GetScalarValueAsInt8(const char* fieldName,
                                                 int8_t& val) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    return GetMissingFieldErrorStatus(fieldName);
  }

  val = m_dynTableReader->GetScalarValueAs < int8_t > (fieldDef);
  return Status();
}

Status FlatbuffersDocument::GetScalarValueAsInt16(const char* fieldName,
                                                  int16_t& val) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    return GetMissingFieldErrorStatus(fieldName);
  }

  val = m_dynTableReader->GetScalarValueAs < int16_t > (fieldDef);
  return Status();
}

Status FlatbuffersDocument::GetScalarValueAsInt32(const char* fieldName,
                                                  int32_t& val) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    return GetMissingFieldErrorStatus(fieldName);
  }

  val = m_dynTableReader->GetScalarValueAs < int32_t > (fieldDef);
  return Status();
}

Status FlatbuffersDocument::GetScalarValueAsInt64(const char* fieldName,
                                                  int64_t& val) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    return GetMissingFieldErrorStatus(fieldName);
  }

  val = m_dynTableReader->GetScalarValueAs < int64_t > (fieldDef);
  return Status();
}

Status FlatbuffersDocument::GetScalarValueAsUInt8(const char* fieldName,
                                                  uint8_t& val) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    return GetMissingFieldErrorStatus(fieldName);
  }

  val = m_dynTableReader->GetScalarValueAs < uint8_t > (fieldDef);
  return Status();
}

Status FlatbuffersDocument::GetScalarValueAsUInt16(const char* fieldName,
                                                   uint16_t& val) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    return GetMissingFieldErrorStatus(fieldName);
  }

  val = m_dynTableReader->GetScalarValueAs < uint16_t > (fieldDef);
  return Status();
}

Status FlatbuffersDocument::GetScalarValueAsUInt32(const char* fieldName,
                                                   uint32_t& val) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    return GetMissingFieldErrorStatus(fieldName);
  }

  val = m_dynTableReader->GetScalarValueAs < uint32_t > (fieldDef);
  return Status();
}

Status FlatbuffersDocument::GetScalarValueAsUInt64(const char* fieldName,
                                                   uint64_t& val) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    return GetMissingFieldErrorStatus(fieldName);
  }

  val = m_dynTableReader->GetScalarValueAs < uint64_t > (fieldDef);
  return Status();
}

Status FlatbuffersDocument::GetScalarValueAsFloat(const char* fieldName,
                                                  float& val) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    return GetMissingFieldErrorStatus(fieldName);
  }

  val = m_dynTableReader->GetScalarValueAs<float>(fieldDef);
  return Status();
}

Status FlatbuffersDocument::GetScalarValueAsDouble(const char* fieldName,
                                                   double& val) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    return GetMissingFieldErrorStatus(fieldName);
  }

  val = m_dynTableReader->GetScalarValueAs<double>(fieldDef);
  return Status();
}

Status FlatbuffersDocument::GetStringValue(const char* fieldName,
                                           char*& val) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    return GetMissingFieldErrorStatus(fieldName);
  }

  val = const_cast<char*>(m_dynTableReader->GetStringValue(fieldDef));
  return Status();
}

Status FlatbuffersDocument::GetDocumentValue(const char* fieldName,
                                             Document*& val) const {
  FlatbuffersDocument* fbDoc = dynamic_cast<FlatbuffersDocument*>(val);
  if (fbDoc == nullptr) {
    // This means that the passed in doc cannot be casted to FlatbuffersDocument    
    string errorMsg = "Argument val cannot be casted to underlying document "
        "implementation i.e. FlatbuffersDocument. "
        "Make sure you are creating the val by calling AllocateDocument call.";
    return Status(kStatusInvalidArgumentCode, errorMsg.c_str(),
                  __FILE__, "", __LINE__);
  }

  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    return GetMissingFieldErrorStatus(fieldName);
  }

  m_dynTableReader->GetTableValue(fieldDef, *fbDoc->m_dynTableReader.get());

  return Status();
}

Status FlatbuffersDocument::AllocateSubDocument(Document*& doc) const {
  try {
    doc = new FlatbuffersDocument(
        m_fbDcumentSchema,
        move(unique_ptr < DynamicTableReader > (new DynamicTableReader())));
  } catch (bad_alloc&) {
    // Memory allocation failed
    string errorMsg = "Memory allocation failed.";
    return Status(kStatusOutOfMemoryErrorCode, errorMsg.c_str(),
                  __FILE__, "", __LINE__);
  }

  return Status();
}

void FlatbuffersDocument::Dispose() {
  delete this;
}

FlatbuffersDocument::FlatbuffersDocument(
  const shared_ptr<FlatbuffersDocumentSchema> fbDocumentSchema,
  unique_ptr<DynamicTableReader> dynTableReader)
  : m_fbDcumentSchema(fbDocumentSchema),
  m_dynTableReader(move(dynTableReader)) {
}

Status FlatbuffersDocument::GetMissingFieldErrorStatus(
    const char* fieldName) const {
  ostringstream ss;
  ss << "Field definition for " << fieldName
     << " not found in the parsed schema.";
  string errorMsg = ss.str();
  return Status(kStatusGenericErrorCode, errorMsg.c_str(), __FILE__, "", __LINE__);
}
