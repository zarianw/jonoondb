#include <sstream>
#include <cstdint>
#include <memory>
#include <flatbuffers/reflection.h>
#include "flatbuffers_document.h"
#include "flatbuffers_document_schema.h"
#include "buffer_impl.h"
#include "null_helpers.h"

using namespace std;
using namespace jonoondb_api;
using namespace flatbuffers;

FlatbuffersDocument::FlatbuffersDocument(
    FlatbuffersDocumentSchema* fbDocumentSchema, BufferImpl* buffer) :
    m_fbDcumentSchema(fbDocumentSchema),
    m_buffer(buffer),
    m_obj(const_cast<reflection::Object*>(reflection::GetSchema(fbDocumentSchema->GetSchemaText().c_str())->root_table())),
    m_table(flatbuffers::GetAnyRoot((uint8_t*) (buffer->GetData()))) {
}

std::string FlatbuffersDocument::GetStringValue(const std::string& fieldName) const {
  auto fieldDef = m_obj->fields()->LookupByKey(fieldName.c_str());
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName),
                            __FILE__, __func__, __LINE__);
  }
  if (fieldDef->type()->base_type() != reflection::BaseType::String) {
    std::ostringstream ss;
    ss << "Field " << fieldName << " has FieldType "
        << fieldDef->type()->base_type()
        << " and it cannot be safely converted into string.";
    throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }

  if (m_table->CheckField(fieldDef->offset())) {
    return flatbuffers::GetAnyFieldS(*m_table, *fieldDef, nullptr);
  } else {
    return JONOONDB_NULL_STR;
  }
}

const char* FlatbuffersDocument::GetStringValue(const std::string& fieldName,
                                                std::size_t& size) const {
  auto fieldDef = m_obj->fields()->LookupByKey(fieldName.c_str());
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName),
                            __FILE__, __func__, __LINE__);
  }
  if (fieldDef->type()->base_type() != reflection::BaseType::String) {
    std::ostringstream ss;
    ss << "Field " << fieldName << " has FieldType "
        << fieldDef->type()->base_type()
        << " and it cannot be safely converted into string.";
    throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }

  auto val = flatbuffers::GetFieldS(*m_table, *fieldDef);
  size = val->size();
  return val->c_str();
}

std::int64_t FlatbuffersDocument::GetIntegerValueAsInt64(const std::string& fieldName) const {
  auto fieldDef = m_obj->fields()->LookupByKey(fieldName.c_str());
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName),
                            __FILE__, __func__, __LINE__);
  }

  if (fieldDef->type()->base_type() == reflection::BaseType::Obj ||
      fieldDef->type()->base_type() == reflection::BaseType::Vector ||
      fieldDef->type()->base_type() == reflection::BaseType::Union) {
    std::ostringstream ss;
    ss << "Field " << fieldName << " has FieldType "
        << fieldDef->type()->base_type()
        << " and it cannot be safely converted into a 64 bit integer.";
    throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }

  return flatbuffers::GetAnyFieldI(*m_table, *fieldDef);
}

double FlatbuffersDocument::GetFloatingValueAsDouble(const std::string& fieldName) const {
  auto fieldDef = m_obj->fields()->LookupByKey(fieldName.c_str());
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName),
                            __FILE__, __func__, __LINE__);
  }

  if (fieldDef->type()->base_type() == reflection::BaseType::Obj ||
      fieldDef->type()->base_type() == reflection::BaseType::Vector ||
      fieldDef->type()->base_type() == reflection::BaseType::Union) {
    std::ostringstream ss;
    ss << "Field " << fieldName << " has FieldType "
        << fieldDef->type()->base_type()
        << " and it cannot be safely converted into a 64 bit floating value.";
    throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }

  return flatbuffers::GetAnyFieldF(*m_table, *fieldDef);
}

bool FlatbuffersDocument::TryGetDocumentValue(const std::string& fieldName,
                                              Document& val) const {  
  auto fieldDef = m_obj->fields()->LookupByKey(fieldName.c_str());
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName),
                            __FILE__, __func__, __LINE__);
  }

  if (fieldDef->type()->base_type() != reflection::BaseType::Obj) {
    std::ostringstream ss;
    ss << "Field " << fieldName << " has FieldType "
      << fieldDef->type()->base_type()
      << " and it cannot be safely converted into a document value.";
    throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }

  auto obj = reflection::GetSchema(
    m_fbDcumentSchema->GetSchemaText().c_str())->
    objects()->Get(fieldDef->type()->index());
  assert(obj != nullptr);

  auto table = flatbuffers::GetFieldT(*m_table, *fieldDef);
  if (!table) {
    // this means that this nested field is null
    return false;
  }

  try {
    // Todo: dynamic_cast can be expensive, this should be optimized.
    FlatbuffersDocument& fbDoc = dynamic_cast<FlatbuffersDocument&>(val);
    fbDoc.SetMembers(m_fbDcumentSchema, m_buffer,
                     const_cast<reflection::Object*>(obj), table);
  } catch (std::bad_cast) {
    // This means that the passed in doc cannot be casted to FlatbuffersDocument    
    string errorMsg = "Argument val cannot be casted to underlying document "
      "implementation i.e. FlatbuffersDocument. "
      "Make sure you are creating the val by calling AllocateDocument call.";
    throw InvalidArgumentException(errorMsg, __FILE__, __func__, __LINE__);
  }

  return true;
}

const char* FlatbuffersDocument::GetBlobValue(const std::string& fieldName,
                                 std::size_t& size) const {
  auto fieldDef = m_obj->fields()->LookupByKey(fieldName.c_str());
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName),
                            __FILE__, __func__, __LINE__);
  }
  if (fieldDef->type()->base_type() != reflection::BaseType::Vector && 
      (fieldDef->type()->element() != reflection::BaseType::Byte || 
       fieldDef->type()->element() != reflection::BaseType::UByte)) {
    std::ostringstream ss;
    ss << "Field " << fieldName << " has FieldType "
      << fieldDef->type()->base_type()
      << " and it cannot be safely converted into blob.";
    throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }

  auto val = flatbuffers::GetFieldV<char>(*m_table, *fieldDef);
  if (val) {
    size = val->size();
    return val->data();
  } else {
    size = 0;
    return nullptr;
  }
}

std::unique_ptr<Document> FlatbuffersDocument::AllocateSubDocument() const {
  return std::unique_ptr<Document>(new FlatbuffersDocument());
}

void FlatbuffersDocument::VerifyFieldForRead(const std::string& fieldName,
                                             FieldType expectedType) const {
  // Make sure field exists
  auto fieldDef = m_obj->fields()->LookupByKey(fieldName.c_str());
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName),
                            __FILE__, __func__, __LINE__);
  }

  // Make sure it has the same type
  FieldType actualType;
  if (fieldDef->type()->base_type() == reflection::BaseType::Vector &&
     (fieldDef->type()->element() == reflection::BaseType::Byte ||
     fieldDef->type()->element() == reflection::BaseType::UByte)) {
    actualType = FieldType::BASE_TYPE_BLOB;
  } else {
    actualType = FlatbuffersDocumentSchema::MapFlatbuffersToJonoonDBType(
      fieldDef->type()->base_type());
  }

  if (actualType != expectedType) {
    ostringstream ss;
    ss << "Actual field type for field " << fieldName << " is "
        << GetFieldString(actualType) <<
        " which is different from the expected field type "
        << GetFieldString(expectedType) << ".";
    throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }
}

const BufferImpl* FlatbuffersDocument::GetRawBuffer() const {
  return m_buffer;
}

void FlatbuffersDocument::SetMembers(FlatbuffersDocumentSchema* schema,
                                     BufferImpl* buffer,
                                     reflection::Object* obj,
                                     flatbuffers::Table* table) {
  m_fbDcumentSchema = schema;
  m_buffer = buffer;
  m_obj = obj;
  m_table = table;
}

bool FlatbuffersDocument::VerifyVector(flatbuffers::Verifier& v,
                                       flatbuffers::Table* table,
                                       const reflection::Field* vecField) const {
  assert(vecField->type()->base_type() == reflection::BaseType::Vector);
  bool isValid = table->VerifyField<uoffset_t>(v, vecField->offset());
  if (!isValid)
    return isValid;

  switch (vecField->type()->element()) {
    case reflection::BaseType::None:
    case reflection::BaseType::UType:
      assert(false);
      break;
    case reflection::BaseType::Bool:
    case reflection::BaseType::Byte:
    case reflection::BaseType::UByte:
      isValid = v.Verify(flatbuffers::GetFieldV<int8_t>(*table, *vecField));
      break;
    case reflection::BaseType::Short:
    case reflection::BaseType::UShort:
      isValid = v.Verify(flatbuffers::GetFieldV<int16_t>(*table, *vecField));
      break;
    case reflection::BaseType::Int:
    case reflection::BaseType::UInt:
      isValid = v.Verify(flatbuffers::GetFieldV<int32_t>(*table, *vecField));
      break;
    case reflection::BaseType::Long:
    case reflection::BaseType::ULong:
      isValid = v.Verify(flatbuffers::GetFieldV<int64_t>(*table, *vecField));
      break;
    case reflection::BaseType::Float:
      isValid = v.Verify(flatbuffers::GetFieldV<float>(*table, *vecField));
      break;
    case reflection::BaseType::Double:
      isValid = v.Verify(flatbuffers::GetFieldV<double>(*table, *vecField));
      break;
    case reflection::BaseType::String: {
      auto vecString =
        flatbuffers::GetFieldV<flatbuffers::
        Offset<flatbuffers::String>>(*table, *vecField);
      isValid = v.Verify(vecString) && v.VerifyVectorOfStrings(vecString);
      break;
    }
    case reflection::BaseType::Vector:
      assert(false);
      break;
    case reflection::BaseType::Obj:
      // TODO
    case reflection::BaseType::Union:
    default:
      break;
  }
}

// TODO: 1 struct, 2 vectorOfTables/vectorOfStructs/vectorOfUnions, Union, 
bool FlatbuffersDocument::VerifyObject(flatbuffers::Verifier& v,
                                       flatbuffers::Table* table,
                                       const reflection::Object* obj) const {
  bool isValid = table->VerifyTableStart(v);
  if (!isValid)
    return isValid;

  for (int i = 0; i < obj->fields()->size(); i++) {
    auto fieldDef = obj->fields()->Get(i);
    switch (fieldDef->type()->base_type()) {
      case reflection::BaseType::None:
      case reflection::BaseType::UType:
        assert(false);
        break;
      case reflection::BaseType::Bool:
      case reflection::BaseType::Byte:
      case reflection::BaseType::UByte:
        isValid = table->VerifyField<int8_t>(v, fieldDef->offset());
        break;
      case reflection::BaseType::Short:
      case reflection::BaseType::UShort:
        isValid = table->VerifyField<int16_t>(v, fieldDef->offset());
        break;
      case reflection::BaseType::Int:
      case reflection::BaseType::UInt:
        isValid = table->VerifyField<int32_t>(v, fieldDef->offset());
        break;
      case reflection::BaseType::Long:
      case reflection::BaseType::ULong:
        isValid = table->VerifyField<int64_t>(v, fieldDef->offset());
        break;
      case reflection::BaseType::Float:
        isValid = table->VerifyField<float>(v, fieldDef->offset());
        break;
      case reflection::BaseType::Double:
        isValid = table->VerifyField<double>(v, fieldDef->offset());
        break;
      case reflection::BaseType::String:
        isValid = table->VerifyField<uoffset_t>(v, fieldDef->offset()) &&
          v.Verify(flatbuffers::GetFieldS(*table, *fieldDef));
        break;
      case reflection::BaseType::Vector:
        isValid = VerifyVector(v, table, fieldDef);
      case reflection::BaseType::Obj: {
        auto obj = reflection::GetSchema(
          m_fbDcumentSchema->GetSchemaText().c_str())->
          objects()->Get(fieldDef->type()->index());
        isValid = VerifyObject(v, flatbuffers::GetFieldT(*table, *fieldDef), obj);
        break;
      }
      case reflection::BaseType::Union:
      default:
        break;
    }

    return isValid;
  }
}

bool FlatbuffersDocument::Verify() {
  return VerifyObject(Verifier(reinterpret_cast<const uint8_t*>(m_buffer->GetData()),
                               m_buffer->GetLength()), m_table, m_obj);
}

FlatbuffersDocument::FlatbuffersDocument() {
}

std::string FlatbuffersDocument::GetMissingFieldErrorString(
    const std::string& fieldName) const {
  ostringstream ss;
  ss << "Field definition for " << fieldName
      << " not found in the parsed schema.";
  return ss.str();
}
