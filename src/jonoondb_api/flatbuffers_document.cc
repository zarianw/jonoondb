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
  m_table(flatbuffers::GetAnyRoot((uint8_t*)(buffer->GetData()))) {
}

std::string FlatbuffersDocument::GetStringValue(const std::string& fieldName) const {
  auto fieldDef = m_obj->fields()->LookupByKey(fieldName.c_str());
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName),
                            __FILE__, __func__, __LINE__);
  }
  if (fieldDef->type()->base_type() != reflection::BaseType::String) {
    std::ostringstream ss;
    ss << "Field " << fieldName << " has FieldType " << fieldDef->type()->base_type()
      << " and it cannot be safely converted into string.";
    throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }

  if (m_table->CheckField(fieldDef->offset())) {
    return flatbuffers::GetAnyFieldS(*m_table, *fieldDef, nullptr);
  } else {
    return JONOONDB_NULL_STR;
  }  
}

const char* FlatbuffersDocument::GetStringValue(const std::string& fieldName, std::size_t& size) const {
  auto fieldDef = m_obj->fields()->LookupByKey(fieldName.c_str());
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName),
                            __FILE__, __func__, __LINE__);
  }
  if (fieldDef->type()->base_type() != reflection::BaseType::String) {
    std::ostringstream ss;
    ss << "Field " << fieldName << " has FieldType " << fieldDef->type()->base_type()
      << " and it cannot be safely converted into string.";
    throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }
  
  return flatbuffers::GetFieldS(*m_table, *fieldDef)->c_str();      
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
    ss << "Field " << fieldName << " has FieldType " << fieldDef->type()->base_type()
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
    ss << "Field " << fieldName << " has FieldType " << fieldDef->type()->base_type()
      << " and it cannot be safely converted into a 64 bit floating value.";
    throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }
  
  return flatbuffers::GetAnyFieldF(*m_table, *fieldDef);  
}

void FlatbuffersDocument::GetDocumentValue(const std::string& fieldName,
                                           Document& val) const {
  try {
    // Todo: dynamic_cast can be expensive, this should be optimized.
    FlatbuffersDocument& fbDoc = dynamic_cast<FlatbuffersDocument&>(val);
    auto fieldDef = m_obj->fields()->LookupByKey(fieldName.c_str());
    if (fieldDef == nullptr) {
      throw JonoonDBException(GetMissingFieldErrorString(fieldName),
                              __FILE__, __func__, __LINE__);
    }

    if (fieldDef->type()->base_type() != reflection::BaseType::Obj) {
      std::ostringstream ss;
      ss << "Field " << fieldName << " has FieldType " << fieldDef->type()->base_type()
        << " and it cannot be safely converted into a document value.";
      throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
    }

    auto obj = reflection::GetSchema(
      m_fbDcumentSchema->GetSchemaText().c_str())->
      objects()->Get(fieldDef->type()->index());
    assert(obj != nullptr);

    auto table = flatbuffers::GetFieldT(*m_table, *fieldDef);
    assert(table != nullptr);
    
    fbDoc.SetMembers(m_fbDcumentSchema, m_buffer,
                     const_cast<reflection::Object*>(obj), table);
  } catch (std::bad_cast) {
    // This means that the passed in doc cannot be casted to FlatbuffersDocument    
    string errorMsg = "Argument val cannot be casted to underlying document "
      "implementation i.e. FlatbuffersDocument. "
      "Make sure you are creating the val by calling AllocateDocument call.";
    throw InvalidArgumentException(errorMsg, __FILE__, __func__, __LINE__);
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
  auto actualType = FlatbuffersDocumentSchema::MapFlatbuffersToJonoonDBType(
    fieldDef->type()->base_type());
  if (actualType != expectedType) {
    ostringstream ss;
    ss << "Actual field type for field " << fieldName << " is " << GetFieldString(actualType) <<
      " which is different from the expected field type " << GetFieldString(expectedType) << ".";
    throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }
}

void FlatbuffersDocument::SetMembers(FlatbuffersDocumentSchema* schema,
                                     BufferImpl* buffer, reflection::Object* obj,
                                     flatbuffers::Table* table) {
  m_fbDcumentSchema = schema;
  m_buffer = buffer;
  m_obj = obj;
  m_table = table;
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
