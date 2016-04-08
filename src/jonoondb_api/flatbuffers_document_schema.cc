#include <memory>
#include <string>
#include <boost/tokenizer.hpp>
#include "flatbuffers/idl.h"
#include "flatbuffers/reflection.h"
#include "flatbuffers_document_schema.h"
#include "jonoondb_api/flatbuffers_document_schema.h"
#include "enums.h"
#include "string_utils.h"
#include "exception_utils.h"
#include "flatbuffers_field.h"
#include "field.h"

using namespace std;
using namespace jonoondb_api;
using namespace boost;

FlatbuffersDocumentSchema::FlatbuffersDocumentSchema(std::string binarySchema) :
  m_binarySchema(move(binarySchema))  
{
  flatbuffers::Verifier verifier(
    reinterpret_cast<const std::uint8_t*>(
      m_binarySchema.c_str()), m_binarySchema.size());

  if (!reflection::VerifySchemaBuffer(verifier)) {
    throw InvalidSchemaException(
      "Schema verification failed for flatbuffers binary schema.",
      __FILE__, __func__, __LINE__);
  }

  // TODO: Add more checks for schema
  // 1. Maybe dont support UINT64
  // 2. Check if union types have no fields with same name and different types

  m_schema = const_cast<reflection::Schema*>(reflection::GetSchema(m_binarySchema.c_str()));
}

FlatbuffersDocumentSchema::~FlatbuffersDocumentSchema() {
}

const std::string& FlatbuffersDocumentSchema::GetSchemaText() const {
  return m_binarySchema;
}

SchemaType FlatbuffersDocumentSchema::GetSchemaType() const {
  return SchemaType::FLAT_BUFFERS;
}

FieldType FlatbuffersDocumentSchema::GetFieldType(const std::string& fieldName) const {
  // The fieldName is dot(.) sperated e.g. Field1.Field2.Field3
  if (fieldName.size() == 0) {
    throw InvalidArgumentException("Argument fieldName is empty.",
                                   __FILE__, __func__, __LINE__);
  }

  char_separator<char> sep(".");
  tokenizer<char_separator<char>> tokens(fieldName, sep);
  auto refObj = m_schema->root_table();
  vector<string> tokenVec(tokens.begin(), tokens.end());
  
  for (size_t i = 0; i < tokenVec.size() - 1; i++) {
    auto fieldDef = refObj->fields()->LookupByKey(tokenVec[i].c_str());
    if (fieldDef == nullptr) {
      throw JonoonDBException(ExceptionUtils::GetMissingFieldErrorString(tokenVec[i]),
                              __FILE__, __func__, __LINE__);
    }

    if (fieldDef->type()->base_type() != reflection::Obj) {
      // TODO: Remove once unions are supported
      throw JonoonDBException(ExceptionUtils::GetInvalidStructFieldErrorString(
        tokenVec[i], fieldName), __FILE__, __func__, __LINE__);
    }

    refObj = m_schema->objects()->Get(fieldDef->type()->index());    
  }

  auto fieldDef = refObj->fields()->LookupByKey(tokenVec[tokenVec.size() - 1].c_str());  
  if (fieldDef == nullptr) {
    throw JonoonDBException(ExceptionUtils::GetMissingFieldErrorString(fieldName),
                            __FILE__, __func__, __LINE__);
  }

  return FlatbuffersDocumentSchema::MapFlatbuffersToJonoonDBType(fieldDef->type()->base_type());
}

std::size_t FlatbuffersDocumentSchema::GetRootFieldCount() const {
  return m_schema->root_table()->fields()->size();  
}

void FlatbuffersDocumentSchema::GetRootField(size_t index, Field*& field) const {
  FlatbuffersField* fbField = dynamic_cast<FlatbuffersField*>(field);
  if (fbField == nullptr) {
    // This means that the passed in doc cannot be casted to FlatbuffersDocument    
    string errorMsg = "Argument field cannot be casted to underlying field "
      "implementation i.e. FlatbuffersField. "
      "Make sure you are creating the val by calling AllocateField call.";
    throw InvalidArgumentException(errorMsg, __FILE__, __func__, __LINE__);
  }

  if (index > GetRootFieldCount() - 1 || index < 0) {
    throw IndexOutOfBoundException("Index was outside the bounds of the array.", __FILE__, __func__, __LINE__);
  }
  auto rootField = m_schema->root_table()->fields()->Get(index);
  fbField->SetMembers(
    const_cast<reflection::Field*>(
      m_schema->root_table()->fields()->Get(index)), m_schema);
}

Field* FlatbuffersDocumentSchema::AllocateField() const {
  return new FlatbuffersField();
}

FieldType FlatbuffersDocumentSchema::MapFlatbuffersToJonoonDBType(
  reflection::BaseType flatbuffersType) {
  switch (flatbuffersType) {
    case reflection::None:      
    case reflection::UType:      
    case reflection::Bool:     
    case reflection::Byte:
    case reflection::UByte:
    case reflection::Short:
    case reflection::UShort:
    case reflection::Int:
      return FieldType::BASE_TYPE_INT32;
    case reflection::UInt:
    case reflection::Long:
      return FieldType::BASE_TYPE_INT64;
      break;
    case reflection::ULong:
      throw JonoonDBException(
        "Cannot map flatbuffers ULong type. JonoonDB does not support this type.",
        __FILE__, __func__, __LINE__);      
    case reflection::Float:
      return FieldType::BASE_TYPE_FLOAT32;      
    case reflection::Double:
      return FieldType::BASE_TYPE_DOUBLE;      
    case reflection::String:
      return FieldType::BASE_TYPE_STRING;      
    case reflection::Vector:
      return FieldType::BASE_TYPE_VECTOR;
    case reflection::Obj:
      return FieldType::BASE_TYPE_COMPLEX;      
    case reflection::Union:
      return FieldType::BASE_TYPE_UNION;
    default:
    {
      std::ostringstream ss;
      ss << "Cannot map flatbuffers field type to JonoonDB field type. "
        "Unknown type " << flatbuffersType << " encountered.";
      throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
    }      
  }  
}