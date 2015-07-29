#include "flatbuffers_field.h"
#include "status.h"
#include "flatbuffers_document_schema.h"

using namespace flatbuffers;
using namespace jonoondb_api;
using namespace std;

const char* FlatbuffersField::GetName() const {  
  return m_fieldDef->name.c_str();
}

FieldType FlatbuffersField::GetType() const {
  return FlatbuffersDocumentSchema::MapFlatbuffersToJonoonDBType(m_fieldDef->value.type.base_type);
}

size_t FlatbuffersField::GetSubFieldCount() const {
  if (m_fieldDef->value.type.base_type == BASE_TYPE_STRUCT) {
    return m_fieldDef->value.type.struct_def->fields.vec.size();
  } else {
    return 0;
  }    
}

Status FlatbuffersField::GetSubField(size_t index, Field*& field) const {
  FlatbuffersField* fbField = dynamic_cast<FlatbuffersField*>(field);
  if (fbField == nullptr) {
    // This means that the passed in doc cannot be casted to FlatbuffersDocument    
    string errorMsg = "Argument field cannot be casted to underlying field "
      "implementation i.e. FlatbuffersField. "
      "Make sure you are creating the val by calling AllocateField call.";
    return Status(kStatusInvalidArgumentCode, errorMsg.c_str(),
      errorMsg.length());
  }

  if (index > GetSubFieldCount()-1 || index < 0) {
    string errorMsg = "Index was outside the bounds of the array.";
    return Status(kStatusIndexOutOfBoundErrorCode, errorMsg.c_str(),
      errorMsg.length());
  }

  fbField->SetFieldDef(m_fieldDef->value.type.struct_def->fields.vec[index]);
  return Status();
}

Status FlatbuffersField::AllocateField(Field*& field) const {
  try {
    field = new FlatbuffersField();
  } catch (bad_alloc) {
    // Memory allocation failed
    string errorMsg = "Memory allocation failed.";
    return Status(kStatusOutOfMemoryErrorCode, errorMsg.c_str(),
      errorMsg.length());
  }

  return Status();
}

void FlatbuffersField::Dispose() {
  m_fieldDef = nullptr;
  delete this;
}

void FlatbuffersField::SetFieldDef(flatbuffers::FieldDef* val) {
  m_fieldDef = val;
}