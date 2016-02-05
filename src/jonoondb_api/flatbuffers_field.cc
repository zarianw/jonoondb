#include "flatbuffers_field.h"
#include "flatbuffers_document_schema.h"

using namespace flatbuffers;
using namespace jonoondb_api;
using namespace std;

const std::string& FlatbuffersField::GetName() const {
  return m_fieldDef->name;
}

FieldType FlatbuffersField::GetType() const {
  return FlatbuffersDocumentSchema::MapFlatbuffersToJonoonDBType(
      m_fieldDef->value.type.base_type);
}

size_t FlatbuffersField::GetSubFieldCount() const {
  if (m_fieldDef->value.type.base_type == BASE_TYPE_STRUCT) {
    return m_fieldDef->value.type.struct_def->fields.vec.size();
  } else {
    return 0;
  }
}

void FlatbuffersField::GetSubField(size_t index, Field*& field) const {
  FlatbuffersField* fbField = dynamic_cast<FlatbuffersField*>(field);
  if (fbField == nullptr) {
    // This means that the passed in doc cannot be casted to FlatbuffersDocument    
    string errorMsg = "Argument field cannot be casted to underlying field "
        "implementation i.e. FlatbuffersField. "
        "Make sure you are creating the val by calling AllocateField call.";
    throw InvalidArgumentException(errorMsg, __FILE__, __func__, __LINE__);
  }

  if (index > GetSubFieldCount() - 1 || index < 0) {
    throw IndexOutOfBoundException("Index was outside the bounds of the array.",
      __FILE__, __func__, __LINE__);
  }

  fbField->SetFieldDef(m_fieldDef->value.type.struct_def->fields.vec[index]);
}

Field* FlatbuffersField::AllocateField() const {
  return new FlatbuffersField();  
}

void FlatbuffersField::Dispose() {
  m_fieldDef = nullptr;
  delete this;
}

void FlatbuffersField::SetFieldDef(flatbuffers::FieldDef* val) {
  m_fieldDef = val;
}
