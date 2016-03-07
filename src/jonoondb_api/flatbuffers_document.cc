#include <sstream>
#include <cstdint>
#include <memory>
#include "flatbuffers_document.h"
#include "flatbuffers_document_schema.h"
#include "buffer_impl.h"

using namespace std;
using namespace jonoondb_api;
using namespace flatbuffers;

FlatbuffersDocument::FlatbuffersDocument(
  const std::shared_ptr<FlatbuffersDocumentSchema>& fbDocumentSchema, const BufferImpl& buffer) :
  m_fbDcumentSchema(fbDocumentSchema) {
  Table* table = const_cast<Table*>(flatbuffers::GetRoot<Table>(buffer.GetData()));
  m_dynTableReader.reset(new DynamicTableReader(table,
                                                m_fbDcumentSchema->GetRootStruct(),
                                                m_fbDcumentSchema->GetChildStructs()));
}

int8_t FlatbuffersDocument::GetScalarValueAsInt8(const std::string& fieldName) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName),
      __FILE__, __func__, __LINE__);
  }

  return m_dynTableReader->GetScalarValueAs < int8_t > (fieldDef);  
}

int16_t FlatbuffersDocument::GetScalarValueAsInt16(const std::string& fieldName) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName), __FILE__, __func__, __LINE__);
  }

  return m_dynTableReader->GetScalarValueAs < int16_t > (fieldDef);
}

int32_t FlatbuffersDocument::GetScalarValueAsInt32(const std::string& fieldName) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName), __FILE__, __func__, __LINE__);
  }

  return m_dynTableReader->GetScalarValueAs < int32_t > (fieldDef);
}

int64_t FlatbuffersDocument::GetScalarValueAsInt64(const std::string& fieldName) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName), __FILE__, __func__, __LINE__);
  }

  return m_dynTableReader->GetScalarValueAs < int64_t > (fieldDef);
}

uint8_t FlatbuffersDocument::GetScalarValueAsUInt8(const std::string& fieldName) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName), __FILE__, __func__, __LINE__);
  }

  return m_dynTableReader->GetScalarValueAs < uint8_t > (fieldDef);  
}

uint16_t FlatbuffersDocument::GetScalarValueAsUInt16(const std::string& fieldName) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName), __FILE__, __func__, __LINE__);
  }

  return m_dynTableReader->GetScalarValueAs < uint16_t > (fieldDef);  
}

uint32_t FlatbuffersDocument::GetScalarValueAsUInt32(const std::string& fieldName) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName), __FILE__, __func__, __LINE__);
  }

  return m_dynTableReader->GetScalarValueAs < uint32_t > (fieldDef);  
}

uint64_t FlatbuffersDocument::GetScalarValueAsUInt64(const std::string& fieldName) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName), __FILE__, __func__, __LINE__);
  }

  return m_dynTableReader->GetScalarValueAs < uint64_t > (fieldDef);  
}

float FlatbuffersDocument::GetScalarValueAsFloat(const std::string& fieldName) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName), __FILE__, __func__, __LINE__);
  }

  return m_dynTableReader->GetScalarValueAs<float>(fieldDef);  
}

double FlatbuffersDocument::GetScalarValueAsDouble(const std::string& fieldName) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName), __FILE__, __func__, __LINE__);
  }

  return m_dynTableReader->GetScalarValueAs<double>(fieldDef);  
}

std::string FlatbuffersDocument::GetStringValue(const std::string& fieldName) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName), __FILE__, __func__, __LINE__);
  }

  return m_dynTableReader->GetStringValue(fieldDef);  
}

const char* FlatbuffersDocument::GetStringValue(const std::string& fieldName, std::size_t& size) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName), __FILE__, __func__, __LINE__);
  }

  return m_dynTableReader->GetStringValue(fieldDef, size);
}

std::int64_t FlatbuffersDocument::GetIntegerValueAsInt64(const std::string& fieldName) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName), __FILE__, __func__, __LINE__);
  }

  int64_t val;
  switch (fieldDef->value.type.base_type) {
    case BaseType::BASE_TYPE_BOOL:
    case BaseType::BASE_TYPE_UCHAR: {
      // Flatbuffer represents boolean as uint8_t (0=false, 1=true) 
      // so for fb we can convert bool to an integer value
      val = m_dynTableReader->GetScalarValueAs<std::uint8_t>(fieldDef);
      break;
    }
    case BaseType::BASE_TYPE_USHORT: {
      val = m_dynTableReader->GetScalarValueAs<std::uint16_t>(fieldDef);
      break;
    }
    case BaseType::BASE_TYPE_UINT: {
      val = m_dynTableReader->GetScalarValueAs<std::uint32_t>(fieldDef);
      break;
    }
    case BaseType::BASE_TYPE_ULONG: {
      val = m_dynTableReader->GetScalarValueAs<std::uint64_t>(fieldDef);
      break;
    }
    case BaseType::BASE_TYPE_CHAR: {
      val = m_dynTableReader->GetScalarValueAs<std::int8_t>(fieldDef);
      break;
    }
    case BaseType::BASE_TYPE_SHORT: {
      val = m_dynTableReader->GetScalarValueAs<std::int16_t>(fieldDef);
      break;
    }
    case BaseType::BASE_TYPE_INT: {
      val = m_dynTableReader->GetScalarValueAs<std::int32_t>(fieldDef);
      break;
    }
    case BaseType::BASE_TYPE_LONG: {
      val = m_dynTableReader->GetScalarValueAs<std::int64_t>(fieldDef);
      break;
    }    
    default: {
      std::ostringstream ss;
      ss << "Field " << fieldName << " has FieldType " << fieldDef->value.type.base_type
        << " and it cannot be safely converted into a 64 bit integer.";
      throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
    }
  }

  return val;
}

double FlatbuffersDocument::GetFloatingValueAsDouble(const std::string& fieldName) const {
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName), __FILE__, __func__, __LINE__);
  }

  double val;
  switch (fieldDef->value.type.base_type) {
    case BaseType::BASE_TYPE_FLOAT: {
      val = m_dynTableReader->GetScalarValueAs<float>(fieldDef);
      break;
    }
    case BaseType::BASE_TYPE_DOUBLE: {
      val = m_dynTableReader->GetScalarValueAs<double>(fieldDef);
      break;
    }    
    default: {
      std::ostringstream ss;
      ss << "Field " << fieldName << " has FieldType " << fieldDef->value.type.base_type
        << " and it cannot be safely converted into a 64 bit floating point number.";
      throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
    }
  }

  return val;
}

void FlatbuffersDocument::GetDocumentValue(const std::string& fieldName,
                                           Document& val) const {
  try {
    // Todo: dynamic_cast can be expensive, this should be optimized.
    FlatbuffersDocument& fbDoc = dynamic_cast<FlatbuffersDocument&>(val);
    auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
    if (fieldDef == nullptr) {
      throw JonoonDBException(GetMissingFieldErrorString(fieldName), __FILE__, __func__, __LINE__);
    }

    m_dynTableReader->GetTableValue(fieldDef, *(fbDoc.m_dynTableReader.get()));
  } catch (std::bad_cast) {
    // This means that the passed in doc cannot be casted to FlatbuffersDocument    
    string errorMsg = "Argument val cannot be casted to underlying document "
      "implementation i.e. FlatbuffersDocument. "
      "Make sure you are creating the val by calling AllocateDocument call.";
    throw InvalidArgumentException(errorMsg, __FILE__, __func__, __LINE__);
  }
}

std::unique_ptr<Document> FlatbuffersDocument::AllocateSubDocument() const {
  return std::unique_ptr<Document>(new FlatbuffersDocument(
    m_fbDcumentSchema,
    move(unique_ptr < DynamicTableReader >(new DynamicTableReader()))));
}

void FlatbuffersDocument::VerifyFieldForRead(const std::string& fieldName, FieldType expectedType) const {
  // Make sure field exists
  auto fieldDef = m_dynTableReader->GetFieldDef(fieldName);
  if (fieldDef == nullptr) {
    throw JonoonDBException(GetMissingFieldErrorString(fieldName), __FILE__, __func__, __LINE__);
  }

  // Make sure it has the same type
  auto actualType = FlatbuffersDocumentSchema::MapFlatbuffersToJonoonDBType(fieldDef->value.type.base_type);
  if (actualType != expectedType) {
    ostringstream ss;
    ss << "Actual field type for field " << fieldName << " is " << GetFieldString(actualType) <<
      " which is different from the expected field type " << GetFieldString(expectedType) << ".";
    throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }
}

FlatbuffersDocument::FlatbuffersDocument(
  const shared_ptr<FlatbuffersDocumentSchema> fbDocumentSchema,
  unique_ptr<DynamicTableReader> dynTableReader)
  : m_fbDcumentSchema(fbDocumentSchema),
  m_dynTableReader(move(dynTableReader)) {
}

std::string FlatbuffersDocument::GetMissingFieldErrorString(
    const std::string& fieldName) const {
  ostringstream ss;
  ss << "Field definition for " << fieldName
     << " not found in the parsed schema."; 
  return ss.str();  
}
