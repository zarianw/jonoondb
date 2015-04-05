#include <memory>
#include <string>
#include <boost/tokenizer.hpp>
#include "flatbuffers/idl.h"
#include "flatbuffers_document_schema.h"
#include "enums.h"
#include "status.h"
#include "string_utils.h"


using namespace std;
using namespace jonoondb_api;
using namespace flatbuffers;
using namespace boost;

FlatbuffersDocumentSchema::FlatbuffersDocumentSchema(const char* schemaText,
  SchemaType schemaType, unique_ptr<Parser> parser) : 
  m_schemaText(schemaText), m_schemaType(schemaType), m_parser(move(parser)) {
}

FlatbuffersDocumentSchema::~FlatbuffersDocumentSchema() {
}

Status FlatbuffersDocumentSchema::Construct(const char* schemaText,
  SchemaType schemaType, FlatbuffersDocumentSchema*& documentSchema) {
  if (StringUtils::IsNullOrEmpty(schemaText)) {
    string errorMsg = "Argument schemaText is null or empty.";
    return Status(kStatusInvalidArgumentCode, errorMsg.c_str(),
      errorMsg.length());
  }  
  
  unique_ptr<Parser> parser(new Parser());
  if (!parser->Parse(schemaText)) {
    ostringstream ss;
    ss << "Flatbuffers parser failed to parse the given schema." << endl << schemaText;
    string errorMsg = ss.str();
    return Status(kStatusSchemaParseErrorCode, errorMsg.c_str(),
      errorMsg.length());
  }

  documentSchema = new FlatbuffersDocumentSchema(schemaText, schemaType, move(parser));
  return Status();
}

const char* FlatbuffersDocumentSchema::GetSchemaText() const {
  return m_schemaText.c_str();
}

SchemaType FlatbuffersDocumentSchema::GetSchemaType() const {
  return m_schemaType;
}

Status FlatbuffersDocumentSchema::GetFieldType(const char* fieldName,
                                                FieldType& fieldType) const {
  // The fieldName is dot(.) sperated e.g. Field1.Field2.Field3
  if (StringUtils::IsNullOrEmpty(fieldName)) {
    string errorMsg = "Argument fieldName is null or empty.";
    return Status(kStatusInvalidArgumentCode, errorMsg.c_str(),
      errorMsg.length());
  }

  char_separator<char> sep(".");
  // TODO: Find a way to avoid the cost of string construction
  tokenizer<char_separator<char>> tokens(string(fieldName), sep);
  auto structDef = m_parser->root_struct_def;
  vector<string> tokenVec(tokens.begin(), tokens.end());
  
  for (size_t i = 0; i < tokenVec.size() - 1; i++) {
    structDef = m_parser->structs_.Lookup(tokenVec[i]);
  }
  
  auto fieldDef = structDef->fields.Lookup(tokenVec[tokenVec.size() - 1]);
  if (fieldDef == nullptr) {
    return GetMissingFieldErrorStatus(fieldName);
  }
  fieldType = MapFlatbuffersToJonoonDBType(fieldDef->value.type.base_type);

  return Status();
}

const StructDef* FlatbuffersDocumentSchema::GetRootStruct() const {
  return m_parser->root_struct_def;
}

const SymbolTable<StructDef>* FlatbuffersDocumentSchema::GetChildStructs() const {
  return &m_parser->structs_;
}

FieldType FlatbuffersDocumentSchema::MapFlatbuffersToJonoonDBType(BaseType flatbuffersType) const {
  switch (flatbuffersType) {
    case BASE_TYPE_NONE: return FieldType::BASE_TYPE_UINT8;
    case BASE_TYPE_UTYPE: return FieldType::BASE_TYPE_UINT8;
    case BASE_TYPE_BOOL: return FieldType::BASE_TYPE_UINT8;
    case BASE_TYPE_CHAR: return FieldType::BASE_TYPE_INT8;
    case BASE_TYPE_UCHAR: return FieldType::BASE_TYPE_UINT8;
    case BASE_TYPE_SHORT: return FieldType::BASE_TYPE_INT16;
    case BASE_TYPE_USHORT: return FieldType::BASE_TYPE_UINT16;
    case BASE_TYPE_INT: return FieldType::BASE_TYPE_INT32;
    case BASE_TYPE_UINT: return FieldType::BASE_TYPE_UINT32;
    case BASE_TYPE_LONG: return FieldType::BASE_TYPE_INT64;
    case BASE_TYPE_ULONG: return FieldType::BASE_TYPE_UINT64;
    case BASE_TYPE_FLOAT: return FieldType::BASE_TYPE_FLOAT32;
    case BASE_TYPE_DOUBLE: return FieldType::BASE_TYPE_DOUBLE;
    case BASE_TYPE_STRING: return FieldType::BASE_TYPE_STRING;
    case BASE_TYPE_VECTOR: return FieldType::BASE_TYPE_VECTOR;
    case BASE_TYPE_STRUCT: return FieldType::BASE_TYPE_COMPLEX;
    // case BASE_TYPE_UNION: break; TODO: Need to handle this type
    default: assert(0); // this should never happen
  }
}

Status FlatbuffersDocumentSchema::GetMissingFieldErrorStatus(const char* fieldName) const {
  ostringstream ss;
  ss << "Field definition for " << fieldName << " not found in the parsed schema.";
  string errorMsg = ss.str();
  return Status(kStatusGenericErrorCode, errorMsg.c_str(),
    errorMsg.length());
}

