#include <string>
#include <sstream>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "sqlite3.h"
#include "query_processor.h"
#include "status.h"
#include "exception_utils.h"
#include "document_collection.h"
#include "document_collection_dictionary.h"
#include "document_schema.h"
#include "field.h"
#include "enums.h"
#include "guard_funcs.h"

using namespace std;
using namespace jonoondb_api;

struct sqlite3_api_routines;
int jonoondb_vtable_init(sqlite3 *db, char **error,
                         const sqlite3_api_routines *api);

Status QueryProcessor::Construct(QueryProcessor*& obj) {
  int code = sqlite3_auto_extension((void (*)(void))jonoondb_vtable_init);
  if (code != SQLITE_OK) {
    return ExceptionUtils::GetSQLiteErrorStatusFromSQLiteErrorCode(code);
  }

  sqlite3* db = nullptr;
  code = sqlite3_open(":memory:", &db);
  if (code != SQLITE_OK) {
    return ExceptionUtils::GetSQLiteErrorStatusFromSQLiteErrorCode(code);
  }

  obj = new QueryProcessor(std::unique_ptr<sqlite3, void(*)(sqlite3*)>(db, GuardFuncs::SQLite3Close));

  return Status();
}

Status QueryProcessor::AddCollection(const shared_ptr<DocumentCollection>& collection) {
  // Generate create table statement for sqlite vtable
  ostringstream ss;
  auto sts = GenerateCreateTableStatement(collection, ss);
  if (!sts) {
    return sts;
  }

  // Generate key and insert the collection in a singleton dictionary.
  // vtable will use this key to get the collection.
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  string key = "'";
  key.append(boost::uuids::to_string(uuid)).append("'");
  DocumentCollectionDictionary::Instance()->Insert(key, collection);

  ostringstream sqlStmt;
  sqlStmt << "CREATE VIRTUAL TABLE " << collection->GetName() 
    << " USING jonoondb_vtable(" << key << "$" << ss.str() << ")";

  char* errMsg;
  int code = sqlite3_exec(m_db.get(), sqlStmt.str().c_str(), nullptr, nullptr, &errMsg);
  // Remove the collection from dictionary
  DocumentCollectionDictionary::Instance()->Remove(key);
  if (code != SQLITE_OK) {
    if (errMsg != nullptr) {
      string sqliteErrorMsg = errMsg;
      sqlite3_free(errMsg);
      return Status(kStatusSQLiteErrorCode, sqliteErrorMsg.c_str(),
        sqliteErrorMsg.length());      
    }

    return ExceptionUtils::GetSQLiteErrorStatusFromSQLiteErrorCode(code);
  }

  return sts;
}

QueryProcessor::QueryProcessor(std::unique_ptr<sqlite3, void(*)(sqlite3*)> db)
    : m_db(move(db)) {
}

const char* GetSQLiteTypeString(FieldType fieldType) {
  static string integer = "INTEGER";
  static string real = "REAL";
  static string text = "TEXT";
  static string unknown = "UNKNOWN";
  switch (fieldType) {
    case jonoondb_api::FieldType::BASE_TYPE_UINT8:
    case jonoondb_api::FieldType::BASE_TYPE_UINT16:
    case jonoondb_api::FieldType::BASE_TYPE_UINT32:
    case jonoondb_api::FieldType::BASE_TYPE_UINT64:
    case jonoondb_api::FieldType::BASE_TYPE_INT8:
    case jonoondb_api::FieldType::BASE_TYPE_INT16:
    case jonoondb_api::FieldType::BASE_TYPE_INT32:
    case jonoondb_api::FieldType::BASE_TYPE_INT64:
      return integer.c_str();
    case jonoondb_api::FieldType::BASE_TYPE_FLOAT32:
    case jonoondb_api::FieldType::BASE_TYPE_DOUBLE:
      return real.c_str();
    case jonoondb_api::FieldType::BASE_TYPE_STRING:
      return text.c_str();
    case jonoondb_api::FieldType::BASE_TYPE_VECTOR:
    case jonoondb_api::FieldType::BASE_TYPE_COMPLEX:
    default:
      assert("These types should never be encountered here.");
      break;
  }

  return unknown.c_str();
}

Status BuildCreateTableStatement(const Field* complexField, string& prefix, ostringstream& stringStream) {
  assert(complexField->GetType() == FieldType::BASE_TYPE_COMPLEX);
  Field* field;
  auto sts = complexField->AllocateField(field);
  if (!sts) return sts;
  std::unique_ptr<Field, void(*)(Field*)> fieldGuard(field, GuardFuncs::DisposeField);

  for (size_t i = 0; i < complexField->GetSubFieldCount(); i++) {
    sts = complexField->GetSubField(i, field);
    if (!sts) return sts;

    if (field->GetType() == FieldType::BASE_TYPE_COMPLEX) {
      prefix.append(field->GetName());
      prefix.append(".");
      BuildCreateTableStatement(field, prefix, stringStream);
    } else {
      stringStream << prefix << field->GetName() << "'" << " " << GetSQLiteTypeString(field->GetType());
      stringStream << ", ";      
    }
  }

  return sts;
}

Status QueryProcessor::GenerateCreateTableStatement(const shared_ptr<DocumentCollection>& collection,
                                                    ostringstream& stringStream) {
  Field* field;
  auto sts = collection->GetDocumentSchema()->AllocateField(field);
  if (!sts) return sts;
  unique_ptr<Field, void(*)(Field*)> fieldGuard(field, GuardFuncs::DisposeField);
  
  stringStream.clear();
  stringStream << "CREATE TABLE " << collection->GetName() << " (";
  auto count = collection->GetDocumentSchema()->GetRootFieldCount();
  for (size_t i = 0; i < count; i++) {
    auto sts = collection->GetDocumentSchema()->GetRootField(i, field);
    if (!sts) return sts;

    if (field->GetType() == FieldType::BASE_TYPE_COMPLEX) {
      string prefix = "'";
      prefix.append(field->GetName());
      prefix.append(".");
      sts = BuildCreateTableStatement(field, prefix, stringStream);
      if (!sts) return sts;
    } else {
      stringStream << field->GetName() << " " << GetSQLiteTypeString(field->GetType());
      stringStream << ", ";
    }
  }

  if (count > 0) {
    long pos = stringStream.tellp();
    stringStream.seekp(pos - 2);
  }

  stringStream << ");";  
  return sts;
}



