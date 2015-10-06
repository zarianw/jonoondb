#include <cstring>
#include <string>
#include <sstream>
#include <stdlib.h>
#include <stdio.h>
#include <memory>
#include <vector>
#include <assert.h>
#include "sqlite3ext.h"
#include "document_collection_dictionary.h"
#include "document_collection.h"
#include "field.h"
#include "guard_funcs.h"
#include "status.h"
#include "enums.h"
#include "document_schema.h"
#include "index_stat.h"


using namespace jonoondb_api;

SQLITE_EXTENSION_INIT1;

typedef struct jonoondb_vtab_s {
  sqlite3_vtab vtab;
  std::shared_ptr<DocumentCollection> collection;
  std::vector<std::string> columnNames;
} jonoondb_vtab;

typedef struct jonoondb_cursor_s {
  sqlite3_vtab_cursor cur;
  sqlite_int64 row;
} jonoondb_cursor;

const char* GetSQLiteTypeString(FieldType fieldType) {
  static std::string integer = "INTEGER";
  static std::string real = "REAL";
  static std::string text = "TEXT";
  static std::string unknown = "UNKNOWN";  
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
      assert(false && "These types should never be encountered here.");
      break;
  }

  return unknown.c_str();
}

Status BuildCreateTableStatement(const Field* complexField,
                                 std::string& prefix,
                                 std::ostringstream& stringStream,
                                 std::vector<std::string>& columnNames) {
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
      sts = BuildCreateTableStatement(field, prefix, stringStream, columnNames);
      if (!sts) return sts;
    } else {
      auto fullName = prefix;
      fullName.append(field->GetName());
      columnNames.push_back(fullName);
      stringStream << "'" << fullName << "'" << " "
        << GetSQLiteTypeString(field->GetType());
      stringStream << ", ";
    }
  }

  return sts;
}

Status GenerateCreateTableStatementForCollection(const std::shared_ptr<DocumentCollection>& collection,
                                                 std::ostringstream& stringStream,
                                                 std::vector<std::string>& columnNames) {
  Field* field;
  auto sts = collection->GetDocumentSchema()->AllocateField(field);
  if (!sts) return sts;
  std::unique_ptr<Field, void(*)(Field*)> fieldGuard(field, GuardFuncs::DisposeField);

  stringStream.clear();
  stringStream << "CREATE TABLE " << collection->GetName() << " (";
  auto count = collection->GetDocumentSchema()->GetRootFieldCount();
  for (size_t i = 0; i < count; i++) {
    auto sts = collection->GetDocumentSchema()->GetRootField(i, field);
    if (!sts) return sts;

    if (field->GetType() == FieldType::BASE_TYPE_COMPLEX) {
      std::string prefix;
      prefix.append(field->GetName());
      prefix.append(".");
      sts = BuildCreateTableStatement(field, prefix, stringStream, columnNames);
      if (!sts) return sts;
    } else {
      columnNames.push_back(field->GetName());
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


static int jonoondb_create(sqlite3 *db, void *udp, int argc,
                           const char * const *argv, sqlite3_vtab **vtab,
                           char **errmsg) {  
  // Validate arguments
  if (argc != 4) {
    std::ostringstream errMessage;
    if (errmsg != nullptr) {
      errMessage << "jonnondb_vtable needs 4 arguments, but only " << argc << " arguments were provided.";
      auto str = errMessage.str();
      *errmsg = (char*)sqlite3_malloc(str.size() + 1);
      std::strncpy(*errmsg, str.c_str(), str.size());
    }
    return SQLITE_MISUSE;
  } 

  std::string key = argv[argc - 1];
  std::string collectionName = argv[2];
  std::shared_ptr<DocumentCollection> col;    
  if (!DocumentCollectionDictionary::Instance()->TryGet(key, col)) {
    if (errmsg != nullptr) {
      std::ostringstream errMessage;
      errMessage << "jonnondb_vtable could not find collection " << collectionName <<
        " in the dictionary using key " << key << ".";
      auto str = errMessage.str();
      *errmsg = (char*)sqlite3_malloc(str.size() + 1);
      std::strncpy(*errmsg, str.c_str(), str.size());
    }
    return SQLITE_MISUSE;
  }

  std::unique_ptr<jonoondb_vtab> v(new jonoondb_vtab());
  if (v == nullptr)
    return SQLITE_NOMEM;
  v->collection = col; // set the document collection shared pointer

  // Generate the create table stmt
  std::ostringstream ss;
  auto sts = GenerateCreateTableStatementForCollection(col, ss, v->columnNames);
  if (!sts) {
    if (errmsg != nullptr) {
      std::string str = sts.GetMessage();
      *errmsg = (char*)sqlite3_malloc(str.size() + 1);
      std::strncpy(*errmsg, str.c_str(), str.size());
    }
    return SQLITE_MISUSE;
  }  

  int code = sqlite3_declare_vtab(db, ss.str().c_str());
  if (code != SQLITE_OK) 
    return code;  

  *vtab = (sqlite3_vtab*)v.release();

  return SQLITE_OK;
}

static int jonoondb_connect(sqlite3 *db, void *udp, int argc,
                            const char * const *argv, sqlite3_vtab **vtab,
                            char **errmsg) {
  std::string str = "jonoondb_connect method not implemented.";
  *errmsg = (char*)sqlite3_malloc(str.size() + 1);
  std::strncpy(*errmsg, str.c_str(), str.size());
  return SQLITE_ERROR;
}

static int jonoondb_disconnect(sqlite3_vtab *vtab) {
  delete vtab;
  //sqlite3_free(vtab);
  return SQLITE_OK;
}

static int jonoondb_destroy(sqlite3_vtab *vtab) {
  delete vtab;
  //sqlite3_free(vtab);
  return SQLITE_OK;
}

static char* op(unsigned char op) {
  if (op == SQLITE_INDEX_CONSTRAINT_EQ)
    return "=";
  if (op == SQLITE_INDEX_CONSTRAINT_GT)
    return ">";
  if (op == SQLITE_INDEX_CONSTRAINT_LE)
    return "<=";
  if (op == SQLITE_INDEX_CONSTRAINT_LT)
    return "<";
  if (op == SQLITE_INDEX_CONSTRAINT_GE)
    return ">=";
  if (op == SQLITE_INDEX_CONSTRAINT_MATCH)
    return "MATCH";
  return "?";
}

static IndexConstraintOperator MapSQLiteToJonoonDBOperator(unsigned char op) {
  switch (op) {
    case SQLITE_INDEX_CONSTRAINT_EQ:
      return IndexConstraintOperator::EQUAL;
    case SQLITE_INDEX_CONSTRAINT_GT:
      return IndexConstraintOperator::GREATER_THAN;
    case SQLITE_INDEX_CONSTRAINT_LE:
      return IndexConstraintOperator::LESS_THAN_EQUAL;
    case SQLITE_INDEX_CONSTRAINT_LT:
      return IndexConstraintOperator::LESS_THAN;
    case SQLITE_INDEX_CONSTRAINT_GE:
      return IndexConstraintOperator::GREATER_THAN_EQUAL;
    case SQLITE_INDEX_CONSTRAINT_MATCH:
      return IndexConstraintOperator::MATCH;
    default:
      break;
  }

  assert(false && "Invalid SQL operator encountered.");
}

static int jonoondb_bestindex(sqlite3_vtab *vtab, sqlite3_index_info *info) {
  int i;
  // printf("BEST INDEX:\n");
  jonoondb_vtab* jdbVtab = reinterpret_cast<jonoondb_vtab*>(vtab);
  IndexStat indexStat;
  for (i = 0; i < info->nConstraint; i++) {
    if (info->aConstraint[i].usable) {
      if (jdbVtab->collection->TryGetBestIndex(jdbVtab->columnNames[info->aConstraint[i].iColumn],
                                               MapSQLiteToJonoonDBOperator(info->aConstraint[i].op),
                                               indexStat)) {
        info->aConstraintUsage[i].argvIndex = 1;
        info->aConstraintUsage[i].omit = 1;                        
      }
    }

    info->orderByConsumed = 0;    
  }

  /*for (i = 0; i < info->nOrderBy; i++) {
    printf("   ORDER[%d]: %d %s\n", i, info->aOrderBy[i].iColumn,
           info->aOrderBy[i].desc ? "DESC" : "ASC");
  }*/     

  return SQLITE_OK;
}

static int jonoondb_open(sqlite3_vtab *vtab, sqlite3_vtab_cursor **cur) {
  jonoondb_vtab *v = (jonoondb_vtab*) vtab;
  jonoondb_cursor *c;

  // printf("OPEN\n");

  c = (jonoondb_cursor*) sqlite3_malloc(sizeof(jonoondb_cursor));
  if (c == NULL)
    return SQLITE_NOMEM;

  *cur = (sqlite3_vtab_cursor*) c;
  c->row = 0;
  return SQLITE_OK;
}

static int jonoondb_close(sqlite3_vtab_cursor *cur) {
  // printf("CLOSE\n");

  sqlite3_free(cur);
  return SQLITE_OK;
}

static int jonoondb_filter(sqlite3_vtab_cursor *cur, int idxnum,
                           const char *idxstr, int argc,
                           sqlite3_value **value) {
  // printf("FILTER\n");
  return SQLITE_OK;
}

static int jonoondb_next(sqlite3_vtab_cursor *cur) {
  // printf("NEXT\n");
  ((jonoondb_cursor*) cur)->row++;
  return SQLITE_OK;
}

static int jonoondb_eof(sqlite3_vtab_cursor *cur) {
  // printf("EOF\n");
  return (((jonoondb_cursor*) cur)->row >= 10);
}

static int jonoondb_rowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *rowid) {
  // printf("ROWID: %lld\n", ((jonoondb_cursor*) cur)->row);
  *rowid = ((jonoondb_cursor*) cur)->row;
  return SQLITE_OK;
}

static int jonoondb_column(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
                           int cidx) {
  jonoondb_cursor *c = (jonoondb_cursor*) cur;

  // printf("COLUMN: %d\n", cidx);
  sqlite3_result_int(ctx, c->row * 10 + cidx);
  return SQLITE_OK;
}

static int jonoondb_rename(sqlite3_vtab *vtab, const char *newname) {
  // printf("RENAME\n");
  return SQLITE_OK;
}

static int jonoondb_update(sqlite3_vtab *vtab, int argc, sqlite3_value **argv,
                           sqlite_int64 *rowid) {
  printf("UPDATE: ");

  if (argc == 1) {
    printf("DELETE %d\n", sqlite3_value_int(argv[0]));

    return SQLITE_OK;
  }

  if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
    if (sqlite3_value_type(argv[1]) == SQLITE_NULL) {
      printf("INSERT -\n");

      *rowid = 10;
    } else {
      printf("INSERT %d\n", sqlite3_value_int(argv[1]));
    }
    return SQLITE_OK;
  }

  printf("UPDATE %d %d\n", sqlite3_value_int(argv[0]),
         sqlite3_value_int(argv[1]));
  return SQLITE_OK;
}

static int jonoondb_begin(sqlite3_vtab *vtab) {
  // printf("BEGIN\n");
  return SQLITE_OK;
}

static int jonoondb_sync(sqlite3_vtab *vtab) {
  //printf("SYNC\n");
  return SQLITE_OK;
}

static int jonoondb_commit(sqlite3_vtab *vtab) {
  //printf("COMMIT\n");
  return SQLITE_OK;
}

static int jonoondb_rollback(sqlite3_vtab *vtab) {
  // printf("ROLLBACK\n");
  return SQLITE_OK;
}

static int jonoondb_xFindFunction(
    sqlite3_vtab *pVtab, /* Virtual table handle */
    int nArg, /* Number of SQL function arguments */
    const char *zName, /* Name of SQL function */
    void (**pxFunc)(sqlite3_context*, int, sqlite3_value**), /* OUT: Result */
    void **ppArg /* Unused */
    ) {
}

static sqlite3_module jonoondb_mod = { 1, /* iVersion        */
jonoondb_create, /* xCreate()       */
jonoondb_connect, /* xConnect()      */
jonoondb_bestindex, /* xBestIndex()    */
jonoondb_disconnect, /* xDisconnect()   */
jonoondb_destroy, /* xDestroy()      */
jonoondb_open, /* xOpen()         */
jonoondb_close, /* xClose()        */
jonoondb_filter, /* xFilter()       */
jonoondb_next, /* xNext()         */
jonoondb_eof, /* xEof()          */
jonoondb_column, /* xColumn()       */
jonoondb_rowid, /* xRowid()        */
jonoondb_update, /* xUpdate()       */
jonoondb_begin, /* xBegin()        */
jonoondb_sync, /* xSync()         */
jonoondb_commit, /* xCommit()       */
jonoondb_rollback, /* xRollback()     */
NULL,/* xFindFunction() */
jonoondb_rename /* xRename()       */
};

int jonoondb_vtable_init(sqlite3 *db, char **error,
                         const sqlite3_api_routines *api) {
  SQLITE_EXTENSION_INIT2(api);
  return sqlite3_create_module(db, "jonoondb_vtable", &jonoondb_mod, NULL);
}
