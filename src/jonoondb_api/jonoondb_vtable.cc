#include <cstring>
#include <string>
#include <sstream>
#include <stdlib.h>
#include <stdio.h>
#include <memory>
#include <vector>
#include <assert.h>
#include <tuple>
#include "sqlite3ext.h"
#include "document_collection_dictionary.h"
#include "document_collection.h"
#include "field.h"
#include "guard_funcs.h"
#include "status.h"
#include "enums.h"
#include "document_schema.h"
#include "index_stat.h"
#include "constraint.h"
#include "mama_jennies_bitmap.h"


using namespace jonoondb_api;

SQLITE_EXTENSION_INIT1;



struct jonoondb_vtab {
  sqlite3_vtab vtab;
  // This collection object is shared with the DatabaseImpl object
  std::shared_ptr<DocumentCollectionInfo> collectionInfo;  
};

struct jonoondb_cursor {
  jonoondb_cursor(std::shared_ptr<DocumentCollectionInfo>& colInfo) : collectionInfo(colInfo) {
  }

  sqlite3_vtab_cursor cur;  
  // we can keep reference here because we will always close the
  // jonoondb_cursor before closing the jonoondb_vtab
  std::shared_ptr<DocumentCollectionInfo>& collectionInfo;  
  std::shared_ptr<MamaJenniesBitmap> filteredIds;
  std::unique_ptr<MamaJenniesBitmap::const_iterator> iter;
  std::unique_ptr<MamaJenniesBitmap::const_iterator> end;
};

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



int GetSQLiteType(FieldType fieldType) {
  switch (fieldType) {
    case jonoondb_api::FieldType::BASE_TYPE_UINT8:
    case jonoondb_api::FieldType::BASE_TYPE_UINT16:
    case jonoondb_api::FieldType::BASE_TYPE_UINT32:
    case jonoondb_api::FieldType::BASE_TYPE_UINT64:
    case jonoondb_api::FieldType::BASE_TYPE_INT8:
    case jonoondb_api::FieldType::BASE_TYPE_INT16:
    case jonoondb_api::FieldType::BASE_TYPE_INT32:
    case jonoondb_api::FieldType::BASE_TYPE_INT64:
      return SQLITE_INTEGER;
    case jonoondb_api::FieldType::BASE_TYPE_FLOAT32:
    case jonoondb_api::FieldType::BASE_TYPE_DOUBLE:
      return SQLITE_FLOAT;
    case jonoondb_api::FieldType::BASE_TYPE_STRING:
      return SQLITE_TEXT;
    case jonoondb_api::FieldType::BASE_TYPE_VECTOR:
    case jonoondb_api::FieldType::BASE_TYPE_COMPLEX:
    default:
    {
      std::ostringstream ss;
      ss << "Argument fieldType has a value " << static_cast<int32_t>(fieldType)
        << " which does not have a correponding sql type.";
      throw InvalidArgumentException(ss.str(), __FILE__, "", __LINE__);        
    }
  }
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
  std::shared_ptr<DocumentCollectionInfo> colInfo;    
  if (!DocumentCollectionDictionary::Instance()->TryGet(key, colInfo)) {
    if (errmsg != nullptr) {
      std::ostringstream errMessage;
      errMessage << "jonnondb_vtable could not find collection info for " << collectionName <<
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
  v->collectionInfo = colInfo; // set the document collection shared pointer 

  int code = sqlite3_declare_vtab(db, v->collectionInfo->createVTableStmt.c_str());
  if (code != SQLITE_OK) 
    return code;  

  *vtab = (sqlite3_vtab*)v.release();

  return SQLITE_OK;
}

static int jonoondb_connect(sqlite3 *db, void *udp, int argc,
                            const char * const *argv, sqlite3_vtab **vtab,
                            char **errmsg) {
  return jonoondb_create(db, udp, argc, argv, vtab, errmsg);
}

static int jonoondb_disconnect(sqlite3_vtab* vtab) {
  delete vtab;  
  return SQLITE_OK;
}

static int jonoondb_destroy(sqlite3_vtab* vtab) {
  delete vtab;  
  return SQLITE_OK;
}

static int jonoondb_bestindex(sqlite3_vtab* vtab, sqlite3_index_info *info) {  
  jonoondb_vtab* jdbVtab = reinterpret_cast<jonoondb_vtab*>(vtab);
  IndexStat indexStat;
  int argvIndex = 0;  
  std::string sbuf;
  for (int i = 0; i < info->nConstraint; i++) {
    if (info->aConstraint[i].usable) {
      if (info->aConstraint[i].iColumn == -1) {
        // Means someone has used RowID in contraint
        // "Queries on RowID are not allowed.";
        return SQLITE_ERROR;
      }

      IndexConstraintOperator op = MapSQLiteToJonoonDBOperator(info->aConstraint[i].op);
      if (jdbVtab->collectionInfo->collection->TryGetBestIndex(jdbVtab->collectionInfo->columnsInfo[info->aConstraint[i].iColumn].columnName,
                                               op,
                                               indexStat)) {
        info->aConstraintUsage[i].argvIndex = ++argvIndex;
        info->aConstraintUsage[i].omit = 1;
        assert(sizeof(int) == sizeof(info->aConstraint[i].iColumn));
        assert(sizeof(IndexConstraintOperator) == sizeof(op));
        // type of info->aConstraint[i].iColumn is int
        sbuf.append((char*)&info->aConstraint[i].iColumn, sizeof(int)); 
        sbuf.append((char*)&op, sizeof(IndexConstraintOperator));
      }
    }    
  }
  
  if (sbuf.size() > 0) {    
    info->idxStr = (char*)sqlite3_malloc(sbuf.size());
    if (info->idxStr == nullptr) {
      return SQLITE_NOMEM;
    }
    info->needToFreeIdxStr = 1;
    info->idxNum = sbuf.size();    
    
    std::memcpy(info->idxStr, sbuf.data(), sbuf.size());    
  }

  info->orderByConsumed = 0;    

  return SQLITE_OK;
}

static int jonoondb_open(sqlite3_vtab* vtab, sqlite3_vtab_cursor** cur) {
  jonoondb_vtab* v = (jonoondb_vtab*) vtab;
  try {
    jonoondb_cursor* c = new jonoondb_cursor(v->collectionInfo);
    *cur = reinterpret_cast<sqlite3_vtab_cursor*>(c);    
  } catch (std::bad_alloc&) {
    return SQLITE_NOMEM;
  } catch (std::exception&) {
    return SQLITE_ERROR;
  }
  
  return SQLITE_OK;
}

static int jonoondb_close(sqlite3_vtab_cursor* cur) {
  delete cur;
  return SQLITE_OK;
}

static int jonoondb_filter(sqlite3_vtab_cursor* cur, int idxnum,
                           const char* idxstr, int argc,
                           sqlite3_value** value) {
  try {
    auto cursor = reinterpret_cast<jonoondb_cursor*>(cur);
    // Get the constraints    
    if (argc > 0) {
      std::vector<Constraint> constraints;
      auto currIndex = idxstr;

      while (currIndex < (idxstr + idxnum)) {
        // idxstr is encoded as: sizeof(int) bytes for column index, sizeof(IndexConstraintOperator) for Op
        int colIndex;
        memcpy(&colIndex, currIndex, sizeof(int));
        currIndex += sizeof(int);
        
        IndexConstraintOperator op;
        memcpy(&op, currIndex, sizeof(IndexConstraintOperator));
        currIndex += sizeof(IndexConstraintOperator);

        Constraint constraint(cursor->collectionInfo->columnsInfo[colIndex].columnName, op);
        
        switch (sqlite3_value_type(*value)) {
          case SQLITE_INTEGER:
            constraint.operandType = OperandType::INTEGER;
            constraint.operand.int64Val = sqlite3_value_int64(*value);
            break;
          case SQLITE_FLOAT:
            constraint.operandType = OperandType::DOUBLE;
            constraint.operand.doubleVal = sqlite3_value_double(*value);
            break;
          case SQLITE_TEXT:
            constraint.operandType = OperandType::STRING;
            constraint.strVal = (char*)(sqlite3_value_text(*value));
            break;
          default:
            std::ostringstream ss;
            ss << "Argument value has sql type " << sqlite3_value_type(*value)
              << " which is not supported yet.";
            throw InvalidArgumentException(ss.str(), __FILE__, "", __LINE__);
        }

        constraints.push_back(std::move(constraint)); 
        value++;
      }

      cursor->filteredIds = cursor->collectionInfo->collection->Filter(constraints);
      cursor->iter = cursor->filteredIds->begin_pointer();
      cursor->end = cursor->filteredIds->end_pointer();
    } else {
      // We need to do a full scan
      cursor->filteredIds = cursor->collectionInfo->collection->Filter(std::vector<Constraint>());
      cursor->iter = cursor->filteredIds->begin_pointer();
      cursor->end = cursor->filteredIds->end_pointer();
    }
  } catch (std::exception&) {
    return SQLITE_ERROR;
  }

  return SQLITE_OK;
}

static int jonoondb_next(sqlite3_vtab_cursor* cur) {
  auto jdbCursor = (jonoondb_cursor*)cur;
  ++(*jdbCursor->iter);
  
  return SQLITE_OK;
}

static int jonoondb_eof(sqlite3_vtab_cursor* cur) {
  auto jdbCursor = (jonoondb_cursor*)cur;
  if (*(jdbCursor->iter) >= *(jdbCursor->end)) {
    return 1;
  }
  
  return 0;
}

static int jonoondb_rowid(sqlite3_vtab_cursor* cur, sqlite3_int64 *rowid) {
  jonoondb_cursor* jdbCursor = (jonoondb_cursor*)cur;  
  *rowid = jdbCursor->iter->operator*();
  return SQLITE_OK;
}

static int jonoondb_column(sqlite3_vtab_cursor* cur, sqlite3_context *ctx,
  int cidx) {
  try {
    jonoondb_cursor* jdbCursor = (jonoondb_cursor*)cur;
    if (jdbCursor->collectionInfo->columnsInfo[cidx].columnType == FieldType::BASE_TYPE_STRING) {
      auto val = jdbCursor->collectionInfo->collection->GetDocumentFieldAsString(jdbCursor->iter->operator*(), jdbCursor->collectionInfo->columnsInfo[cidx].columnName);
      sqlite3_result_text(ctx, val.c_str(), val.size() + 1, SQLITE_TRANSIENT); // SQLITE_TRANSIENT causes SQLite to copy the string on its side
    } else if (jdbCursor->collectionInfo->columnsInfo[cidx].columnType == FieldType::BASE_TYPE_INT64 ||
      jdbCursor->collectionInfo->columnsInfo[cidx].columnType == FieldType::BASE_TYPE_INT32 ||
      jdbCursor->collectionInfo->columnsInfo[cidx].columnType == FieldType::BASE_TYPE_INT16 ||
      jdbCursor->collectionInfo->columnsInfo[cidx].columnType == FieldType::BASE_TYPE_INT8 ||
      jdbCursor->collectionInfo->columnsInfo[cidx].columnType == FieldType::BASE_TYPE_UINT64 ||
      jdbCursor->collectionInfo->columnsInfo[cidx].columnType == FieldType::BASE_TYPE_UINT32 ||
      jdbCursor->collectionInfo->columnsInfo[cidx].columnType == FieldType::BASE_TYPE_UINT16 ||
      jdbCursor->collectionInfo->columnsInfo[cidx].columnType == FieldType::BASE_TYPE_UINT8) {
      // Get the integer value
      auto val = jdbCursor->collectionInfo->collection->GetDocumentFieldAsInteger(jdbCursor->iter->operator*(), jdbCursor->collectionInfo->columnsInfo[cidx].columnName);
      sqlite3_result_int64(ctx, val);
    } else {
      // Get the floating value
      auto val = jdbCursor->collectionInfo->collection->GetDocumentFieldAsDouble(jdbCursor->iter->operator*(), jdbCursor->collectionInfo->columnsInfo[cidx].columnName);
      sqlite3_result_double(ctx, val);
    }
  } catch (std::exception& ex) {
    //Todo: Log the error
    return SQLITE_ERROR;
  }

  return SQLITE_OK;
}

static int jonoondb_rename(sqlite3_vtab* vtab, const char *newname) {
  // printf("RENAME\n");
  return SQLITE_OK;
}

static int jonoondb_update(sqlite3_vtab* vtab, int argc, sqlite3_value **argv,
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

static int jonoondb_begin(sqlite3_vtab* vtab) {
  // printf("BEGIN\n");
  return SQLITE_OK;
}

static int jonoondb_sync(sqlite3_vtab* vtab) {
  //printf("SYNC\n");
  return SQLITE_OK;
}

static int jonoondb_commit(sqlite3_vtab* vtab) {
  //printf("COMMIT\n");
  return SQLITE_OK;
}

static int jonoondb_rollback(sqlite3_vtab* vtab) {
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
