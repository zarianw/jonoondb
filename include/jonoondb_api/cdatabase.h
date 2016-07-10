#pragma once

#include <stdint.h>
#include <stdbool.h>


#ifdef __cplusplus
extern "C" {
#endif

//
// Status Functions
//

typedef enum jonoondb_status_codes {
  status_genericerrorcode = 1,
  status_invalidargumentcode = 2,
  status_missingdatabasefilecode = 3,
  status_missingdatabasefoldercode = 4,
  status_outofmemoryerrorcode = 5,
  status_duplicatekeyerrorcode = 6,
  status_collectionalreadyexistcode = 7,
  status_indexalreadyexistcode = 8,
  status_collectionnotfoundcode = 9,
  status_invalidschemaerrorcode = 10,
  status_indexoutofbounderrorcode = 11,
  status_sqlerrorcode = 12,
  status_fileioerrorcode = 13
} jonoondb_status_codes;

typedef struct status* status_ptr;

void jonoondb_status_destruct(status_ptr sts);
const char* jonoondb_status_message(status_ptr sts);
uint64_t jonoondb_status_code(status_ptr sts);
const char* jonoondb_status_file(status_ptr sts);
const char* jonoondb_status_function(status_ptr sts);
uint64_t jonoondb_status_line(status_ptr sts);

//
// Options Functions
//
typedef struct options* options_ptr;
options_ptr jonoondb_options_construct();
options_ptr jonoondb_options_copy_construct(const options_ptr other);
options_ptr jonoondb_options_construct2
    (bool createDBIfMissing,
     uint64_t maxDataFileSize,
     bool compressionEnabled,
     bool isSynchronous,
     uint64_t memCleanupThresholdInBytes,
     status_ptr* sts);
void jonoondb_options_destruct(options_ptr opt);

bool jonoondb_options_getcreatedbifmissing(options_ptr opt);
void jonoondb_options_setcreatedbifmissing(options_ptr opt, bool value);

bool jonoondb_options_getcompressionenabled(options_ptr opt);
void jonoondb_options_setcompressionenabled(options_ptr opt, bool value);

uint64_t jonoondb_options_getmaxdatafilesize(options_ptr opt);
void jonoondb_options_setmaxdatafilesize(options_ptr opt, uint64_t value);

bool jonoondb_options_getsynchronous(options_ptr opt);
void jonoondb_options_setsynchronous(options_ptr opt, bool value);

uint64_t jonoondb_options_getmemorycleanupthreshold(options_ptr opt);
void jonoondb_options_setmemorycleanupthreshold
    (options_ptr opt, uint64_t valueInBytes);

//
// IndexInfo Functions
//
typedef struct indexinfo* indexinfo_ptr;
indexinfo_ptr jonoondb_indexinfo_construct();
indexinfo_ptr jonoondb_indexinfo_construct2
    (const char* indexName, int32_t type, const char* columnName,
     bool isAscending, status_ptr* sts);
void jonoondb_indexinfo_destruct(indexinfo_ptr indexInfo);

const char* jonoondb_indexinfo_getindexname(indexinfo_ptr indexInfo);
void
    jonoondb_indexinfo_setindexname(indexinfo_ptr indexInfo, const char* value);
int32_t jonoondb_indexinfo_gettype(indexinfo_ptr indexInfo);
void jonoondb_indexinfo_settype(indexinfo_ptr indexInfo, int32_t value);
const char* jonoondb_indexinfo_getcolumnname(indexinfo_ptr indexInfo);
void jonoondb_indexinfo_setcolumnname
    (indexinfo_ptr indexInfo, const char* columnName);
void jonoondb_indexinfo_setisascending(indexinfo_ptr indexInfo, bool value);
bool jonoondb_indexinfo_getisascending(indexinfo_ptr indexInfo);

//
// Buffer Functions
//
typedef struct jonoondb_buffer* jonoondb_buffer_ptr;
jonoondb_buffer_ptr jonoondb_buffer_construct();
jonoondb_buffer_ptr jonoondb_buffer_construct2(uint64_t bufferCapacityInBytes,
                                               status_ptr* sts);
jonoondb_buffer_ptr jonoondb_buffer_construct3(const char* buffer,
                                               uint64_t bufferLengthInBytes,
                                               uint64_t bufferCapacityInBytes,
                                               status_ptr* sts);
jonoondb_buffer_ptr jonoondb_buffer_construct4(char* buffer,
                                               uint64_t bufferLengthInBytes,
                                               uint64_t bufferCapacityInBytes,
                                               void(* customDeleterFunc)(char*),
                                               status_ptr* sts);

jonoondb_buffer_ptr
    jonoondb_buffer_copy_construct(jonoondb_buffer_ptr buf, status_ptr* sts);
void jonoondb_buffer_copy_assignment
    (jonoondb_buffer_ptr self, jonoondb_buffer_ptr other, status_ptr* sts);
void jonoondb_buffer_destruct(jonoondb_buffer_ptr buf);
int32_t jonoondb_buffer_op_lessthan
    (jonoondb_buffer_ptr self, jonoondb_buffer_ptr other);
int32_t jonoondb_buffer_op_lessthanorequal(jonoondb_buffer_ptr self,
                                           jonoondb_buffer_ptr other);
int32_t jonoondb_buffer_op_greaterthan(jonoondb_buffer_ptr self,
                                       jonoondb_buffer_ptr other);
int32_t jonoondb_buffer_op_greaterthanorequal(jonoondb_buffer_ptr self,
                                              jonoondb_buffer_ptr other);
int32_t jonoondb_buffer_op_equal(jonoondb_buffer_ptr self,
                                 jonoondb_buffer_ptr other);
int32_t jonoondb_buffer_op_notequal(jonoondb_buffer_ptr self,
                                    jonoondb_buffer_ptr other);
void jonoondb_buffer_copy(jonoondb_buffer_ptr buf,
                          const char* srcBuf,
                          uint64_t bytesToCopy,
                          status_ptr* sts);
void jonoondb_buffer_resize(jonoondb_buffer_ptr buf,
                            uint64_t newBufferCapacityInBytes,
                            status_ptr* sts);
const char* jonoondb_buffer_getdata(jonoondb_buffer_ptr buf);
uint64_t jonoondb_buffer_getlength(jonoondb_buffer_ptr buf);
uint64_t jonoondb_buffer_getcapacity(jonoondb_buffer_ptr buf);

//
// ResultSet Functions
//
typedef struct resultset* resultset_ptr;
void jonoondb_resultset_destruct(resultset_ptr rs);
int32_t jonoondb_resultset_next(resultset_ptr rs);
int64_t jonoondb_resultset_getinteger
    (resultset_ptr rs, int32_t columnIndex, status_ptr* sts);
double jonoondb_resultset_getdouble
    (resultset_ptr rs, int32_t columnIndex, status_ptr* sts);
const char* jonoondb_resultset_getstring(resultset_ptr rs,
                                         int32_t columnIndex,
                                         uint64_t** retValSize,
                                         status_ptr* sts);
const char* jonoondb_resultset_getblob(resultset_ptr rs,
                                       int32_t columnIndex,
                                       uint64_t** retValSize,
                                       status_ptr* sts);
int32_t jonoondb_resultset_getcolumnindex(resultset_ptr rs,
                                          const char* columnLabel,
                                          uint64_t columnLabelLength,
                                          status_ptr* sts);
int32_t jonoondb_resultset_getcolumncount(resultset_ptr rs);
int32_t jonoondb_resultset_getcolumntype
    (resultset_ptr rs, int32_t columnIndex, status_ptr* sts);
const char* jonoondb_resultset_getcolumnlabel(resultset_ptr rs,
                                              int32_t columnIndex,
                                              uint64_t** retValSize,
                                              status_ptr* sts);
int32_t jonoondb_resultset_isnull
    (resultset_ptr rs, int32_t columnIndex, status_ptr* sts);

//
// Database Functions
//
typedef struct database* database_ptr;
database_ptr jonoondb_database_construct(const char* dbPath,
                                         const char* dbName,
                                         const options_ptr opt,
                                         status_ptr* sts);
void jonoondb_database_destruct(database_ptr db);
void jonoondb_database_createcollection
    (database_ptr db,
     const char* name,
     int32_t schemaType,
     const char* schema,
     uint64_t schemaSize,
     indexinfo_ptr* indexes,
     uint64_t indexesLength,
     status_ptr* sts);
void jonoondb_database_insert(database_ptr db,
                              const char* collectionName,
                              const jonoondb_buffer_ptr documentData,
                              status_ptr* sts);
void jonoondb_database_multi_insert
    (database_ptr db, const char* collectionName, uint64_t collectionNameLength,
     const jonoondb_buffer_ptr* documentArr, uint64_t documentArrLength,
     status_ptr* sts);
resultset_ptr jonoondb_database_executeselect(database_ptr db,
                                              const char* selectStmt,
                                              uint64_t selectStmtLength,
                                              status_ptr* sts);

#ifdef __cplusplus
} // extern "C"
#endif