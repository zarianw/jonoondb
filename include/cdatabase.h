#pragma once

#include <stdint.h>
#include <stdbool.h>


#ifdef __cplusplus
extern "C" {
#endif

//
// Status Functions
//
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
options_ptr jonoondb_options_construct2(bool createDBIfMissing, uint64_t maxDataFileSize,
  bool compressionEnabled, bool isSynchronous, status_ptr* sts);
void jonoondb_options_destruct(options_ptr opt);

bool jonoondb_options_getcreatedbifmissing(options_ptr opt);
void jonoondb_options_setcreatedbifmissing(options_ptr opt, bool value);

bool jonoondb_options_getcompressionenabled(options_ptr opt);
void jonoondb_options_setcompressionenabled(options_ptr opt, bool value);

uint64_t jonoondb_options_getmaxdatafilesize(options_ptr opt);
void jonoondb_options_setmaxdatafilesize(options_ptr opt, uint64_t value);

bool jonoondb_options_getsynchronous(options_ptr opt);
void jonoondb_options_setsynchronous(options_ptr opt, bool value);

//
// IndexInfo Functions
//
typedef struct indexinfo* indexinfo_ptr;
indexinfo_ptr jonoondb_indexinfo_construct();
indexinfo_ptr jonoondb_indexinfo_construct2(const char* indexName, int32_t type, const char* columnName,
                                   bool isAscending, status_ptr* sts);
void jonoondb_indexinfo_destruct(indexinfo_ptr indexInfo);

const char* jonoondb_indexinfo_getindexname(indexinfo_ptr indexInfo);
void jonoondb_indexinfo_setindexname(indexinfo_ptr indexInfo, const char* value);
int32_t jonoondb_indexinfo_gettype(indexinfo_ptr indexInfo);
void jonoondb_indexinfo_settype(indexinfo_ptr indexInfo, int32_t value);
const char* jonoondb_indexinfo_getcolumnname(indexinfo_ptr indexInfo);
void jonoondb_indexinfo_setcolumnname(indexinfo_ptr indexInfo, const char* columnName);
void jonoondb_indexinfo_setisascending(indexinfo_ptr indexInfo, bool value);
bool jonoondb_indexinfo_getisascending(indexinfo_ptr indexInfo);

//
// IndexInfoVectorView Functions
//
typedef struct indexinfo_vectorview* indexinfo_vectorview_ptr;
indexinfo_vectorview_ptr jonoondb_indexinfo_vectorview_construct(indexinfo_ptr indexes, uint64_t indexesLength, status_ptr* sts);
indexinfo_vectorview_ptr jonoondb_indexinfo_vectorview_construct2();
void jonoondb_indexinfo_vectorview_destruct(indexinfo_vectorview_ptr vecView);
void jonoondb_indexinfo_vectorview_push_back(indexinfo_vectorview_ptr vecView, indexinfo_ptr val, status_ptr* sts);

//
// Database Functions
//
typedef struct jonoondb_buffer* jonoondb_buffer_ptr;
jonoondb_buffer_ptr jonoondb_buffer_construct();
void jonoondb_buffer_destruct(jonoondb_buffer_ptr buf);
void jonoondb_buffer_copy(jonoondb_buffer_ptr buf, const char* srcBuf, uint64_t bytesToCopy, status_ptr* sts);
void jonoondb_buffer_resize(jonoondb_buffer_ptr buf, uint64_t newBufferCapacityInBytes, status_ptr* sts);
uint64_t jonoondb_buffer_getcapacity(jonoondb_buffer_ptr buf);

//
// Database Functions
//
typedef struct database* database_ptr;
database_ptr jonoondb_database_open(const char* dbPath, const char* dbName, const options_ptr opt, status_ptr* sts);
void jonoondb_database_close(database_ptr db, status_ptr* sts);
void jonoondb_database_createcollection(database_ptr db, const char* name, int32_t schemaType, const char* schema,
                                        indexinfo_ptr* indexes, uint64_t indexesLength, status_ptr* sts);
void jonoondb_database_insert(database_ptr db, const char* collectionName, const jonoondb_buffer_ptr documentData, status_ptr* sts);

#ifdef __cplusplus
} // extern "C"
#endif