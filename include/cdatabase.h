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

const char* jonoondb_indexinfo_getindexname();
void jonoondb_indexinfo_setindexname(const char* value);
int32_t jonoondb_indexinfo_gettype();
void jonoondb_indexinfo_settype(int32_t value);
const const char* jonoondb_indexinfo_getcolumnname();
void jonoondb_indexinfo_setcolumnname(const char* columnName);
void jonoondb_indexinfo_setisascending(bool value);
bool jonoondb_indexinfo_getisascending();


//
// Database Functions
//
typedef struct database* database_ptr;
database_ptr jonoondb_database_open(const char* dbPath, const char* dbName, const options_ptr opt, status_ptr* sts);
void jonoondb_database_close(database_ptr db, status_ptr* sts);

#ifdef __cplusplus
} // extern "C"
#endif