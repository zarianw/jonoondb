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

const char* status_message(status_ptr sts);
void status_destruct(status_ptr sts);

//
// Options Functions
//
typedef struct options* options_ptr;
options_ptr options_construct();
options_ptr options_construct2(bool createDBIfMissing, uint64_t maxDataFileSize,
  bool compressionEnabled, bool isSynchronous, status_ptr* sts);
void options_destruct(options_ptr opt);

bool options_getcreatedbifmissing(options_ptr opt);
void options_setcreatedbifmissing(options_ptr opt, bool value);

bool options_getcompressionenabled(options_ptr opt);
void options_setcompressionenabled(options_ptr opt, bool value);

uint64_t options_getmaxdatafilesize(options_ptr opt);
void options_setmaxdatafilesize(options_ptr opt, uint64_t value);

bool options_getsynchronous(options_ptr opt);
void options_setsynchronous(options_ptr opt, bool value);



//
// Database Functions
//
typedef struct database* database_ptr;
database_ptr database_open(const char* dbPath, const char* dbName, const options_ptr opt, status_ptr* sts);


#ifdef __cplusplus
} // extern "C"
#endif