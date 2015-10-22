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
options_ptr options_construct(bool createDBIfMissing, uint64_t maxDataFileSize,
  bool compressionEnabled, bool isSynchronous, status_ptr* sts);
void options_destruct(options_ptr opt);

bool get_createdbifmissing(options_ptr opt);
void set_createdbifmissing(options_ptr opt, bool value);

bool get_compressionenabled(options_ptr opt);
void set_compressionenabled(options_ptr opt, bool value);

/*int64_t get_maxdatafilesize((options_ptr opt);
void set_maxdatafilesize((options_ptr opt, int64_t value);

bool get_synchronous((options_ptr opt);
void set_synchronous((options_ptr opt, bool value);*/



//
// Database Functions
//
typedef struct database* database_ptr;
database_ptr database_open(const char* dbPath, const char* dbName, const options_ptr opt, status_ptr* sts);


#ifdef __cplusplus
} // extern "C"
#endif