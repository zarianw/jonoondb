#include "cdatabase.h"
#include "status.h"
#include "options.h"
#include "database_impl.h"

using namespace jonoondb_api;

extern "C" {

//
// Status
//
struct status {
  Status impl;
};

void status_destruct(status_ptr sts) {
  delete sts;
}

const char* status_message(status_ptr sts) {
  return sts->impl.GetMessage();
}

//
// Options
//
struct options {
  options(bool createDBIfMissing, int64_t maxDataFileSize,
    bool compressionEnabled, bool synchronous) :
    impl(createDBIfMissing, maxDataFileSize, compressionEnabled, synchronous) {
  }

  Options impl;
};

options_ptr options_construct(bool createDBIfMissing, uint64_t maxDataFileSize,
  bool compressionEnabled, bool synchronous, status_ptr* sts) {
  return new options(createDBIfMissing, maxDataFileSize, compressionEnabled, synchronous);
}

void options_destruct(options_ptr opt) {
  delete opt;
}

bool get_createdbifmissing(options_ptr opt) {
  return opt->impl.GetCreateDBIfMissing();
}

void set_createdbifmissing(options_ptr opt, bool value) {
  return opt->impl.SetCreateDBIfMissing(value);
}

bool get_compressionenabled(options_ptr opt) {
  return opt->impl.GetCompressionEnabled();
}

void set_compressionenabled(options_ptr opt, bool value) {
  return opt->impl.SetCompressionEnabled(value);
}

int64_t get_maxdatafilesize(options_ptr opt) {
  return opt->impl.GetMaxDataFileSize();
}

void set_maxdatafilesize(options_ptr opt, int64_t value) {
  return opt->impl.SetMaxDataFileSize(value);
}

bool get_synchronous(options_ptr opt) {
  return opt->impl.GetSynchronous();
}

void set_synchronous(options_ptr opt, bool value) {
  return opt->impl.SetSynchronous(value);
}

//
// Database
//
struct database {
  database(const char* dbPath, const char* dbName, const Options& opt) {
    DatabaseImpl::Open(dbPath, dbName, opt, impl);
  }

  DatabaseImpl* impl;
};

database_ptr database_open(const char* dbPath, const char* dbName, const options_ptr opt, status_ptr* sts) {
  return new database(dbPath, dbName, opt->impl);
}  

} // extern "C"