#include "cdatabase.h"
#include "status.h"
#include "options.h"

using namespace jonoondb_api;

extern "C" {

//
// Status Functions
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
// Options Functions
//
struct options {
  template<typename... Args>
  options(Args&&... args) : impl(std::forward<Args>(args)...) {
  }

  Options impl;
};

void options_construct(bool createDBIfMissing, int64_t maxDataFileSize,
  bool compressionEnabled, bool isSynchronous) {
   
}

bool get_createdbifmissing(options_ptr opt);
void set_createdbifmissing(options_ptr opt, bool value);

bool get_compressionenabled(options_ptr opt);
void set_compressionenabled(options_ptr opt, bool value);

size_t get_maxdatafilesize();
void set_maxdatafilesize(int64_t value);

void set_synchronous(bool value);
bool get_synchronous();

void options_destruct(options_ptr opt);

  

} // extern "C"