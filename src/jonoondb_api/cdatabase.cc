#include "cdatabase.h"
#include "status.h"
#include "options.h"
#include "database_impl.h"
#include "jonoondb_exceptions.h"

using namespace jonoondb_api;

//
// Error Handling
//
// Returns true if fn executed without throwing an error, false otherwise.
// If calling fn threw an error, capture it in *out_error.
template<typename Fn>
bool TranslateExceptions(Fn&& fn, status_ptr& sts) {
  bool retVal = false;
  try {
    fn();
    retVal = true;
  } catch (const InvalidArgumentException& ex) {
    sts = new status(kStatusInvalidArgumentCode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const MissingDatabaseFileException& ex) {
    sts = new status(kStatusInvalidArgumentCode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const MissingDatabaseFolderException& ex) {
    sts = new status(kStatusInvalidArgumentCode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const OutOfMemoryException& ex) {
    sts = new status(kStatusInvalidArgumentCode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const DuplicateKeyException& ex) {
    sts = new status(kStatusInvalidArgumentCode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const CollectionAlreadyExistException& ex) {
    sts = new status(kStatusInvalidArgumentCode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const IndexAlreadyExistException& ex) {
    sts = new status(kStatusInvalidArgumentCode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const CollectionNotFoundException& ex) {
    sts = new status(kStatusInvalidArgumentCode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const SchemaParseException& ex) {
    sts = new status(kStatusInvalidArgumentCode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const IndexOutOfBoundException& ex) {
    sts = new status(kStatusInvalidArgumentCode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const SQLException& ex) {
    sts = new status(kStatusInvalidArgumentCode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const JonoonDBException& ex) {
    sts = new status(kStatusInvalidArgumentCode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const std::exception& ex) {
    sts = new status(kStatusInvalidArgumentCode, ex.what(), "", "", 0);
  } catch (...) {
    sts = new status(kStatusInvalidArgumentCode, "Unknown Error.", "", "", 0);
  }

  return retVal;
}

extern "C" {
//
// Status
//
struct status {
  status(std::size_t code, const char* message, const char* srcFileName,
    const char* funcName, std::size_t lineNum) :
    impl(code, message, srcFileName, funcName, lineNum) {
  }

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
  options() : impl() {}

  options(bool createDBIfMissing, int64_t maxDataFileSize,
    bool compressionEnabled, bool synchronous) :
    impl(createDBIfMissing, maxDataFileSize, compressionEnabled, synchronous) {
  }

  Options impl;
};

options_ptr options_construct() {
  return new options();
}

options_ptr options_construct2(bool createDBIfMissing, uint64_t maxDataFileSize,
  bool compressionEnabled, bool synchronous, status_ptr* sts) {
  return new options(createDBIfMissing, maxDataFileSize, compressionEnabled, synchronous);
}

void options_destruct(options_ptr opt) {
  delete opt;
}

bool options_getcreatedbifmissing(options_ptr opt) {
  return opt->impl.GetCreateDBIfMissing();
}

void options_setcreatedbifmissing(options_ptr opt, bool value) {
  return opt->impl.SetCreateDBIfMissing(value);
}

bool options_getcompressionenabled(options_ptr opt) {
  return opt->impl.GetCompressionEnabled();
}

void options_setcompressionenabled(options_ptr opt, bool value) {
  return opt->impl.SetCompressionEnabled(value);
}

uint64_t options_getmaxdatafilesize(options_ptr opt) {
  return opt->impl.GetMaxDataFileSize();
}

void options_setmaxdatafilesize(options_ptr opt, uint64_t value) {
  return opt->impl.SetMaxDataFileSize(value);
}

bool options_getsynchronous(options_ptr opt) {
  return opt->impl.GetSynchronous();
}

void options_setsynchronous(options_ptr opt, bool value) {
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
  database_ptr db = nullptr;
  TranslateExceptions([&]{
    db = new database(dbPath, dbName, opt->impl);
  }, *sts);
  
  return db;
}
  
} // extern "C"