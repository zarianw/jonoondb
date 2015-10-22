#include "cdatabase.h"
#include "status.h"
#include "options.h"
#include "jonoondb_exceptions.h"

using namespace jonoondb_api;

//
// Error Handling
//
// Returns true if fn executed without throwing an error, false otherwise.
// If calling fn threw an error, capture it in *out_error.
template<typename Fn>
bool translateExceptions(Fn&& fn, status_ptr& sts) {
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
    sts = new status(kStatusInvalidArgumentCode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (...) {
    sts = new status(kStatusInvalidArgumentCode, "Unknown Error.", "", "", "");
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
  options(bool createDBIfMissing, int64_t maxDataFileSize,
    bool compressionEnabled, bool synchronous) :
    impl(createDBIfMissing, maxDataFileSize, compressionEnabled, synchronous) {
  }

  Options impl;
};

options_ptr options_construct(bool createDBIfMissing, int64_t maxDataFileSize,
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

size_t get_maxdatafilesize(options_ptr opt) {
  return opt->impl.GetMaxDataFileSize();
}

void set_maxdatafilesize(options_ptr opt, int64_t value) {
  return opt->impl.SetMaxDataFileSize(value);
}

void set_synchronous(options_ptr opt, bool value) {
  return opt->impl.SetSynchronous(value);
}

bool get_synchronous(options_ptr opt) {
  return opt->impl.GetSynchronous();
}

//
// Database
//
struct database {
  database(const char* dbPath, const char* dbName, const options_ptr opt, status_ptr* sts) {

  }
};

database_ptr database_open(const char* dbPath, const char* dbName, const options_ptr opt, status_ptr* sts) {

}
  

} // extern "C"