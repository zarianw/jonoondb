#include <string>
#include "cdatabase.h"
#include "jonoondb_exceptions.h"

namespace jonoondb_api {
class ex_Status {
public:
  ex_Status() : opaque(nullptr) {
  }

  ~ex_Status() {
    if (opaque) {
      status_destruct(opaque);
    }
  }

  status_ptr opaque;
};

class ThrowOnError {
public:
  ~ThrowOnError() {
    if (m_status.opaque) {
      throw std::runtime_error(status_message(m_status.opaque));
    }
  }

  operator status_ptr*() { return &m_status.opaque; }

private:
  ex_Status m_status;
};

class ex_Options {
public:
  //Default constructor that sets all the options to their default value
  ex_Options() {
    m_opaque = options_construct();
  }
  ex_Options(bool createDBIfMissing, size_t maxDataFileSize,
    bool compressionEnabled, bool synchronous) {    
    m_opaque = options_construct2(createDBIfMissing, maxDataFileSize, compressionEnabled,
      synchronous, ThrowOnError{});
  }

  ex_Options(ex_Options&& other);

  ~ex_Options() {
    options_destruct(m_opaque);
  }

  void SetCreateDBIfMissing(bool value) {
    options_setcreatedbifmissing(m_opaque, value);
  }
  bool GetCreateDBIfMissing() const {
    options_getcreatedbifmissing(m_opaque);
  }

  void SetCompressionEnabled(bool value) {
    options_setcompressionenabled(m_opaque, value);
  }

  bool GetCompressionEnabled() const {
    return options_getcompressionenabled(m_opaque);
  }

  void SetMaxDataFileSize(size_t value) {
    options_setmaxdatafilesize(m_opaque, value);
  }

  size_t GetMaxDataFileSize() const {
    return options_getmaxdatafilesize(m_opaque);
  }

  void SetSynchronous(bool value) {
    options_setsynchronous(m_opaque, value);
  }

  bool GetSynchronous() const {
    return options_getsynchronous(m_opaque);
  }

  const options_ptr GetOpaquePtr() const {
    return m_opaque;
  }

private:  
  options_ptr m_opaque;
};

class ex_Database {
public:
  static ex_Database* Open(const std::string& dbPath, const std::string& dbName,
    const ex_Options& opt) {
    auto db = database_open(dbPath.c_str(), dbName.c_str(), opt.GetOpaquePtr(), ThrowOnError{});
    return new ex_Database(db);
  }

private:
  ex_Database(database_ptr db) : m_db(db) {}
  database_ptr m_db;
};

}  // jonoondb_api