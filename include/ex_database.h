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
  ex_Options();
  ex_Options(bool createDBIfMissing, size_t maxDataFileSize,
    bool compressionEnabled, bool synchronous);
  ex_Options(ex_Options&& other);

  ~ex_Options();

  void SetCreateDBIfMissing(bool value);
  bool GetCreateDBIfMissing() const;

  void SetCompressionEnabled(bool value);
  bool GetCompressionEnabled() const;

  void SetMaxDataFileSize(size_t value);
  size_t GetMaxDataFileSize() const;

  void SetSynchronous(bool value);
  bool GetSynchronous() const;

  const options_ptr GetOpaquePtr() const {
    return m_options;
  }

private:  
  options_ptr m_options;
};

class ex_Database {
public:
  static ex_Database* Open(const std::string& dbPath, const std::string& dbName,
    const ex_Options& opt) {
    auto p = database_open(dbPath.c_str(), dbName.c_str(), opt.GetOpaquePtr(), ThrowOnError{});
  }
};

}  // jonoondb_api