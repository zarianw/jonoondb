#include <string>
#include <vector>
#include "cdatabase.h"
#include "cenums.h"
#include "jonoondb_exceptions.h"
#include "enums.h"


namespace jonoondb_api {
class ex_Status {
public:
  ex_Status() : opaque(nullptr) {
  }

  ~ex_Status() {
    if (opaque) {
      jonoondb_status_destruct(opaque);
    }
  }

  status_ptr opaque;
};

class ThrowOnError {
public:
  ~ThrowOnError() {
    if (m_status.opaque) {
      switch (jonoondb_status_code(m_status.opaque)) {
        case status_genericerrorcode:
          throw JonoonDBException(jonoondb_status_message(m_status.opaque),
            jonoondb_status_file(m_status.opaque),
            jonoondb_status_function(m_status.opaque),
            jonoondb_status_line(m_status.opaque));
          break;
        case status_invalidargumentcode:
          throw InvalidArgumentException(jonoondb_status_message(m_status.opaque),
            jonoondb_status_file(m_status.opaque),
            jonoondb_status_function(m_status.opaque),
            jonoondb_status_line(m_status.opaque));
          break;
        case status_missingdatabasefilecode:
          throw MissingDatabaseFileException(jonoondb_status_message(m_status.opaque),
            jonoondb_status_file(m_status.opaque),
            jonoondb_status_function(m_status.opaque),
            jonoondb_status_line(m_status.opaque));
          break;
        case status_missingdatabasefoldercode:
          throw MissingDatabaseFolderException(jonoondb_status_message(m_status.opaque),
            jonoondb_status_file(m_status.opaque),
            jonoondb_status_function(m_status.opaque),
            jonoondb_status_line(m_status.opaque));
          break;
        case status_outofmemoryerrorcode:
          throw OutOfMemoryException(jonoondb_status_message(m_status.opaque),
            jonoondb_status_file(m_status.opaque),
            jonoondb_status_function(m_status.opaque),
            jonoondb_status_line(m_status.opaque));
          break;
        case status_duplicatekeyerrorcode:
          throw DuplicateKeyException(jonoondb_status_message(m_status.opaque),
            jonoondb_status_file(m_status.opaque),
            jonoondb_status_function(m_status.opaque),
            jonoondb_status_line(m_status.opaque));
          break;                
        case status_collectionalreadyexistcode:
          throw CollectionAlreadyExistException(jonoondb_status_message(m_status.opaque),
            jonoondb_status_file(m_status.opaque),
            jonoondb_status_function(m_status.opaque),
            jonoondb_status_line(m_status.opaque));
          break;
        case status_indexalreadyexistcode:
          throw IndexAlreadyExistException(jonoondb_status_message(m_status.opaque),
            jonoondb_status_file(m_status.opaque),
            jonoondb_status_function(m_status.opaque),
            jonoondb_status_line(m_status.opaque));
          break;
        case status_collectionnotfoundcode:
          throw CollectionNotFoundException(jonoondb_status_message(m_status.opaque),
            jonoondb_status_file(m_status.opaque),
            jonoondb_status_function(m_status.opaque),
            jonoondb_status_line(m_status.opaque));
          break;
        case status_schemaparseerrorcode:
          throw SchemaParseException(jonoondb_status_message(m_status.opaque),
            jonoondb_status_file(m_status.opaque),
            jonoondb_status_function(m_status.opaque),
            jonoondb_status_line(m_status.opaque));
          break;
        case status_indexoutofbounderrorcode:
          throw IndexOutOfBoundException(jonoondb_status_message(m_status.opaque),
            jonoondb_status_file(m_status.opaque),
            jonoondb_status_function(m_status.opaque),
            jonoondb_status_line(m_status.opaque));
          break;
        case status_sqlerrorcode:
          throw SQLException(jonoondb_status_message(m_status.opaque),
            jonoondb_status_file(m_status.opaque),
            jonoondb_status_function(m_status.opaque),
            jonoondb_status_line(m_status.opaque));
          break;
        default:          
          throw std::runtime_error(jonoondb_status_message(m_status.opaque));
          break;
      }      
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
    m_opaque = jonoondb_options_construct();
  }
  ex_Options(bool createDBIfMissing, size_t maxDataFileSize,
    bool compressionEnabled, bool synchronous) {    
    m_opaque = jonoondb_options_construct2(createDBIfMissing, maxDataFileSize, compressionEnabled,
      synchronous, ThrowOnError{});
  }

  ex_Options(ex_Options&& other);

  ~ex_Options() {
    jonoondb_options_destruct(m_opaque);
  }

  void SetCreateDBIfMissing(bool value) {
    jonoondb_options_setcreatedbifmissing(m_opaque, value);
  }
  bool GetCreateDBIfMissing() const {
    jonoondb_options_getcreatedbifmissing(m_opaque);
  }

  void SetCompressionEnabled(bool value) {
    jonoondb_options_setcompressionenabled(m_opaque, value);
  }

  bool GetCompressionEnabled() const {
    return jonoondb_options_getcompressionenabled(m_opaque);
  }

  void SetMaxDataFileSize(size_t value) {
    jonoondb_options_setmaxdatafilesize(m_opaque, value);
  }

  size_t GetMaxDataFileSize() const {
    return jonoondb_options_getmaxdatafilesize(m_opaque);
  }

  void SetSynchronous(bool value) {
    jonoondb_options_setsynchronous(m_opaque, value);
  }

  bool GetSynchronous() const {
    return jonoondb_options_getsynchronous(m_opaque);
  }

  const options_ptr GetOpaquePtr() const {
    return m_opaque;
  }

private:  
  options_ptr m_opaque;
};

//
// IndexInfo Functions
//
class ex_IndexInfo {
public:
  ex_IndexInfo() {
    m_opaque = jonoondb_indexinfo_construct();
  }

  ex_IndexInfo(const std::string& indexName, IndexType type, const std::string& columnName, bool isAscending) {
    m_opaque = jonoondb_indexinfo_construct2(indexName.c_str(), 0, columnName.c_str(), isAscending, ThrowOnError{});
  }
    
  ex_IndexInfo(const ex_IndexInfo& other);
  ~ex_IndexInfo() {
    jonoondb_indexinfo_destruct(m_opaque);
  }

  ex_IndexInfo& operator=(const ex_IndexInfo& other);
  
  const char* GetIndexName() const {
    return jonoondb_indexinfo_getindexname(m_opaque);
  }

  void SetIndexName(const std::string& value) {
    jonoondb_indexinfo_setindexname(m_opaque, value.c_str());
  }

  IndexType GetType() const {
    return ToIndexType(jonoondb_indexinfo_gettype(m_opaque));
  }

  void SetType(IndexType value) {
    jonoondb_indexinfo_settype(m_opaque, static_cast<int32_t>(value));
  }

  const char* GetColumnName() const {
    return jonoondb_indexinfo_getcolumnname(m_opaque);
  }

  void SetColumnName(const std::string& value) {
    jonoondb_indexinfo_setcolumnname(m_opaque, value.c_str());
  }

  void SetIsAscending(bool value) {
    jonoondb_indexinfo_setisascending(m_opaque, value);
  }

  bool GetIsAscending() const {
    jonoondb_indexinfo_getisascending(m_opaque);
  }

private:
  indexinfo_ptr m_opaque;
};

class ex_Database {
public:
  static ex_Database* Open(const std::string& dbPath, const std::string& dbName,
    const ex_Options& opt) {
    auto db = jonoondb_database_open(dbPath.c_str(), dbName.c_str(), opt.GetOpaquePtr(), ThrowOnError{});
    return new ex_Database(db);
  }

  void Close() {
    jonoondb_database_close(m_opaque, ThrowOnError{});
  }

  void CreateCollection(const std::string& name, SchemaType schemaType,
    const std::string& schema, const std::vector<ex_IndexInfo>& indexes) {

  }

private:
  ex_Database(database_ptr db) : m_opaque(db) {}
  database_ptr m_opaque;
};

}  // jonoondb_api