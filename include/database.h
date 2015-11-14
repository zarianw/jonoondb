#pragma once

#include <string>
#include <vector>
#include "cdatabase.h"
#include "jonoondb_exceptions.h"
#include "enums.h"

namespace jonoondb_api {

//
// Status
//
class ex_Status {
public:
  ex_Status() : opaque(nullptr) {
  }

  ex_Status(status_ptr sts) : opaque(sts) {
  }

  ex_Status(ex_Status&& other) {
    if (this != &other) {
      this->opaque = other.opaque;
      other.opaque = nullptr;
    }
  }

  ~ex_Status() {
    if (opaque) {
      jonoondb_status_destruct(opaque);
    }
  }

  status_ptr opaque;
};

//
// ThrowOnError
//
class ThrowOnError {
public:
  // TODO: change the throw to noexcept(false) when we move to vs2015
  ~ThrowOnError() throw(std::exception) {
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
        case status_fileioerrorcode:
          throw FileIOException(jonoondb_status_message(m_status.opaque),
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

//
// Options
//
class Options {
public:
  //Default constructor that sets all the options to their default value
  Options() {
    m_opaque = jonoondb_options_construct();
  }
  Options(bool createDBIfMissing, size_t maxDataFileSize,
    bool compressionEnabled, bool synchronous) {    
    m_opaque = jonoondb_options_construct2(createDBIfMissing, maxDataFileSize, compressionEnabled,
      synchronous, ThrowOnError{});
  }

  Options(Options&& other) {
    if (this != &other) {
      this->m_opaque = other.m_opaque;
      other.m_opaque = nullptr;
    }
  }

  ~Options() {
    if (m_opaque != nullptr) {
      jonoondb_options_destruct(m_opaque);
    }
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
// IndexInfo
//
class IndexInfo {
public:
  IndexInfo() {
    m_opaque = jonoondb_indexinfo_construct();
  }

  IndexInfo(const std::string& indexName, IndexType type, const std::string& columnName, bool isAscending) {
    m_opaque = jonoondb_indexinfo_construct2(indexName.c_str(), 0, columnName.c_str(), isAscending, ThrowOnError{});
  }

  IndexInfo(IndexInfo&& other) {
    if (this != &other) {
      this->m_opaque = other.m_opaque;
      other.m_opaque = nullptr;
    }
  }
    
  IndexInfo(const IndexInfo& other) {
    if (this != &other) {
      m_opaque = jonoondb_indexinfo_construct();
      this->SetIndexName(other.GetIndexName());
      this->SetColumnName(other.GetColumnName());
      this->SetIsAscending(other.GetIsAscending());
      this->SetType(other.GetType());
    }
  }

  ~IndexInfo() {
    if (m_opaque != nullptr) {
      jonoondb_indexinfo_destruct(m_opaque);
    }
  }

  IndexInfo& operator=(const IndexInfo& other) {
    if (this != &other) {
      m_opaque = jonoondb_indexinfo_construct();
      this->SetIndexName(other.GetIndexName());
      this->SetColumnName(other.GetColumnName());
      this->SetIsAscending(other.GetIsAscending());
      this->SetType(other.GetType());
    }

    return *this;
  }

  IndexInfo& operator=(IndexInfo&& other) {
    if (this != &other) {
      this->m_opaque = other.m_opaque;
      other.m_opaque = nullptr;
    }

    return *this;
  }
  
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
    return jonoondb_indexinfo_getisascending(m_opaque);
  }

  indexinfo_ptr GetOpaqueType() const {
    return m_opaque;
  }

private:
  indexinfo_ptr m_opaque;
};

//
// Buffer
//
class Buffer {
public:
  //Buffer(const Buffer& other);
  //Buffer& operator=(const Buffer& other);
  Buffer() {
    m_opaque = jonoondb_buffer_construct();
  }
  
  Buffer(Buffer&& other) {
    if (this != &other) {
      this->m_opaque = other.m_opaque;
      other.m_opaque = nullptr;
    }
  }
  
  ~Buffer() {
    if (m_opaque != nullptr) {
      jonoondb_buffer_destruct(m_opaque);
    }
  }
  /*Buffer& operator=(Buffer&& other);
  bool operator<(const Buffer& other) const;

  Status Assign(char* buffer, size_t bufferLengthInBytes,
    size_t bufferCapacityInBytes, DeleterFuncPtr customDeleterFunc);
  Status Assign(Buffer& buffer, DeleterFuncPtr customDeleterFunc);

  Status Copy(const char* buffer, size_t bufferLengthInBytes,
    size_t bufferCapacityInBytes);
  Status Copy(const Buffer& buffer);*/
  void Copy(const char* srcBuffer, size_t bytesToCopy) {
    jonoondb_buffer_copy(m_opaque, srcBuffer, bytesToCopy, ThrowOnError{});
  }

  void Resize(size_t newBufferCapacityInBytes) {
    jonoondb_buffer_resize(m_opaque, newBufferCapacityInBytes, ThrowOnError{});
  }

  //void Reset();
  //const char* GetData() const;
  //char* GetDataForWrite();
  const size_t GetCapacity() const {
    return jonoondb_buffer_getcapacity(m_opaque);
  }
  //const size_t GetLength() const;
  //Status SetLength(size_t value);*/
  jonoondb_buffer_ptr GetOpaqueType() const {
    return m_opaque;
  }

private:
  jonoondb_buffer_ptr m_opaque;
};

class ResultSet {
public:
  ResultSet(resultset_ptr opaque) : m_opaque(opaque) {
  }

  ResultSet(const ResultSet& other) = delete;
  ResultSet(ResultSet&& other) {
    if (this != &other) {
      if (m_opaque != nullptr) {
        jonoondb_resultset_destruct(m_opaque);
      }
      this->m_opaque = other.m_opaque;
      other.m_opaque = nullptr;
    }
  }

  ~ResultSet() {
    if (m_opaque != nullptr) {
      jonoondb_resultset_destruct(m_opaque);
    }
  }

  ResultSet& operator=(const ResultSet& other) = delete;
  ResultSet& operator=(ResultSet&& other) {
    if (this != &other) {
      if (m_opaque != nullptr) {
        jonoondb_resultset_destruct(m_opaque);
      }
      this->m_opaque = other.m_opaque;
      other.m_opaque = nullptr;
    }

    return *this;
  }

  bool Next();

  std::int8_t GetInt8(const std::string& fieldName) const;
  std::int16_t GetInt16(const std::string& fieldName) const;
  std::int32_t GetInt32(const std::string& fieldName) const;
  std::int64_t GetInt64(const std::string& fieldName) const;

  std::uint8_t GetUInt8(const std::string& fieldName) const;
  std::uint16_t GetUInt16(const std::string& fieldName) const;
  std::uint32_t GetUInt32(const std::string& fieldName) const;
  std::uint64_t GetUInt64(const std::string& fieldName) const;

  float GetFloat(const std::string& fieldName) const;
  double GetDouble(const std::string& fieldName) const;

  const char* GetStringValue(const std::string& fieldName) const;
private:
  resultset_ptr m_opaque;
};

class Database {
public:
  static Database* Open(const std::string& dbPath, const std::string& dbName,
    const Options& opt) {
    auto db = jonoondb_database_open(dbPath.c_str(), dbName.c_str(), opt.GetOpaquePtr(), ThrowOnError{});
    return new Database(db);
  }

  void Close() {
    status_ptr sts = nullptr;
    jonoondb_database_close(m_opaque, &sts);
    if (sts != nullptr) {
      //Todo: Handle errors that can happen on shutdown
    }

    delete this;
  }

  void CreateCollection(const std::string& name, SchemaType schemaType,
                        const std::string& schema, const std::vector<IndexInfo>& indexes) {
    std::vector<indexinfo_ptr> vec;
    for (auto& item : indexes) {
      vec.push_back(item.GetOpaqueType());
    }

    jonoondb_database_createcollection(m_opaque, name.c_str(),
                                       static_cast<int32_t>(schemaType),
                                       schema.c_str(),
                                       vec.data(),
                                       vec.size(),
                                       ThrowOnError{});
  }

  void Insert(const std::string& collectionName, const Buffer& documentData) {
    jonoondb_database_insert(m_opaque, collectionName.c_str(), documentData.GetOpaqueType(), ThrowOnError{});
  }

  ResultSet ExecuteSelect(const std::string& selectStatement) {
    auto rs = jonoondb_database_executeselect(m_opaque, selectStatement.c_str(), ThrowOnError{});
    return ResultSet(rs);
  }

  

private:
  Database(database_ptr db) : m_opaque(db) {}
  database_ptr m_opaque;
};

}  // jonoondb_api
