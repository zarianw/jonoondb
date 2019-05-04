#pragma once

#include <assert.h>
#include <string>
#include <vector>
#include "cdatabase.h"
#include "enums.h"
#include "jonoondb_exceptions.h"

namespace jonoondb_api {
//
// StringView
//
class StringView {
 public:
  StringView(const char* str, std::size_t size) : m_str(str), m_size(size) {}

  const char* str() {
    return m_str;
  }

  std::size_t size() {
    return m_size;
  }

 private:
  const char* m_str;
  std::size_t m_size;
};
//
// Status
//
class Status {
 public:
  Status() : opaque(nullptr) {}

  Status(status_ptr sts) : opaque(sts) {}

  Status(Status&& other) {
    this->opaque = other.opaque;
    other.opaque = nullptr;
  }

  ~Status() {
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
  ~ThrowOnError() noexcept(false) {
    if (m_status.opaque) {
      switch (jonoondb_status_code(m_status.opaque)) {
        case status_genericerrorcode:
          throw JonoonDBException(jonoondb_status_message(m_status.opaque),
                                  jonoondb_status_file(m_status.opaque),
                                  jonoondb_status_function(m_status.opaque),
                                  jonoondb_status_line(m_status.opaque));
        case status_invalidargumentcode:
          throw InvalidArgumentException(
              jonoondb_status_message(m_status.opaque),
              jonoondb_status_file(m_status.opaque),
              jonoondb_status_function(m_status.opaque),
              jonoondb_status_line(m_status.opaque));
        case status_missingdatabasefilecode:
          throw MissingDatabaseFileException(
              jonoondb_status_message(m_status.opaque),
              jonoondb_status_file(m_status.opaque),
              jonoondb_status_function(m_status.opaque),
              jonoondb_status_line(m_status.opaque));
        case status_missingdatabasefoldercode:
          throw MissingDatabaseFolderException(
              jonoondb_status_message(m_status.opaque),
              jonoondb_status_file(m_status.opaque),
              jonoondb_status_function(m_status.opaque),
              jonoondb_status_line(m_status.opaque));
        case status_outofmemoryerrorcode:
          throw OutOfMemoryException(jonoondb_status_message(m_status.opaque),
                                     jonoondb_status_file(m_status.opaque),
                                     jonoondb_status_function(m_status.opaque),
                                     jonoondb_status_line(m_status.opaque));
        case status_duplicatekeyerrorcode:
          throw DuplicateKeyException(jonoondb_status_message(m_status.opaque),
                                      jonoondb_status_file(m_status.opaque),
                                      jonoondb_status_function(m_status.opaque),
                                      jonoondb_status_line(m_status.opaque));
        case status_collectionalreadyexistcode:
          throw CollectionAlreadyExistException(
              jonoondb_status_message(m_status.opaque),
              jonoondb_status_file(m_status.opaque),
              jonoondb_status_function(m_status.opaque),
              jonoondb_status_line(m_status.opaque));
        case status_indexalreadyexistcode:
          throw IndexAlreadyExistException(
              jonoondb_status_message(m_status.opaque),
              jonoondb_status_file(m_status.opaque),
              jonoondb_status_function(m_status.opaque),
              jonoondb_status_line(m_status.opaque));
        case status_collectionnotfoundcode:
          throw CollectionNotFoundException(
              jonoondb_status_message(m_status.opaque),
              jonoondb_status_file(m_status.opaque),
              jonoondb_status_function(m_status.opaque),
              jonoondb_status_line(m_status.opaque));
        case status_invalidschemaerrorcode:
          throw InvalidSchemaException(
              jonoondb_status_message(m_status.opaque),
              jonoondb_status_file(m_status.opaque),
              jonoondb_status_function(m_status.opaque),
              jonoondb_status_line(m_status.opaque));
        case status_indexoutofbounderrorcode:
          throw IndexOutOfBoundException(
              jonoondb_status_message(m_status.opaque),
              jonoondb_status_file(m_status.opaque),
              jonoondb_status_function(m_status.opaque),
              jonoondb_status_line(m_status.opaque));
        case status_sqlerrorcode:
          throw SQLException(jonoondb_status_message(m_status.opaque),
                             jonoondb_status_file(m_status.opaque),
                             jonoondb_status_function(m_status.opaque),
                             jonoondb_status_line(m_status.opaque));
        case status_fileioerrorcode:
          throw FileIOException(jonoondb_status_message(m_status.opaque),
                                jonoondb_status_file(m_status.opaque),
                                jonoondb_status_function(m_status.opaque),
                                jonoondb_status_line(m_status.opaque));
        case status_apimisusecode:
          throw ApiMisuseException(jonoondb_status_message(m_status.opaque),
                                   jonoondb_status_file(m_status.opaque),
                                   jonoondb_status_function(m_status.opaque),
                                   jonoondb_status_line(m_status.opaque));
        default:
          // this should not happen
          assert(false);
          throw std::runtime_error(jonoondb_status_message(m_status.opaque));
      }
    }
  }

  operator status_ptr*() {
    return &m_status.opaque;
  }

 private:
  Status m_status;
};

//
// Options
//
class Options {
 public:
  // Default constructor that sets all the options to their default value
  Options() : m_opaque(jonoondb_options_construct()) {}

  Options(bool createDBIfMissing, size_t maxDataFileSize,
          size_t memCleanupThresholdInBytes)
      : m_opaque(jonoondb_options_construct2(createDBIfMissing, maxDataFileSize,
                                             memCleanupThresholdInBytes,
                                             ThrowOnError{})) {}

  Options(const Options& other)
      : m_opaque(jonoondb_options_copy_construct(other.m_opaque)) {}

  friend void swap(Options& first, Options& second) {
    using std::swap;
    swap(first.m_opaque, second.m_opaque);
  }

  Options(Options&& other) : m_opaque(nullptr) {
    swap(*this, other);
  }

  Options& operator=(const Options& other) {
    Options copy(other);
    swap(*this, copy);
    return *this;
  }

  Options& operator=(Options&& other) {
    swap(*this, other);
    return *this;
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
    return jonoondb_options_getcreatedbifmissing(m_opaque);
  }

  void SetMaxDataFileSize(size_t value) {
    jonoondb_options_setmaxdatafilesize(m_opaque, value);
  }

  size_t GetMaxDataFileSize() const {
    return jonoondb_options_getmaxdatafilesize(m_opaque);
  }

  void SetMemoryCleanupThreshold(std::size_t valueInBytes) {
    jonoondb_options_setmemorycleanupthreshold(m_opaque, valueInBytes);
  }

  std::size_t GetMemoryCleanupThreshold() {
    return jonoondb_options_getmemorycleanupthreshold(m_opaque);
  }

  const options_ptr GetOpaquePtr() const {
    return m_opaque;
  }

 private:
  options_ptr m_opaque;
};

//
// WriteOptions
//
class WriteOptions {
 public:
  // Default constructor that sets all the options to their default value
  WriteOptions() : m_opaque(jonoondb_write_options_construct()) {}

  WriteOptions(bool compress, bool verifyDocuments)
      : m_opaque(jonoondb_write_options_construct2(compress, verifyDocuments)) {
  }

  WriteOptions(const WriteOptions& other)
      : m_opaque(jonoondb_write_options_copy_construct(other.m_opaque)) {}

  friend void swap(WriteOptions& first, WriteOptions& second) {
    using std::swap;
    swap(first.m_opaque, second.m_opaque);
  }

  WriteOptions(WriteOptions&& other) : m_opaque(nullptr) {
    swap(*this, other);
  }

  WriteOptions& operator=(const WriteOptions& other) {
    WriteOptions copy(other);
    swap(*this, copy);
    return *this;
  }

  WriteOptions& operator=(WriteOptions&& other) {
    swap(*this, other);
    return *this;
  }

  ~WriteOptions() {
    if (m_opaque != nullptr) {
      jonoondb_write_options_destruct(m_opaque);
    }
  }

  void Compress(bool value) {
    jonoondb_write_options_set_compress(m_opaque, value);
  }

  bool Compress() const {
    return jonoondb_write_options_get_compress(m_opaque);
  }

  void VerifyDocuments(bool value) {
    jonoondb_write_options_set_verify_documents(m_opaque, value);
  }

  bool VerifyDocuments() const {
    return jonoondb_write_options_get_verify_documents(m_opaque);
  }

  const write_options_ptr GetOpaquePtr() const {
    return m_opaque;
  }

 private:
  write_options_ptr m_opaque;
};

//
// IndexInfo
//
class IndexInfo {
 public:
  IndexInfo() {
    m_opaque = jonoondb_indexinfo_construct();
  }

  IndexInfo(const std::string& indexName, IndexType type,
            const std::string& columnName, bool isAscending) {
    m_opaque = jonoondb_indexinfo_construct2(
        indexName.c_str(), static_cast<int32_t>(type), columnName.c_str(),
        isAscending, ThrowOnError{});
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
  Buffer() : m_opaque(jonoondb_buffer_construct()) {}

  Buffer(size_t bufferCapacityInBytes)
      : m_opaque(jonoondb_buffer_construct2(bufferCapacityInBytes,
                                            ThrowOnError{})) {}

  Buffer(const char* buffer, std::size_t bufferLengthInBytes)
      : m_opaque(jonoondb_buffer_construct3(
            buffer, bufferLengthInBytes, bufferLengthInBytes, ThrowOnError{})) {
  }

  Buffer(const char* buffer, std::size_t bufferLengthInBytes,
         std::size_t bufferCapacityInBytes)
      : m_opaque(jonoondb_buffer_construct3(buffer, bufferLengthInBytes,
                                            bufferCapacityInBytes,
                                            ThrowOnError{})) {}

  Buffer(char* buffer, std::size_t bufferLengthInBytes,
         std::size_t bufferCapacityInBytes, void (*customDeleterFunc)(char*))
      : m_opaque(jonoondb_buffer_construct4(
            buffer, bufferLengthInBytes, bufferCapacityInBytes,
            customDeleterFunc, ThrowOnError{})) {}

  Buffer(const Buffer& other)
      : m_opaque(
            jonoondb_buffer_copy_construct(other.m_opaque, ThrowOnError{})) {}

  Buffer(Buffer&& other) {
    m_opaque = other.m_opaque;
    other.m_opaque = jonoondb_buffer_construct();
  }

  Buffer& operator=(const Buffer& other) {
    jonoondb_buffer_copy_assignment(m_opaque, other.m_opaque, ThrowOnError{});
    return *this;
  }

  Buffer& operator=(Buffer&& other) {
    std::swap(m_opaque, other.m_opaque);
    return *this;
  }

  ~Buffer() {
    jonoondb_buffer_destruct(m_opaque);
  }

  bool operator<(const Buffer& other) const {
    return (jonoondb_buffer_op_lessthan(m_opaque, other.m_opaque) != 0);
  }

  bool operator<=(const Buffer& other) const {
    return (jonoondb_buffer_op_lessthanorequal(m_opaque, other.m_opaque) != 0);
  }

  bool operator>(const Buffer& other) const {
    return (jonoondb_buffer_op_greaterthan(m_opaque, other.m_opaque) != 0);
  }

  bool operator>=(const Buffer& other) const {
    return (jonoondb_buffer_op_greaterthanorequal(m_opaque, other.m_opaque) !=
            0);
  }

  bool operator==(const Buffer& other) {
    return (jonoondb_buffer_op_equal(m_opaque, other.m_opaque) != 0);
  }

  bool operator!=(const Buffer& other) {
    return (jonoondb_buffer_op_notequal(m_opaque, other.m_opaque) != 0);
  }

  void Copy(const char* srcBuffer, size_t bytesToCopy) {
    jonoondb_buffer_copy(m_opaque, srcBuffer, bytesToCopy, ThrowOnError{});
  }

  void Resize(size_t newBufferCapacityInBytes) {
    jonoondb_buffer_resize(m_opaque, newBufferCapacityInBytes, ThrowOnError{});
  }

  const char* GetData() const {
    return jonoondb_buffer_getdata(m_opaque);
  }

  const size_t GetLength() const {
    return jonoondb_buffer_getlength(m_opaque);
  }

  const size_t GetCapacity() const {
    return jonoondb_buffer_getcapacity(m_opaque);
  }

  jonoondb_buffer_ptr GetOpaqueType() const {
    return m_opaque;
  }

 private:
  jonoondb_buffer_ptr m_opaque;
};

class ResultSet {
 public:
  ResultSet(resultset_ptr opaque) : m_opaque(opaque) {}

  ResultSet(const ResultSet& other) = delete;
  ResultSet(ResultSet&& other) {
    if (this != &other) {
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

  void Close() {
    if (m_opaque != nullptr) {
      jonoondb_resultset_destruct(m_opaque);
      m_opaque = nullptr;
    }
  }

  bool Next() {
    return jonoondb_resultset_next(m_opaque) != 0;
  }

  std::int64_t GetInteger(std::int32_t columnIndex) const {
    return jonoondb_resultset_getinteger(m_opaque, columnIndex, ThrowOnError());
  }

  double GetDouble(std::int32_t columnIndex) const {
    return jonoondb_resultset_getdouble(m_opaque, columnIndex, ThrowOnError());
  }

  StringView GetString(std::int32_t columnIndex) {
    std::uint64_t size;
    std::uint64_t* sizePtr = &size;
    const char* str = jonoondb_resultset_getstring(m_opaque, columnIndex,
                                                   &sizePtr, ThrowOnError{});
    return StringView(str, size);
  }

  const Buffer& GetBlob(std::int32_t columnIndex) {
    std::uint64_t size;
    std::uint64_t* sizePtr = &size;
    auto blob = jonoondb_resultset_getblob(m_opaque, columnIndex, &sizePtr,
                                           ThrowOnError{});
    // Todo: Optimize Buffer usage
    m_tmpStorage = Buffer(blob, size, size);

    return m_tmpStorage;
  }

  std::int32_t GetColumnIndex(std::string columnLabel) {
    return jonoondb_resultset_getcolumnindex(
        m_opaque, columnLabel.c_str(), columnLabel.size(), ThrowOnError{});
  }

  std::int32_t GetColumnCount() {
    return jonoondb_resultset_getcolumncount(m_opaque);
  }

  SqlType GetColumnType(std::int32_t columnIndex) {
    return static_cast<SqlType>(jonoondb_resultset_getcolumntype(
        m_opaque, columnIndex, ThrowOnError{}));
  }

  StringView GetColumnLabel(std::int32_t columnIndex) {
    std::uint64_t size;
    std::uint64_t* sizePtr = &size;
    const char* str = jonoondb_resultset_getcolumnlabel(
        m_opaque, columnIndex, &sizePtr, ThrowOnError{});
    return StringView(str, size);
  }

  bool IsNull(std::int32_t columnIndex) {
    if (jonoondb_resultset_isnull(m_opaque, columnIndex, ThrowOnError{}) == 0) {
      return false;
    }

    return true;
  }

 private:
  resultset_ptr m_opaque;
  Buffer m_tmpStorage;
};

class Database {
 public:
  // This is a delegating ctor that uses default db options
  Database(const std::string& dbPath, const std::string& dbName)
      : Database(dbPath, dbName, Options()) {}

  Database(const std::string& dbPath, const std::string& dbName,
           const Options& opt)
      : m_opaque(jonoondb_database_construct(dbPath.c_str(), dbName.c_str(),
                                             opt.GetOpaquePtr(),
                                             ThrowOnError{})) {}

  ~Database() {
    jonoondb_database_destruct(m_opaque);
  }

  void CreateCollection(const std::string& name, SchemaType schemaType,
                        const std::string& schema,
                        const std::vector<IndexInfo>& indexes) {
    std::vector<indexinfo_ptr> vec;
    for (auto& item : indexes) {
      vec.push_back(item.GetOpaqueType());
    }

    jonoondb_database_createcollection(
        m_opaque, name.c_str(), static_cast<int32_t>(schemaType),
        schema.c_str(), schema.size(), vec.data(), vec.size(), ThrowOnError{});
  }

  void Insert(const std::string& collectionName, const Buffer& documentData,
              const WriteOptions& wo = WriteOptions()) {
    jonoondb_database_insert(m_opaque, collectionName.c_str(),
                             documentData.GetOpaqueType(), wo.GetOpaquePtr(),
                             ThrowOnError{});
  }

  void MultiInsert(const std::string& collectionName,
                   const std::vector<Buffer>& documents,
                   const WriteOptions& wo = WriteOptions()) {
    static_assert(sizeof(Buffer) == sizeof(jonoondb_buffer_ptr),
                  "Critical Error. Size assumptions not correct for Buffer & "
                  "jonoondb_buffer_ptr.");
    jonoondb_database_multi_insert(
        m_opaque, collectionName.data(), collectionName.size(),
        reinterpret_cast<const jonoondb_buffer_ptr*>(documents.data()),
        documents.size(), wo.GetOpaquePtr(), ThrowOnError{});
  }

  ResultSet ExecuteSelect(const std::string& selectStatement) {
    auto rs =
        jonoondb_database_executeselect(m_opaque, selectStatement.c_str(),
                                        selectStatement.size(), ThrowOnError{});
    return ResultSet(rs);
  }

  int64_t Delete(const std::string& deleteStatement) {
    auto deletedCnt =
        jonoondb_database_delete(m_opaque, deleteStatement.c_str(),
                                 deleteStatement.size(), ThrowOnError{});
    return deletedCnt;
  }

 private:
  database_ptr m_opaque;
};

}  // namespace jonoondb_api
