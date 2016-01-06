#include <sstream>
#include "cdatabase.h"
#include "options_impl.h"
#include "database_impl.h"
#include "jonoondb_exceptions.h"
#include "index_info_impl.h"
#include "enums.h"
#include "buffer_impl.h"
#include "resultset_impl.h"
#include "boost/utility/string_ref.hpp"
#include "status_impl.h"

using namespace jonoondb_api;

//
// Error Handling
//
extern "C" {
//
// Status
//
struct status {
  status(std::size_t code, const char* message, const char* srcFileName,
    const char* funcName, std::size_t lineNum) :
    impl(code, message, srcFileName, funcName, lineNum) {
  }

  StatusImpl impl;
};

void jonoondb_status_destruct(status_ptr sts) {
  delete sts;
}

const char* jonoondb_status_message(status_ptr sts) {
  return sts->impl.GetMessage();
}

uint64_t jonoondb_status_code(status_ptr sts) {
  return sts->impl.GetCode();
}

const char* jonoondb_status_file(status_ptr sts) {
  return sts->impl.GetSourceFileName();
}

const char* jonoondb_status_function(status_ptr sts) {
  return sts->impl.GetFunctionName();
}

uint64_t jonoondb_status_line(status_ptr sts) {
  return sts->impl.GetLineNumber();
}
} // extern "C"

namespace jonoondb_api {
// Returns true if fn executed without throwing an error, false otherwise.
// If calling fn threw an error, capture it in *out_error.
template<typename Fn>
bool TranslateExceptions(Fn&& fn, status_ptr& sts) {
  bool retVal = false;
  try {
    fn();
    retVal = true;
  } catch (const InvalidArgumentException& ex) {
    sts = new status(jonoondb_status_codes::status_invalidargumentcode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const MissingDatabaseFileException& ex) {
    sts = new status(jonoondb_status_codes::status_missingdatabasefilecode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const MissingDatabaseFolderException& ex) {
    sts = new status(jonoondb_status_codes::status_missingdatabasefoldercode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const OutOfMemoryException& ex) {
    sts = new status(jonoondb_status_codes::status_outofmemoryerrorcode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const DuplicateKeyException& ex) {
    sts = new status(jonoondb_status_codes::status_duplicatekeyerrorcode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const CollectionAlreadyExistException& ex) {
    sts = new status(jonoondb_status_codes::status_collectionalreadyexistcode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const IndexAlreadyExistException& ex) {
    sts = new status(jonoondb_status_codes::status_indexalreadyexistcode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const CollectionNotFoundException& ex) {
    sts = new status(jonoondb_status_codes::status_collectionnotfoundcode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const SchemaParseException& ex) {
    sts = new status(jonoondb_status_codes::status_schemaparseerrorcode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const IndexOutOfBoundException& ex) {
    sts = new status(jonoondb_status_codes::status_indexoutofbounderrorcode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const SQLException& ex) {
    sts = new status(jonoondb_status_codes::status_sqlerrorcode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const FileIOException& ex) {
    sts = new status(jonoondb_status_codes::status_fileioerrorcode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const JonoonDBException& ex) {
    sts = new status(jonoondb_status_codes::status_genericerrorcode, ex.what(), ex.GetSourceFileName(),
      ex.GetFunctionName(), ex.GetLineNumber());
  } catch (const std::exception& ex) {
    sts = new status(jonoondb_status_codes::status_genericerrorcode, ex.what(), "", "", 0);
  } catch (...) {
    sts = new status(jonoondb_status_codes::status_genericerrorcode, "Unknown Error.", "", "", 0);
  }

  return retVal;
}

//
// Helper funcs
//
IndexType ToIndexType(std::int32_t type) {
  switch (static_cast<IndexType>(type)) {
    case IndexType::EWAHCompressedBitmap:
      return static_cast<IndexType>(type);
    default:
      throw InvalidArgumentException("Argument type is not valid. Allowed values are {EWAHCompressedBitmap = 1}.",
        __FILE__, "", __LINE__);
  }
}

SchemaType ToSchemaType(std::int32_t type) {
  switch (static_cast<SchemaType>(type)) {
    case SchemaType::FLAT_BUFFERS:
      return static_cast<SchemaType>(type);
    default:
      throw InvalidArgumentException("Argument type is not valid. Allowed values are {FLAT_BUFFERS = 1}.",
        __FILE__, "", __LINE__);
  }
}

} // jonoondb_api

extern "C" {
//
// Options
//
struct options {
  options() : impl() {}

  options(bool createDBIfMissing, int64_t maxDataFileSize,
    bool compressionEnabled, bool synchronous) :
    impl(createDBIfMissing, maxDataFileSize, compressionEnabled, synchronous) {
  }

  OptionsImpl impl;
};

options_ptr jonoondb_options_construct() {
  return new options();
}

options_ptr jonoondb_options_copy_construct(const options_ptr other) {
  return new options(*other);
}

options_ptr jonoondb_options_construct2(bool createDBIfMissing, uint64_t maxDataFileSize,
  bool compressionEnabled, bool synchronous, status_ptr* sts) {
  return new options(createDBIfMissing, maxDataFileSize, compressionEnabled, synchronous);
}

void jonoondb_options_destruct(options_ptr opt) {
  delete opt;
}

bool jonoondb_options_getcreatedbifmissing(options_ptr opt) {
  return opt->impl.GetCreateDBIfMissing();
}

void jonoondb_options_setcreatedbifmissing(options_ptr opt, bool value) {
  return opt->impl.SetCreateDBIfMissing(value);
}

bool jonoondb_options_getcompressionenabled(options_ptr opt) {
  return opt->impl.GetCompressionEnabled();
}

void jonoondb_options_setcompressionenabled(options_ptr opt, bool value) {
  return opt->impl.SetCompressionEnabled(value);
}

uint64_t jonoondb_options_getmaxdatafilesize(options_ptr opt) {
  return opt->impl.GetMaxDataFileSize();
}

void jonoondb_options_setmaxdatafilesize(options_ptr opt, uint64_t value) {
  return opt->impl.SetMaxDataFileSize(value);
}

bool jonoondb_options_getsynchronous(options_ptr opt) {
  return opt->impl.GetSynchronous();
}

void jonoondb_options_setsynchronous(options_ptr opt, bool value) {
  return opt->impl.SetSynchronous(value);
}

//
// IndexInfo
//
struct indexinfo {
  indexinfo() : impl() {
  }

  indexinfo(const char* indexName, IndexType type, const char* columnName, bool isAscending) {
  } 

  IndexInfoImpl impl;
};

indexinfo_ptr jonoondb_indexinfo_construct() {
  return new indexinfo();
}

indexinfo_ptr jonoondb_indexinfo_construct2(const char* indexName, int32_t type, const char* columnName,
  bool isAscending, status_ptr* sts) {
  indexinfo_ptr indexInfo = nullptr;
  TranslateExceptions([&]{
    indexInfo = new indexinfo(indexName, jonoondb_api::ToIndexType(type), columnName, isAscending);
  }, *sts);

  return indexInfo;
}

void jonoondb_indexinfo_destruct(indexinfo_ptr indexInfo) {
  delete indexInfo;
}

const char* jonoondb_indexinfo_getindexname(indexinfo_ptr indexInfo) {
  return indexInfo->impl.GetIndexName().c_str();
}

void jonoondb_indexinfo_setindexname(indexinfo_ptr indexInfo, const char* value) {
  indexInfo->impl.SetIndexName(value);
}

int32_t jonoondb_indexinfo_gettype(indexinfo_ptr indexInfo) {
  return static_cast<int32_t>(indexInfo->impl.GetType());
}

void jonoondb_indexinfo_settype(indexinfo_ptr indexInfo, int32_t value) {
  indexInfo->impl.SetType(ToIndexType(value));
}

const char* jonoondb_indexinfo_getcolumnname(indexinfo_ptr indexInfo) {
  return indexInfo->impl.GetColumnName().c_str();
}

void jonoondb_indexinfo_setcolumnname(indexinfo_ptr indexInfo, const char* value) {
  indexInfo->impl.SetColumnName(value);
}

void jonoondb_indexinfo_setisascending(indexinfo_ptr indexInfo, bool value) {
  indexInfo->impl.SetIsAscending(value);
}

bool jonoondb_indexinfo_getisascending(indexinfo_ptr indexInfo) {
  return indexInfo->impl.GetIsAscending();
}

//
// BufferImpl
//
struct jonoondb_buffer {
  jonoondb_buffer() {}
  jonoondb_buffer(const BufferImpl& other) : impl(other) {}

  BufferImpl impl;
};

jonoondb_buffer_ptr jonoondb_buffer_construct() {
  return new jonoondb_buffer();
}

void jonoondb_buffer_destruct(jonoondb_buffer_ptr buf) {
  delete buf;
}

jonoondb_buffer_ptr jonoondb_buffer_copy_construct(jonoondb_buffer_ptr buf, status_ptr* sts) {
  jonoondb_buffer_ptr retVal = nullptr;
  TranslateExceptions([&]{
    retVal = new jonoondb_buffer(buf->impl);
  }, *sts);

  return retVal;
}

void jonoondb_buffer_copy_assignment(jonoondb_buffer_ptr self, jonoondb_buffer_ptr other, status_ptr* sts) {
  TranslateExceptions([&]{
    self->impl = other->impl;
  }, *sts);  
}

int32_t jonoondb_buffer_op_lessthan(jonoondb_buffer_ptr self, jonoondb_buffer_ptr other) {
  return (self->impl < other->impl ? 1 : 0);
}

void jonoondb_buffer_copy(jonoondb_buffer_ptr buf, const char* srcBuf, uint64_t bytesToCopy, status_ptr* sts) {
  TranslateExceptions([&]{
    buf->impl.Copy(srcBuf, bytesToCopy);
  }, *sts);
}

void jonoondb_buffer_resize(jonoondb_buffer_ptr buf, uint64_t newBufferCapacityInBytes, status_ptr* sts) {
  TranslateExceptions([&]{
    buf->impl.Resize(newBufferCapacityInBytes);
  }, *sts);
}

const char* jonoondb_buffer_getdata(jonoondb_buffer_ptr buf) {
  return buf->impl.GetData();
}

uint64_t jonoondb_buffer_getlength(jonoondb_buffer_ptr buf) {
  return buf->impl.GetLength();
}

uint64_t jonoondb_buffer_getcapacity(jonoondb_buffer_ptr buf) {
  return buf->impl.GetCapacity();
}

//
// ResultSet Functions
//
struct resultset {
  resultset(ResultSetImpl&& val) : impl(std::move(val)) {
  }

  ResultSetImpl impl;
};

void jonoondb_resultset_destruct(resultset_ptr rs) {
  delete rs;
}

int32_t jonoondb_resultset_next(resultset_ptr rs) {
  if (rs->impl.Next()) {
    return 1;
  } else {
    return 0;
  }
}

int64_t jonoondb_resultset_getinteger(resultset_ptr rs, int32_t columnIndex, status_ptr* sts) {
  int64_t val;
  TranslateExceptions([&]{
    val = rs->impl.GetInteger(columnIndex);
  }, *sts);  
  return val;
}

double jonoondb_resultset_getdouble(resultset_ptr rs, int32_t columnIndex, status_ptr* sts) {
  double val;
  TranslateExceptions([&]{
    val = rs->impl.GetDouble(columnIndex);
  }, *sts);
  return val;
}

const char* jonoondb_resultset_getstring(resultset_ptr rs, int32_t columnIndex, uint64_t** retValSize, status_ptr* sts) {
  char* strPtr;
  TranslateExceptions([&]{
    const std::string& str = rs->impl.GetString(columnIndex);
    **retValSize = str.size();
    strPtr = const_cast<char*>(str.c_str());
  }, *sts);  
  return strPtr;
}

int32_t jonoondb_resultset_getcolumnindex(resultset_ptr rs, const char* columnLabel, uint64_t columnLabelLength, status_ptr* sts) {  
  int32_t val;
  TranslateExceptions([&]{
    boost::string_ref strRef(columnLabel, columnLabelLength);
    val = rs->impl.GetColumnIndex(strRef);
  }, *sts);
  return val;
}

//
// Database
//
struct database {
  database(const char* dbPath, const char* dbName, const OptionsImpl& opt) {
    impl = DatabaseImpl::Open(dbPath, dbName, opt);
  }

  ~database() {
    delete impl;
  }

  DatabaseImpl* impl;
};

database_ptr jonoondb_database_open(const char* dbPath, const char* dbName, const options_ptr opt, status_ptr* sts) {
  database_ptr db = nullptr;
  TranslateExceptions([&]{
    db = new database(dbPath, dbName, opt->impl);
  }, *sts);
  
  return db;
}

void jonoondb_database_close(database_ptr db, status_ptr* sts) {
  TranslateExceptions([&]{
    db->impl->Close();
    // Todo: Handle exceptions that can happen on close
    delete db;
  }, *sts);
}

void jonoondb_database_createcollection(database_ptr db, const char* name, int32_t schemaType, const char* schema,
                                        indexinfo_ptr* indexes, uint64_t indexesLength, status_ptr* sts) {
  TranslateExceptions([&]{
    // Todo: We should use array_view here from GSL. That can speedup this and will also be more clean.
    std::vector<IndexInfoImpl*> vec;
    for (uint64_t i = 0; i < indexesLength; i++) {
      vec.push_back(&indexes[i]->impl);
    }
    db->impl->CreateCollection(name, ToSchemaType(schemaType), schema, vec);
  }, *sts);
}

void jonoondb_database_insert(database_ptr db, const char* collectionName, const jonoondb_buffer_ptr documentData, status_ptr* sts) {
  TranslateExceptions([&]{
    db->impl->Insert(collectionName, documentData->impl);
  }, *sts);
}

resultset_ptr jonoondb_database_executeselect(database_ptr db, const char* selectStmt, uint64_t selectStmtLength, status_ptr* sts) {
  resultset_ptr val;
  // Todo: Use string_view for performance improvement
  TranslateExceptions([&]{
    std::string selectStatement(selectStmt, selectStmtLength);
    val = new resultset(db->impl->ExecuteSelect(selectStatement));
  }, *sts);

  return val;  
}
  
} // extern "C"
