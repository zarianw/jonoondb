#pragma once

#include <cstddef>
#include "jonoondb_api_export.h"

namespace jonoondb_api {
// Forward declarations
struct StatusData;

class JONOONDB_API_EXPORT StatusImpl {
 public:
  StatusImpl();
  StatusImpl(const StatusImpl& other);
  StatusImpl(StatusImpl&& other);
  StatusImpl(std::size_t code, const char* message, const char* srcFileName,
             const char* funcName, std::size_t lineNum);
  ~StatusImpl();
  StatusImpl& operator=(StatusImpl&& other);
  StatusImpl& operator=(const StatusImpl& other);
  bool operator!();
  std::size_t GetCode() const;
  const char* GetMessage() const;
  const char* GetSourceFileName() const;
  const char* GetFunctionName() const;
  std::size_t GetLineNumber() const;
  bool OK() const;
  bool GenericError() const;
  bool InvalidArgument() const;
  bool MissingDatabaseFile() const;
  bool MissingDatabaseFolder() const;
  bool FailedToOpenMetadataDatabaseFile() const;
  bool OutOfMemoryError() const;
  bool DuplicateKeyError() const;
  bool KeyNotFound() const;
  bool FileIOError() const;
  bool APIMisuseError() const;
  bool CollectionAlreadyExist() const;
  bool IndexAlreadyExist() const;
  bool CollectionNotFound() const;
  bool SchemaParseError() const;
  bool IndexOutOfBound() const;

 private:
  StatusData* m_statusData;
};

JONOONDB_API_EXPORT extern const char kStatusGenericErrorCode;
JONOONDB_API_EXPORT extern const char kStatusInvalidArgumentCode;
JONOONDB_API_EXPORT extern const char kStatusMissingDatabaseFileCode;
JONOONDB_API_EXPORT extern const char kStatusMissingDatabaseFolderCode;
JONOONDB_API_EXPORT extern const char kStatusFailedToOpenMetadataDatabaseFileCode;
JONOONDB_API_EXPORT extern const char kStatusOutOfMemoryErrorCode;
JONOONDB_API_EXPORT extern const char kStatusDuplicateKeyErrorCode;
JONOONDB_API_EXPORT extern const char kStatusDataFileMissingCode;
JONOONDB_API_EXPORT extern const char kStatusDataFileInfoMissingCode;
JONOONDB_API_EXPORT extern const char kStatusInvalidOperationCode;
JONOONDB_API_EXPORT extern const char kStatusInvalidIteratorCode;
JONOONDB_API_EXPORT extern const char kStatusFileIOErrorCode;
JONOONDB_API_EXPORT extern const char kStatusAPIMisuseErrorCode;
JONOONDB_API_EXPORT extern const char kStatusKeyNotFoundCode;
JONOONDB_API_EXPORT extern const char kStatusCollectionAlreadyExistCode;
JONOONDB_API_EXPORT extern const char kStatusIndexAlreadyExistCode;
JONOONDB_API_EXPORT extern const char kStatusCollectionNotFoundCode;
JONOONDB_API_EXPORT extern const char kStatusSchemaParseErrorCode;
JONOONDB_API_EXPORT extern const char kStatusIndexOutOfBoundErrorCode;

JONOONDB_API_EXPORT extern const char kStatusSQLiteErrorCode;
}  // jonoon_api
