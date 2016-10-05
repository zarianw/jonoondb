#pragma once

#include <cstddef>
#include "jonoondb_api_export.h"

namespace jonoondb_api {
// Forward declarations
struct StatusData;

class StatusImpl {
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

extern const char kStatusGenericErrorCode;
extern const char kStatusInvalidArgumentCode;
extern const char kStatusMissingDatabaseFileCode;
extern const char kStatusMissingDatabaseFolderCode;
extern const char kStatusFailedToOpenMetadataDatabaseFileCode;
extern const char kStatusOutOfMemoryErrorCode;
extern const char kStatusDuplicateKeyErrorCode;
extern const char kStatusDataFileMissingCode;
extern const char kStatusDataFileInfoMissingCode;
extern const char kStatusInvalidOperationCode;
extern const char kStatusInvalidIteratorCode;
extern const char kStatusFileIOErrorCode;
extern const char kStatusAPIMisuseErrorCode;
extern const char kStatusKeyNotFoundCode;
extern const char kStatusCollectionAlreadyExistCode;
extern const char kStatusIndexAlreadyExistCode;
extern const char kStatusCollectionNotFoundCode;
extern const char kStatusSchemaParseErrorCode;
extern const char kStatusIndexOutOfBoundErrorCode;

extern const char kStatusSQLiteErrorCode;
}  // jonoon_api
