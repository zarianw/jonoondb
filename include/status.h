#pragma once

#include <cstddef>

namespace jonoondb_api {
class Status {
 public:
  Status();
  Status(const Status& other);
  Status(Status&& other);
  Status(const char code, const char* message, const std::size_t messageLength);
  ~Status();
  Status& operator=(Status&& other);
  Status& operator=(const Status& other);
  bool operator!();
  const char* c_str() const;
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
  //class StatusImpl;
  //std::unique_ptr<StatusImpl> m_statusImpl;
  //Byte 1 to 4: Size of m_statusData
  //Byte 5: Code
  //Byte 6 onwards: Null terminated message string
  char* m_statusData;
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
