#pragma once

namespace jonoondb_api
{
  class Status
  {
  public:
    Status();
    Status(const Status& other);
    Status(Status&& other);
    Status(const char code, const char* message, const size_t messageLength);
    ~Status();
    Status& operator=(Status&& other);
    Status& operator=(const Status& other);
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

    static const char GenericErrorCode = 1;
    static const char InvalidArgumentCode = 2;
    static const char MissingDatabaseFileCode = 3;
    static const char MissingDatabaseFolderCode = 4;
    static const char FailedToOpenMetadataDatabaseFileCode = 5;
    static const char OutOfMemoryErrorCode = 6;

    static const char DuplicateKeyErrorCode = 7;
    static const char DataFileMissingCode = 8;
    static const char DataFileInfoMissingCode = 9;

    static const char InvalidOperationCode = 10;
    static const char InvalidIteratorCode = 11;
    static const char FileIOErrorCode = 12;
    static const char APIMisuseErrorCode = 13;
    static const char KeyNotFoundCode = 14;

    static const char CollectionAlreadyExistCode = 15;
    static const char IndexAlreadyExistCode = 16;


    static const char SQLiteErrorCode = 101;
  private:
    //class StatusImpl;
    //std::unique_ptr<StatusImpl> m_statusImpl;			
    //Byte 1 to 4: Size of m_statusData
    //Byte 5: Code
    //Byte 6 onwards: Null terminated message string
    char* m_statusData;
  };
} // jonoon_api