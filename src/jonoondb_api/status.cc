#include <string>
#include <cstring>
#include "status.h"

using namespace std;

using namespace jonoondb_api;

namespace jonoondb_api {
const char kStatusGenericErrorCode = 1;
const char kStatusInvalidArgumentCode = 2;
const char kStatusMissingDatabaseFileCode = 3;
const char kStatusMissingDatabaseFolderCode = 4;
const char kStatusFailedToOpenMetadataDatabaseFileCode = 5;
const char kStatusOutOfMemoryErrorCode = 6;
const char kStatusDuplicateKeyErrorCode = 7;
const char kStatusDataFileMissingCode = 8;
const char kStatusDataFileInfoMissingCode = 9;
const char kStatusInvalidOperationCode = 10;
const char kStatusInvalidIteratorCode = 11;
const char kStatusFileIOErrorCode = 12;
const char kStatusAPIMisuseErrorCode = 13;
const char kStatusKeyNotFoundCode = 14;
const char kStatusCollectionAlreadyExistCode = 15;
const char kStatusIndexAlreadyExistCode = 16;
const char kStatusCollectionNotFoundCode = 17;
const char kStatusSchemaParseErrorCode = 18;
const char kStatusIndexOutOfBoundErrorCode = 19;

const char kStatusSQLiteErrorCode = 101;

static const std::string OKStr = "OK";

struct StatusData {
public:
  StatusData(size_t cd, const char* msg, const char* flName,
    const char* fnName, size_t lineNum) : code(cd), message(msg),
    fileName(flName), funcName(fnName) { }

  std::size_t code;
  std::string message;
  std::string fileName;
  std::string funcName;
  size_t lineNum;  
};
}

Status::Status() : m_statusData (nullptr) {
}

Status::Status(const Status& other) {
  if (this != &other) {
    m_statusData = nullptr;

    if (other.m_statusData) {
      m_statusData = new StatusData(other.GetCode(), other.GetMessage(),
        other.GetSourceFileName(), other.GetFunctionName(), other.GetLineNumber());
    }
  }
}

Status::Status(Status&& other) {
  if (this != &other) {
    m_statusData = other.m_statusData;

    other.m_statusData = nullptr;
  }
}

Status::Status(std::size_t code, const char* message, const char* srcFileName,
  const char* funcName, std::size_t lineNum) {
  m_statusData = new StatusData(code, message, srcFileName, funcName, lineNum);  
}

Status::~Status() {
  delete m_statusData;
  m_statusData = nullptr;
}

Status& Status::operator=(Status&& other) {
  if (this != &other) {
    m_statusData = other.m_statusData;
    other.m_statusData = nullptr;
  }

  return *this;
}

Status& Status::operator=(const Status& other) {
  if (this != &other) {
    delete m_statusData;
    m_statusData = nullptr;

    if (other.m_statusData) {      
      m_statusData = new StatusData(other.GetCode(), other.GetMessage(),
        other.GetSourceFileName(), other.GetFunctionName(), other.GetLineNumber());
    }
  }

  return *this;
}

bool Status::operator!() {
  return !OK();
}

std::size_t Status::GetCode() const {
  if (m_statusData == nullptr) {
    return 0;
  }

  return m_statusData->code;
}

const char* Status::GetMessage() const {
  if (m_statusData == nullptr) {    
    return OKStr.c_str();
  }

  return m_statusData->message.c_str();
}

const char* Status::GetSourceFileName() const {
  if (m_statusData == nullptr) {
    return "";
  }

  return m_statusData->fileName.c_str();
}

const char* Status::GetFunctionName() const {
  if (m_statusData == nullptr) {
    return "";
  }

  return m_statusData->funcName.c_str();
}

std::size_t Status::GetLineNumber() const {
  if (m_statusData == nullptr) {
    return 0;
  }

  return m_statusData->lineNum;
}

bool Status::OK() const {
  if (m_statusData == nullptr) {
    return true;
  }

  return false;
}

bool Status::InvalidArgument() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusInvalidArgumentCode) {
    return true;
  }

  return false;
}

bool Status::MissingDatabaseFile() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusMissingDatabaseFileCode) {
    return true;
  }

  return false;
}

bool Status::MissingDatabaseFolder() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusMissingDatabaseFolderCode) {
    return true;
  }

  return false;
}

bool Status::FailedToOpenMetadataDatabaseFile() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusFailedToOpenMetadataDatabaseFileCode) {
    return true;
  }

  return false;
}

bool Status::OutOfMemoryError() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusOutOfMemoryErrorCode) {
    return true;
  }

  return false;
}

bool Status::DuplicateKeyError() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusDuplicateKeyErrorCode) {
    return true;
  }

  return false;
}

bool Status::GenericError() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusGenericErrorCode) {
    return true;
  }

  return false;
}

bool Status::KeyNotFound() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusKeyNotFoundCode) {
    return true;
  }

  return false;
}

bool Status::FileIOError() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusFileIOErrorCode) {
    return true;
  }

  return false;
}

bool Status::APIMisuseError() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusAPIMisuseErrorCode) {
    return true;
  }

  return false;
}

bool Status::CollectionAlreadyExist() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusCollectionAlreadyExistCode) {
    return true;
  }

  return false;
}

bool Status::IndexAlreadyExist() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusIndexAlreadyExistCode) {
    return true;
  }

  return false;
}

bool Status::CollectionNotFound() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusCollectionNotFoundCode) {
    return true;
  }

  return false;
}

bool Status::SchemaParseError() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusSchemaParseErrorCode) {
    return true;
  }

  return false;
}

bool Status::IndexOutOfBound() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusIndexOutOfBoundErrorCode) {   
      return true;    
  }

  return false;
}
