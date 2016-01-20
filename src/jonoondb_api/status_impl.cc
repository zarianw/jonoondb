#include <string>
#include <cstring>
#include "status_impl.h"

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
    const char* fnName, size_t lNum) : code(cd), message(msg),
    fileName(flName), funcName(fnName), lineNum(lNum) { }

  std::size_t code;
  std::string message;
  std::string fileName;
  std::string funcName;
  size_t lineNum;  
};
}

StatusImpl::StatusImpl() : m_statusData (nullptr) {
}

StatusImpl::StatusImpl(const StatusImpl& other) {
  if (this != &other) {
    m_statusData = nullptr;

    if (other.m_statusData) {
      m_statusData = new StatusData(other.GetCode(), other.GetMessage(),
        other.GetSourceFileName(), other.GetFunctionName(), other.GetLineNumber());
    }
  }
}

StatusImpl::StatusImpl(StatusImpl&& other) {
  if (this != &other) {
    m_statusData = other.m_statusData;

    other.m_statusData = nullptr;
  }
}

StatusImpl::StatusImpl(std::size_t code, const char* message, const char* srcFileName,
  const char* funcName, std::size_t lineNum) {
  m_statusData = new StatusData(code, message, srcFileName, funcName, lineNum);  
}

StatusImpl::~StatusImpl() {
  delete m_statusData;
  m_statusData = nullptr;
}

StatusImpl& StatusImpl::operator=(StatusImpl&& other) {
  if (this != &other) {
    m_statusData = other.m_statusData;
    other.m_statusData = nullptr;
  }

  return *this;
}

StatusImpl& StatusImpl::operator=(const StatusImpl& other) {
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

bool StatusImpl::operator!() {
  return !OK();
}

std::size_t StatusImpl::GetCode() const {
  if (m_statusData == nullptr) {
    return 0;
  }

  return m_statusData->code;
}

const char* StatusImpl::GetMessage() const {
  if (m_statusData == nullptr) {    
    return OKStr.c_str();
  }

  return m_statusData->message.c_str();
}

const char* StatusImpl::GetSourceFileName() const {
  if (m_statusData == nullptr) {
    return "";
  }

  return m_statusData->fileName.c_str();
}

const char* StatusImpl::GetFunctionName() const {
  if (m_statusData == nullptr) {
    return "";
  }

  return m_statusData->funcName.c_str();
}

std::size_t StatusImpl::GetLineNumber() const {
  if (m_statusData == nullptr) {
    return 0;
  }

  return m_statusData->lineNum;
}

bool StatusImpl::OK() const {
  if (m_statusData == nullptr) {
    return true;
  }

  return false;
}

bool StatusImpl::InvalidArgument() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusInvalidArgumentCode) {
    return true;
  }

  return false;
}

bool StatusImpl::MissingDatabaseFile() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusMissingDatabaseFileCode) {
    return true;
  }

  return false;
}

bool StatusImpl::MissingDatabaseFolder() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusMissingDatabaseFolderCode) {
    return true;
  }

  return false;
}

bool StatusImpl::FailedToOpenMetadataDatabaseFile() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusFailedToOpenMetadataDatabaseFileCode) {
    return true;
  }

  return false;
}

bool StatusImpl::OutOfMemoryError() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusOutOfMemoryErrorCode) {
    return true;
  }

  return false;
}

bool StatusImpl::DuplicateKeyError() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusDuplicateKeyErrorCode) {
    return true;
  }

  return false;
}

bool StatusImpl::GenericError() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusGenericErrorCode) {
    return true;
  }

  return false;
}

bool StatusImpl::KeyNotFound() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusKeyNotFoundCode) {
    return true;
  }

  return false;
}

bool StatusImpl::FileIOError() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusFileIOErrorCode) {
    return true;
  }

  return false;
}

bool StatusImpl::APIMisuseError() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusAPIMisuseErrorCode) {
    return true;
  }

  return false;
}

bool StatusImpl::CollectionAlreadyExist() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusCollectionAlreadyExistCode) {
    return true;
  }

  return false;
}

bool StatusImpl::IndexAlreadyExist() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusIndexAlreadyExistCode) {
    return true;
  }

  return false;
}

bool StatusImpl::CollectionNotFound() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusCollectionNotFoundCode) {
    return true;
  }

  return false;
}

bool StatusImpl::SchemaParseError() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusSchemaParseErrorCode) {
    return true;
  }

  return false;
}

bool StatusImpl::IndexOutOfBound() const {
  if (m_statusData != nullptr &&
    m_statusData->code == kStatusIndexOutOfBoundErrorCode) {   
      return true;    
  }

  return false;
}
