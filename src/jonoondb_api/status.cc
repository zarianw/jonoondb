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
}

Status::Status() {
  m_statusData = nullptr;
}

Status::Status(const Status& other) {
  if (this != &other) {
    m_statusData = nullptr;

    if (other.m_statusData) {
      size_t size;
      memcpy(&size, other.m_statusData, sizeof(size_t));
      m_statusData = new char[size];

      memcpy(m_statusData, other.m_statusData, size);
    }
  }
}

Status::Status(Status&& other) {
  if (this != &other) {
    m_statusData = other.m_statusData;

    other.m_statusData = nullptr;
  }
}

Status::Status(const char code, const char* message,
               const size_t messageLength) {
  size_t sizetLengthInBytes = sizeof(size_t);
  size_t size = sizetLengthInBytes + 1 + messageLength + 1;  // StatusDataSize + Code + Message + NullCharacter
  m_statusData = new char[size];

  memcpy(m_statusData, &size, sizetLengthInBytes);
  memcpy(m_statusData + sizetLengthInBytes, &code, 1);
  memcpy(m_statusData + sizetLengthInBytes + 1, message, messageLength + 1);
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
      size_t size;
      memcpy(&size, other.m_statusData, sizeof(size_t));
      m_statusData = new char[size];

      memcpy(m_statusData, other.m_statusData, size);
    }
  }

  return *this;
}

bool Status::operator!() {
  return !OK();
}

bool Status::OK() const {
  if (m_statusData == nullptr) {
    return true;
  }

  return false;
}

bool Status::InvalidArgument() const {
  if (m_statusData != nullptr) {
    // Read Code
    if (!memcmp(m_statusData + sizeof(size_t), &kStatusInvalidArgumentCode,
                1)) {
      return true;
    }
  }

  return false;
}

bool Status::MissingDatabaseFile() const {
  if (m_statusData != nullptr) {
    // Read Code
    if (!memcmp(m_statusData + sizeof(size_t), &kStatusMissingDatabaseFileCode,
                1)) {
      return true;
    }
  }

  return false;
}

bool Status::MissingDatabaseFolder() const {
  if (m_statusData != nullptr) {
    // Read Code
    if (!memcmp(m_statusData + sizeof(size_t),
                &kStatusMissingDatabaseFolderCode, 1)) {
      return true;
    }
  }

  return false;
}

bool Status::FailedToOpenMetadataDatabaseFile() const {
  if (m_statusData != nullptr) {
    // Read Code
    if (!memcmp(m_statusData + sizeof(size_t),
                &kStatusFailedToOpenMetadataDatabaseFileCode, 1)) {
      return true;
    }
  }

  return false;
}

bool Status::OutOfMemoryError() const {
  if (m_statusData != nullptr) {
    // Read Code
    if (!memcmp(m_statusData + sizeof(size_t), &kStatusOutOfMemoryErrorCode,
                1)) {
      return true;
    }
  }

  return false;
}

bool Status::DuplicateKeyError() const {
  if (m_statusData != nullptr) {
    // Read Code
    if (!memcmp(m_statusData + sizeof(size_t), &kStatusDuplicateKeyErrorCode,
                1)) {
      return true;
    }
  }

  return false;
}

bool Status::GenericError() const {
  if (m_statusData != nullptr) {
    // Read Code
    if (!memcmp(m_statusData + sizeof(size_t), &kStatusGenericErrorCode, 1)) {
      return true;
    }
  }

  return false;
}

bool Status::KeyNotFound() const {
  if (m_statusData != nullptr) {
    // Read Code
    if (!memcmp(m_statusData + sizeof(size_t), &kStatusKeyNotFoundCode, 1)) {
      return true;
    }
  }

  return false;
}

bool Status::FileIOError() const {
  if (m_statusData != nullptr) {
    // Read Code
    if (!memcmp(m_statusData + sizeof(size_t), &kStatusFileIOErrorCode, 1)) {
      return true;
    }
  }

  return false;
}

bool Status::APIMisuseError() const {
  if (m_statusData != nullptr) {
    // Read Code
    if (!memcmp(m_statusData + sizeof(size_t), &kStatusAPIMisuseErrorCode, 1)) {
      return true;
    }
  }

  return false;
}

bool Status::CollectionAlreadyExist() const {
  if (m_statusData != nullptr) {
    // Read Code
    if (!memcmp(m_statusData + sizeof(size_t),
                &kStatusCollectionAlreadyExistCode, 1)) {
      return true;
    }
  }

  return false;
}

bool Status::IndexAlreadyExist() const {
  if (m_statusData != nullptr) {
    // Read Code
    if (!memcmp(m_statusData + sizeof(size_t), &kStatusIndexAlreadyExistCode,
                1)) {
      return true;
    }
  }

  return false;
}

bool Status::CollectionNotFound() const {
  if (m_statusData != nullptr) {
    // Read Code
    if (!memcmp(m_statusData + sizeof(size_t), &kStatusCollectionNotFoundCode,
                1)) {
      return true;
    }
  }

  return false;
}

bool Status::SchemaParseError() const {
  if (m_statusData != nullptr) {
    // Read Code
    if (!memcmp(m_statusData + sizeof(size_t), &kStatusSchemaParseErrorCode,
                1)) {
      return true;
    }
  }

  return false;
}

bool Status::IndexOutOfBound() const {
  if (m_statusData != nullptr) {
    // Read Code
    if (!memcmp(m_statusData + sizeof(size_t), &kStatusIndexOutOfBoundErrorCode,
                1)) {
      return true;
    }
  }

  return false;
}

const char* Status::c_str() const {
  if (m_statusData == nullptr) {
    static const string OKStr = "OK";
    return OKStr.c_str();
  }

  return &m_statusData[sizeof(size_t) + 1];
}
