#pragma once

#include <memory>
#include <string>
#include <boost/interprocess/file_mapping.hpp> // NOLINT
#include <boost/interprocess/mapped_region.hpp> // NOLINT
#include "status.h"

namespace jonoondb_api {
// Forward declarations
class Status;

enum MemoryMappedFileMode {
  ReadOnly,
  ReadWrite
};

class MemoryMappedFile {
 public:
  static Status Open(const char* fileName, MemoryMappedFileMode mode, size_t writeOffset, bool asynchronous,
    MemoryMappedFile** memoryMappedFile) {
    try {
      *memoryMappedFile = new MemoryMappedFile(fileName, mode, writeOffset, asynchronous);
    }
    catch (boost::interprocess::interprocess_exception& ex) {
      return Status(kStatusFileIOErrorCode, ex.what(), strlen(ex.what()));
    }
    catch (std::exception& ex) {
      return Status(kStatusFileIOErrorCode, ex.what(), strlen(ex.what()));
    }
    catch (...) {
      std::string errorMessage = "Unexpected error occured while opening mmap file.";
      return Status(kStatusGenericErrorCode, errorMessage.c_str(), errorMessage.length());
    }

    return Status();
  }

  void* GetBaseAddress() {
    return m_mappedRegion.get_address();
  }

  char* GetOffsetAddressAsCharPtr(size_t offset) {
    auto offsetAddress = reinterpret_cast<char*>(GetBaseAddress());
    offsetAddress += offset;
    return offsetAddress;
  }

  size_t GetCurrentWriteOffset() {
    return m_currentWriteOffset;
  }

  void SetCurrentWriteOffset(size_t offset) {
    m_currentWriteOffset = offset;
    m_currentWritePosition = reinterpret_cast<char*>(GetBaseAddress());
    m_currentWritePosition += m_currentWriteOffset;
  }

  /*Status Write(const void* data, const size_t length, const size_t offset)
  {
    try {
      char* destinationAddress = reinterpret_cast<char *>(GetBaseAddress());
      destinationAddress += offset;
      memcpy(destinationAddress, data, length);
    }
    catch (boost::interprocess::interprocess_exception& ex) {
      return Status(kStatusFileIOErrorCode, ex.what(), strlen(ex.what()));
    }
    catch (std::exception& ex) {
      return Status(kStatusFileIOErrorCode, ex.what(), strlen(ex.what()));
    }
    catch (...) {
      std::string errorMessage = "Unexpected error occured while opening mmap file.";
      return Status(kStatusGenericErrorCode, errorMessage.c_str(), errorMessage.length());
    }
  }*/

  Status WriteAtCurrentPosition(const void* data, const size_t length) {
    try {
      memcpy(m_currentWritePosition, data, length);
      m_currentWritePosition += length;
      m_currentWriteOffset += length;
    }
    catch (boost::interprocess::interprocess_exception& ex) {
      return Status(kStatusFileIOErrorCode, ex.what(), strlen(ex.what()));
    }
    catch (std::exception& ex) {
      return Status(kStatusFileIOErrorCode, ex.what(), strlen(ex.what()));
    }
    catch (...) {
      std::string errorMessage = "Unexpected error occured while opening memory mapped file.";
      return Status(kStatusGenericErrorCode, errorMessage.c_str(), errorMessage.length());
    }

    return Status();
  }

  Status Flush(size_t offset, size_t numBytes) {
    if (!m_mappedRegion.flush(offset, numBytes, m_asynchronous)) {
      std::string errorMessage = "Unexpected error occured while flushing memory mapped file.";
      return Status(kStatusFileIOErrorCode, errorMessage.c_str(), errorMessage.length());
    }

    return Status();
  }

 private:
  MemoryMappedFile(const char* fileName, MemoryMappedFileMode mode, size_t writeOffset, bool asynchronous) :
    m_currentWritePosition(nullptr), m_currentWriteOffset(0), m_asynchronous(asynchronous) {
    auto internalMode = GetInternalMode(mode);
    m_fileMapping = boost::interprocess::file_mapping(fileName, internalMode);
    m_mappedRegion = boost::interprocess::mapped_region(m_fileMapping, internalMode);
    if (mode == MemoryMappedFileMode::ReadWrite) {
      m_currentWriteOffset = writeOffset;
      m_currentWritePosition = reinterpret_cast<char*>(GetBaseAddress());
      m_currentWritePosition += m_currentWriteOffset;
    }
  }

  boost::interprocess::mode_t GetInternalMode(MemoryMappedFileMode mode) {
    switch (mode) {
    case MemoryMappedFileMode::ReadOnly:
      return boost::interprocess::read_only;
    case MemoryMappedFileMode::ReadWrite:
      return boost::interprocess::read_write;
    default:
      char message[100];
      sprintf(message, "mode value %d is not supported for memory mapped files.", mode);
      // Normally throwing exceptions is not the error handling mechanism but in this class we are using boost
      // which throws exceptions for error handling. Throwing exception in this one function is ok because the callee
      // upstream is catching the exception.
      throw std::invalid_argument(message);
      break;
    }
  }

  boost::interprocess::file_mapping m_fileMapping;
  boost::interprocess::mapped_region m_mappedRegion;
  char* m_currentWritePosition;
  size_t m_currentWriteOffset;
  bool m_asynchronous;
};
}  // namespace jonoondb_api
