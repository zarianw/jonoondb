#pragma once

#include <memory>
#include <string>
#include <sstream>
#include <cstdint>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include "jonoondb_exceptions.h"

namespace jonoondb_api {
enum class MemoryMappedFileMode: std::int32_t {
  ReadOnly = 1,
  ReadWrite = 2
};

class MemoryMappedFile final {
 public:
  MemoryMappedFile(const std::string& fileName, MemoryMappedFileMode mode,
                   std::size_t writeOffset, bool asynchronous)
      : m_currentWritePosition(nullptr),
        m_currentWriteOffset(0),
        m_asynchronous(asynchronous) {
    m_fileName = fileName;
    m_pageSize = boost::interprocess::mapped_region::get_page_size();
    auto internalMode = GetInternalMode(mode);
    m_fileMapping =
        boost::interprocess::file_mapping(fileName.c_str(), internalMode);
    m_mappedRegion = boost::interprocess::mapped_region(m_fileMapping,
                                                        internalMode);

    if (mode == MemoryMappedFileMode::ReadWrite) {
      m_currentWriteOffset = writeOffset;
      m_currentWritePosition = reinterpret_cast<char*>(GetBaseAddress());
      m_currentWritePosition += m_currentWriteOffset;
    }
  }

  MemoryMappedFile(const MemoryMappedFile&) = delete;
  MemoryMappedFile(MemoryMappedFile&&) = delete;
  MemoryMappedFile& operator=(const MemoryMappedFile&) = delete;
  MemoryMappedFile& operator=(MemoryMappedFile&&) = delete;

  const std::string& GetFileName() {
    return m_fileName;
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
   return Status(kStatusFileIOErrorCode, ex.what(), __FILE__, __func__, __LINE__);
   }
   catch (std::exception& ex) {
   return Status(kStatusFileIOErrorCode, ex.what(), __FILE__, __func__, __LINE__);
   }
   catch (...) {
   std::string errorMessage = "Unexpected error occured while opening mmap file.";
   return Status(kStatusGenericErrorCode, errorMessage.c_str(), __FILE__, __func__, __LINE__);
   }
   }*/

  void WriteAtCurrentPosition(const void* data, const size_t length) {
    memcpy(m_currentWritePosition, data, length);
    m_currentWritePosition += length;
    m_currentWriteOffset += length;
  }

  void Flush(size_t offset, size_t numBytes) {
    // On some OS (e.g. Linux) offset needs to be a multiple of a pagesize
    // The next 2 stmts should be optimized into a single div instructions
    auto quotient = offset / m_pageSize;
    auto remainder = offset % m_pageSize;
    offset = m_pageSize * quotient;
    numBytes += remainder;

    if (!m_mappedRegion.flush(offset, numBytes, m_asynchronous)) {
      throw FileIOException(
          "Unexpected error occured while flushing memory mapped file.",
          __FILE__,
          __func__,
          __LINE__);
    }
  }

 private:
  boost::interprocess::mode_t GetInternalMode(MemoryMappedFileMode mode) {
    switch (mode) {
      case MemoryMappedFileMode::ReadOnly:
        return boost::interprocess::read_only;
      case MemoryMappedFileMode::ReadWrite:
        return boost::interprocess::read_write;
      default:
        std::ostringstream ss;
        ss << "Mode value " << static_cast<int32_t>(mode)
            << "is not supported for memory mapped files.";
        throw InvalidArgumentException(ss.str(), __FILE__, __func__, __LINE__);
        break;
    }
  }

  boost::interprocess::file_mapping m_fileMapping;
  boost::interprocess::mapped_region m_mappedRegion;
  char* m_currentWritePosition;
  std::size_t m_currentWriteOffset;
  bool m_asynchronous;
  std::size_t m_pageSize;
  std::string m_fileName;
};
}  // namespace jonoondb_api
