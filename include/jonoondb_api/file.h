#pragma once

#include <string>
#include <sstream>
#include <fstream>
#include "jonoondb_exceptions.h"
#include "exception_utils.h"

#if defined(_WIN32)
#include <Windows.h>
#else
// Linux code goes here
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#endif

namespace jonoondb_api {
class File {
 public:
  static std::string Read(const std::string& path, bool isBinary = true) {
    std::ifstream ifs(path, isBinary ? std::ios::binary : std::ios::in);
    if (!ifs.is_open()) {
      std::string reason = ExceptionUtils::GetErrorTextFromErrorCode(ExceptionUtils::GetError());
      std::ostringstream ss;
      ss << "Failed to open file at path " << path << ". Reason: " << reason;
      throw FileIOException(ss.str(), __FILE__, __func__, __LINE__);
    }

    std::string fileContents;
    if (isBinary) {
      ifs.seekg(0, std::ios::end);
      fileContents.resize(static_cast<size_t>(ifs.tellg()));
      ifs.seekg(0, std::ios::beg);
      ifs.read(const_cast<char*>(fileContents.data()), fileContents.size());
    } else {
      std::ostringstream oss;
      oss << ifs.rdbuf();
      fileContents = oss.str();
    }

    if (ifs.bad()) {
      std::string reason = ExceptionUtils::GetErrorTextFromErrorCode(ExceptionUtils::GetError());
      std::ostringstream ss;
      ss << "Failed to read the file at path " << path << ". Reason: "
         << reason;
      throw FileIOException(ss.str(), __FILE__, __func__, __LINE__);
    }

    return fileContents;
  }

  static void FastAllocate(std::string& fileName, std::size_t fileSize) {
#if defined(_WIN32)
    //1. Create a new file. This will fail if the file already exist, which is what we want.
    //   We don't want to delete or overwrite any data
    auto fileHandle = CreateFile(fileName.c_str(),
        GENERIC_WRITE,
        NULL,//No Sharing
        NULL,
        CREATE_NEW,
        NULL,
        NULL);

    if (fileHandle == INVALID_HANDLE_VALUE) {
      std::string reason = ExceptionUtils::GetErrorTextFromErrorCode(ExceptionUtils::GetError());
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: " << reason;
      throw FileIOException(ss.str(), __FILE__, __func__, __LINE__);
    }

    //2. Move the file pointer to the desired position
    LARGE_INTEGER li;
    li.QuadPart = fileSize;

    if (SetFilePointer(fileHandle, li.LowPart, &li.HighPart, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
      std::string reason = ExceptionUtils::GetErrorTextFromErrorCode(ExceptionUtils::GetError());
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: " << reason;
      throw FileIOException(ss.str(), __FILE__, __func__, __LINE__);
    }

    //3. Set the physical file size
    if (!SetEndOfFile(fileHandle)) {
      std::string reason = ExceptionUtils::GetErrorTextFromErrorCode(ExceptionUtils::GetError());
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: " << reason;
      throw FileIOException(ss.str(), __FILE__, __func__, __LINE__);
    }

    //4. Close FileHandle
    if (!CloseHandle(fileHandle)) {
      std::string reason = ExceptionUtils::GetErrorTextFromErrorCode(ExceptionUtils::GetError());
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: " << reason;
      throw FileIOException(ss.str(), __FILE__, __func__, __LINE__);
    }
#elif defined(__linux) || defined(__APPLE__)
    //1. Create a new file. This will fail if the file already exist, which is what we want.
    //   We don't want to delete or overwrite any data
    int fd;
    if ((fd = open(fileName.c_str(), O_RDWR | O_CREAT | O_EXCL,
    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) == -1) {
      int errCode = errno;
      std::string reason = ExceptionUtils::GetErrorTextFromErrorCode(errCode);
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: "
         << reason;
      throw FileIOException(ss.str(), __FILE__, __func__, __LINE__);
    }

    if (ftruncate_portable(fd, fileSize) != 0) {
      int errCode = errno;
      std::string reason = ExceptionUtils::GetErrorTextFromErrorCode(errCode);
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: "
         << reason;
      close(fd);
      throw FileIOException(ss.str(), __FILE__, __func__, __LINE__);
    }

    if (close(fd) != 0) {
      int errCode = errno;
      std::string reason = ExceptionUtils::GetErrorTextFromErrorCode(errCode);
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: "
         << reason;
      throw FileIOException(ss.str(), __FILE__, __func__, __LINE__);
    }
#else
    static_assert(false, "Unsupported platform. Supported platforms are windows, linux and OS X.");
#endif
  }
 private:
  static int ftruncate_portable(int fd, std::uint64_t fileSize) {
#ifdef __linux__
    return ftruncate64(fd, fileSize);
#elif __APPLE__
    return ftruncate(fd, fileSize);
#else
    static_assert(false, "Unsupported platform. Supported platforms are windows, linux and OS X.");
#endif
  }
};
}  // namespace jonoondb_api

