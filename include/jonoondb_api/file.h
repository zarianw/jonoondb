#pragma once

#include <string>
#include <sstream>
#include <fstream>
#include "jonoondb_exceptions.h"

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
      std::string reason = GetErrorTextFromErrorCode(GetError());
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
      std::string reason = GetErrorTextFromErrorCode(GetError());
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
      std::string reason = GetErrorTextFromErrorCode(GetLastError());
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: " << reason;
      throw FileIOException(ss.str(), __FILE__, __func__, __LINE__);
    }

    //2. Move the file pointer to the desired position
    LARGE_INTEGER li;
    li.QuadPart = fileSize;

    if (SetFilePointer(fileHandle, li.LowPart, &li.HighPart, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
      std::string reason = GetErrorTextFromErrorCode(GetLastError());
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: " << reason;
      throw FileIOException(ss.str(), __FILE__, __func__, __LINE__);
    }

    //3. Set the physical file size
    if (!SetEndOfFile(fileHandle)) {
      std::string reason = GetErrorTextFromErrorCode(GetLastError());
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: " << reason;
      throw FileIOException(ss.str(), __FILE__, __func__, __LINE__);
    }

    //4. Close FileHandle
    if (!CloseHandle(fileHandle)) {
      std::string reason = GetErrorTextFromErrorCode(GetLastError());
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: " << reason;
      throw FileIOException(ss.str(), __FILE__, __func__, __LINE__);
    }
#else
    // Linux code
    //1. Create a new file. This will fail if the file already exist, which is what we want.
    //   We don't want to delete or overwrite any data
    int fd;
    if ((fd = open(fileName.c_str(), O_RDWR | O_CREAT | O_EXCL,
    S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) == -1) {
      int errCode = errno;
      std::string reason = GetErrorTextFromErrorCode(errCode);
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: "
         << reason;
      throw FileIOException(ss.str(), __FILE__, __func__, __LINE__);
    }

    if (ftruncate64(fd, fileSize) != 0) {
      int errCode = errno;
      std::string reason = GetErrorTextFromErrorCode(errCode);
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: "
         << reason;
      close(fd);
      throw FileIOException(ss.str(), __FILE__, __func__, __LINE__);
    }

    if (close(fd) != 0) {
      int errCode = errno;
      std::string reason = GetErrorTextFromErrorCode(errCode);
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: "
         << reason;
      throw FileIOException(ss.str(), __FILE__, __func__, __LINE__);
    }
#endif
  }
 private:
#if defined(_WIN32)
  static std::string GetErrorTextFromErrorCode(DWORD errorCode) {
    std::string errorMsg;
    char* msg = nullptr;
    // Ask Windows to prepare a standard message for a GetLastError() code:
    DWORD size = FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
        NULL, errorCode, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&msg, 0, NULL);
    // Return the message
    if (size == 0) {
      return "Unknown error.";
    } else {
      errorMsg = msg;
    }

    if (msg != nullptr) {
      LocalFree(msg);
    }

    return errorMsg;  //Move ctor will be called here
  }
#else
  // Linux code
  static std::string GetErrorTextFromErrorCode(int errorCode) {
    char reason[512];
    return strerror_r(errorCode, reason, 512);
  }
#endif

#if defined(_WIN32)
  static int GetError() {
    return GetLastError();
  }
#else
  // Linux code
  static int GetError() {
    return errno;
  }
#endif
};
}  // namespace jonoondb_api

