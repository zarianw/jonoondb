#pragma once

#include <string>
#include <sstream>
#include "jonoondb_exceptions.h"

#if defined(_WIN32)
#include <Windows.h>
#else
// Linux code goes here
#endif

namespace jonoondb_api {
class File {
public:
  static void FastAllocate(std::string& fileName, std::size_t fileSize) {
#if defined(_WIN32)
    //1. Create a new file. This will fail if the file already exist, which is what we want.
    //   We don't want to delete or overwrite any data
    auto fileHandle = CreateFile(fileName.c_str(),
      GENERIC_WRITE,
      NULL, //No Sharing
      NULL,
      CREATE_NEW,
      NULL,
      NULL);

    if (fileHandle == INVALID_HANDLE_VALUE) {
      std::string reason = GetErrorTextFromErrorCode(GetLastError());
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: " << reason;
      throw FileIOException(ss.str(), __FILE__, "", __LINE__);
    }

    //2. Move the file pointer to the desired position
    LARGE_INTEGER li;
    li.QuadPart = fileSize;

    if (SetFilePointer(fileHandle, li.LowPart, &li.HighPart, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
      std::string reason = GetErrorTextFromErrorCode(GetLastError());
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: " << reason;
      throw FileIOException(ss.str(), __FILE__, "", __LINE__);
    }

    //3. Set the physical file size
    if (!SetEndOfFile(fileHandle)) {
      std::string reason = GetErrorTextFromErrorCode(GetLastError());
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: " << reason;
      throw FileIOException(ss.str(), __FILE__, "", __LINE__);
    }

    //4. Close FileHandle
    if (!CloseHandle(fileHandle)) {
      std::string reason = GetErrorTextFromErrorCode(GetLastError());
      std::ostringstream ss;
      ss << "Fast allocate for file " << fileName << " failed. Reason: " << reason;
      throw FileIOException(ss.str(), __FILE__, "", __LINE__);
    }
#else
    // Linux code here
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
      return "Unknown error";
    } else {
      errorMsg = msg;
    }

    if (msg != nullptr) {
      LocalFree(msg);
    }

    return errorMsg; //Move ctor will be called here
  }
#endif
};
} // namespace jonoondb_api

