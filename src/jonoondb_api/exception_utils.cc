#include "exception_utils.h"

#if defined(_WIN32)
#include <Windows.h>
#elif defined(__linux__) || defined(__APPLE__)
// Linux and OS X code
#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#else
static_assert(
    false,
    "Unsupported platform. Supported platforms are windows, linux and OS X.");
#endif

using namespace jonoondb_api;

std::string ExceptionUtils::GetMissingFieldErrorString(
    const std::string& fieldName) {
  std::ostringstream ss;
  ss << "Field definition for " << fieldName
     << " not found in the parsed schema.";
  return ss.str();
}

std::string ExceptionUtils::GetInvalidStructFieldErrorString(
    const std::string& fieldName, const std::string& fullName) {
  std::ostringstream ss;
  ss << "Field " << fieldName
     << " is not of type struct. Full name provided was " << fullName;
  return ss.str();
}

#ifdef _WIN32
int ExceptionUtils::GetError() {
  return GetLastError();
}

std::string ExceptionUtils::GetErrorTextFromErrorCode(int errorCode) {
  std::string errorMsg;
  char* msg = nullptr;
  // Ask Windows to prepare a standard message for a GetLastError() code:
  auto size = FormatMessage(
      FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM |
          FORMAT_MESSAGE_IGNORE_INSERTS,
      NULL, errorCode, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), (LPSTR)&msg,
      0, NULL);
  // Return the message
  if (size == 0) {
    return "Unknown error.";
  } else {
    errorMsg = msg;
  }

  if (msg != nullptr) {
    LocalFree(msg);
  }

  return errorMsg;  // Move ctor will be called here
}
#elif __linux__
int ExceptionUtils::GetError() {
  return errno;
}

std::string ExceptionUtils::GetErrorTextFromErrorCode(int errorCode) {
  char reason[512];
  return strerror_r(errorCode, reason, 512);
}
#elif __APPLE__
int ExceptionUtils::GetError() {
  return errno;
}

std::string ExceptionUtils::GetErrorTextFromErrorCode(int errorCode) {
  char reason[512];
  if (strerror_r(errorCode, reason, 512) == 0) {
    return reason;
  } else {
    // strerror_r failed
    return "";
  }
}
#else
static_assert(
    false,
    "Unsupported platform. Supported platforms are windows, linux and OS X.");
#endif
