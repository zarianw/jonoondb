#pragma once

#include <memory>
#include <string>
#include <fstream>
#include <cstdlib>
#include "jonoondb_api/jonoondb_exceptions.h"
#include "jonoondb_api/exception_utils.h"

#if _WIN32
#include <Windows.h>
#include <Psapi.h>
#elif __linux__
#include <sys/types.h>
#include <unistd.h>
#elif __APPLE__
#include <mach/task.h>
#include <mach/mach.h>
#else
static_assert(false, "Unsupported platform. Supported platforms are windows, linux and OS X.");
#endif

namespace jonoondb_api {
struct ProcessMemStat {
  std::size_t MemoryUsedInBytes;
};

class ProcessUtils {
 public:
#ifdef _WIN32
  static void GetProcessMemoryStats(ProcessMemStat& stat) {
    stat.MemoryUsedInBytes = 0;
    HANDLE hProcess = GetCurrentProcess();
    // GetCurrentProcess returns pseudo handle so no need to close it
    PROCESS_MEMORY_COUNTERS pmc;
    if (!GetProcessMemoryInfo(hProcess, &pmc, sizeof(pmc))) {
      auto msg = ExceptionUtils::GetErrorTextFromErrorCode(ExceptionUtils::GetError());
      throw jonoondb_api::JonoonDBException(msg, __FILE__, __func__, __LINE__);
    }
    stat.MemoryUsedInBytes = pmc.WorkingSetSize;
  }
#elif __linux__
  // Linux code goes here
  static void GetProcessMemoryStats(ProcessMemStat& stat) {
    static auto pageSize = getpagesize();
    stat.MemoryUsedInBytes = 0;
    auto pid = getpid();
    std::string fileName = "/proc/";
    fileName.append(std::to_string(pid));
    fileName.append("/statm");
    std::ifstream ifs(fileName);
    std::string line;
    std::getline(ifs, line);
    if(line.size() == 0) {
      std::string message = "Unable to read " + fileName + " to determine memory usage.";
      throw jonoondb_api::JonoonDBException(message, __FILE__, __func__, __LINE__);
    }

    char* curr = nullptr;
    // read size - total program size
    auto size = strtoull(line.c_str(), &curr, 10);
    // read resident - resident set size
    auto resident = strtoull(curr, &curr, 10);

    stat.MemoryUsedInBytes = resident * pageSize;
  }
#elif __APPLE__
  static void GetProcessMemoryStats(ProcessMemStat& stat) {
    stat.MemoryUsedInBytes = 0;

    struct task_basic_info t_info;
    mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;

    if (KERN_SUCCESS != task_info(mach_task_self(),
                                  TASK_BASIC_INFO,
                                  (task_info_t) &t_info, &t_info_count)) {
      std::string message = "Unable to to determine memory usage. ";
      message.append(ExceptionUtils::GetErrorTextFromErrorCode(ExceptionUtils::GetError()));
      throw jonoondb_api::JonoonDBException(message, __FILE__, __func__, __LINE__);
    }
    stat.MemoryUsedInBytes = t_info.resident_size;
  }
#else
  static_assert(false, "Unsupported platform. Supported platforms are windows, linux and OS X.");
#endif
};
} // namespace jonoondb_api
