#pragma once

#include <memory>
#include "jonoondb_api/jonoondb_exceptions.h"
#include "jonoondb_api/exception_utils.h"

#if defined(_WIN32)
#include <Windows.h>
#include <Psapi.h>
#else
// Linux code goes here
#endif

namespace jonoondb_api {
struct ProcessMemStat {
  std::size_t MemoryUsedInBytes;
};

class ProcessUtils {
public:
#if defined(_WIN32)
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
#else
  // Linux code goes here
  static void GetProcessMemoryStats(ProcessMemStat& stat) {
    stat.MemoryUsedInBytes = 0;
  }
#endif 
};
} // namespace jonoondb_api
