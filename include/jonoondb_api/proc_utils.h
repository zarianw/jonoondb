#pragma once

#include <memory>
#include "jonoondb_api/jonoondb_exceptions.h"

#if defined(_WIN32)
#include <Windows.h>
#include <Psapi.h>
#else
// Linux code goes here
#endif

namespace jonoondb_api {
struct ProcessMemStat {
  int MemoryUsedInBytes;
};

class ProcessUtils {
public:
#if defined(_WIN32)
  static void GetProcessMemoryStats(ProcessMemStat& stat) {
    stat.MemoryUsedInBytes = 0;
    HANDLE hProcess = GetCurrentProcess();
    std::unique_ptr<void, BOOL(*)(HANDLE)> ptr(hProcess, CloseHandle);
    PROCESS_MEMORY_COUNTERS pmc;
    if (!GetProcessMemoryInfo(hProcess, &pmc, sizeof(pmc))) {
      throw jonoondb_api::JonoonDBException("GetProcessMemoryStats failed.",
                                            __FILE__, __func__, __LINE__);
    }
    stat.MemoryUsedInBytes = pmc.WorkingSetSize;
  }
#else
  // Linux code goes here
  static void GetProcessMemoryStats(ProcMemStat& stat) {

  }
#endif 
};
} // namespace jonoondb_api