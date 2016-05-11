#pragma once

#include <memory>
#include <string>
#include <fstream>
#include <cstdlib>
#include "jonoondb_api/jonoondb_exceptions.h"
#include "jonoondb_api/exception_utils.h"

#if defined(_WIN32)
#include <Windows.h>
#include <Psapi.h>
#else
// Linux code goes here
#include <sys/types.h>
#include <unistd.h>
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
      //throw exception
    }

    char* curr = nullptr;
    // read size - total program size
    auto size = strtoull(line.c_str(), &curr, 10);
    // read resident - resident set size
    auto resident = strtoull(curr, &curr, 10);

    stat.MemoryUsedInBytes = resident * pageSize;
  }
#endif 
};
} // namespace jonoondb_api
