#include "gtest/gtest.h"
#include "jonoondb_api/proc_utils.h"

using namespace jonoondb_api;

TEST(ProcessUtils, GetProcessMemoryStats) {
  ProcessMemStat stat;
  ProcessUtils::GetProcessMemoryStats(stat);
  ASSERT_GT(stat.MemoryUsedInBytes, 0);
}