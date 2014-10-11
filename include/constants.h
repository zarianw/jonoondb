#pragma once

#include <cstdint>
#include <thread>
#include "status.h"

namespace jonoondb_api
{
	const int64_t MAX_DATA_FILE_SIZE = (1024 * 1024 * 1024); // 1 GB	

	const std::chrono::milliseconds SQLiteBusyHandlerRetryIntervalInMillisecs(200);

	const int SQLiteBusyHandlerRetryCount = 20;

  typedef void(*DeleterFuncPtr)(char*);

  const std::string MemoryAllocationFailedErrorMessage = "Memory allocation failed.";

  const Status InvalidIteratorStatus(Status::InvalidIteratorCode, "Iterator initialized with invalid data.",
    strlen("Iterator initialized with invalid data."));

} // jonoon_api