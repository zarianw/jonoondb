#pragma once

#include <cstdint>
#include <thread>
#include "status.h"

namespace jonoondb_api {
const int64_t MAX_DATA_FILE_SIZE = (1024 * 1024 * 1024);  // 1 GB

const std::chrono::milliseconds SQLiteBusyHandlerRetryIntervalInMillisecs(200);

const int SQLiteBusyHandlerRetryCount = 20;

typedef void (*DeleterFuncPtr)(char*);

const double INT32_MIN_AS_DOUBLE = double(INT32_MIN);
const double INT32_MAX_AS_DOUBLE = double(INT32_MIN);




}  // jonoondb_api
