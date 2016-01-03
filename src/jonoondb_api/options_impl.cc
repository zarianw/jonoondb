#include "options_impl.h"
#include "constants.h"

using namespace jonoondb_api;

OptionsImpl::OptionsImpl() {
  CreateDBIfMissing = true;
  MaxDataFileSize = MAX_DATA_FILE_SIZE;
  CompressionEnabled = false;
  Synchronous = true;
}

OptionsImpl::OptionsImpl(bool createDBIfMissing, size_t maxDataFileSize,
                 bool compressionEnabled, bool synchronous) {
  CreateDBIfMissing = createDBIfMissing;
  MaxDataFileSize = maxDataFileSize;
  CompressionEnabled = compressionEnabled;
  Synchronous = synchronous;
}

void OptionsImpl::SetCreateDBIfMissing(bool value) {
  CreateDBIfMissing = value;
}

bool OptionsImpl::GetCreateDBIfMissing() const {
  return CreateDBIfMissing;
}

void OptionsImpl::SetCompressionEnabled(bool value) {
  CompressionEnabled = value;
}

bool OptionsImpl::GetCompressionEnabled() const {
  return CompressionEnabled;
}

void OptionsImpl::SetMaxDataFileSize(size_t value) {
  MaxDataFileSize = value;
}

size_t OptionsImpl::GetMaxDataFileSize() const {
  return MaxDataFileSize;
}

void OptionsImpl::SetSynchronous(bool value) {
  Synchronous = value;
}

bool OptionsImpl::GetSynchronous() const {
  return Synchronous;
}
