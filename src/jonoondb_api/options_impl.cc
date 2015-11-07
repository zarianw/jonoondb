#include "options_impl.h"
#include "constants.h"

using namespace jonoondb_api;

struct OptionsImpl::OptionsData {
  bool CreateDBIfMissing;
  size_t MaxDataFileSize;
  bool CompressionEnabled;
  bool Synchronous;
};

OptionsImpl::OptionsImpl() {
  m_optionsData = new OptionsData();
  m_optionsData->CreateDBIfMissing = false;
  m_optionsData->MaxDataFileSize = MAX_DATA_FILE_SIZE;
  m_optionsData->CompressionEnabled = true;
  m_optionsData->Synchronous = true;
}

OptionsImpl::OptionsImpl(OptionsImpl&& other) {
  if (this != &other) {
    m_optionsData = other.m_optionsData;
    other.m_optionsData = nullptr;
  }
}

OptionsImpl::OptionsImpl(bool createDBIfMissing, size_t maxDataFileSize,
                 bool compressionEnabled, bool synchronous) {
  m_optionsData = new OptionsData();
  m_optionsData->CreateDBIfMissing = createDBIfMissing;
  m_optionsData->MaxDataFileSize = maxDataFileSize;
  m_optionsData->CompressionEnabled = compressionEnabled;
  m_optionsData->Synchronous = synchronous;
}

OptionsImpl::~OptionsImpl() {
  delete m_optionsData;
  m_optionsData = nullptr;
}

void OptionsImpl::SetCreateDBIfMissing(bool value) {
  m_optionsData->CreateDBIfMissing = value;
}

bool OptionsImpl::GetCreateDBIfMissing() const {
  return m_optionsData->CreateDBIfMissing;
}

void OptionsImpl::SetCompressionEnabled(bool value) {
  m_optionsData->CompressionEnabled = value;
}

bool OptionsImpl::GetCompressionEnabled() const {
  return m_optionsData->CompressionEnabled;
}

void OptionsImpl::SetMaxDataFileSize(size_t value) {
  m_optionsData->MaxDataFileSize = value;
}

size_t OptionsImpl::GetMaxDataFileSize() const {
  return m_optionsData->MaxDataFileSize;
}

void OptionsImpl::SetSynchronous(bool value) {
  m_optionsData->Synchronous = value;
}

bool OptionsImpl::GetSynchronous() const {
  return m_optionsData->Synchronous;
}
