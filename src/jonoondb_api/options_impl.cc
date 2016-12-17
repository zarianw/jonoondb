#include "options_impl.h"

using namespace jonoondb_api;

OptionsImpl::OptionsImpl() {
  m_createDBIfMissing = true;
  m_maxDataFileSize = 1024L * 1024L * 512L; // 512 MB
  m_memCleanupThresholdInBytes = 1024LL * 1024LL * 1024LL * 4LL; // 4 GB
}

OptionsImpl::OptionsImpl(bool createDBIfMissing, size_t maxDataFileSize,
                         std::size_t memClenupThresholdInBytes) :
    m_createDBIfMissing(createDBIfMissing), m_maxDataFileSize(maxDataFileSize),    
    m_memCleanupThresholdInBytes(memClenupThresholdInBytes) {
}

void OptionsImpl::SetCreateDBIfMissing(bool value) {
  m_createDBIfMissing = value;
}

bool OptionsImpl::GetCreateDBIfMissing() const {
  return m_createDBIfMissing;
}

void OptionsImpl::SetMaxDataFileSize(size_t value) {
  m_maxDataFileSize = value;
}

size_t OptionsImpl::GetMaxDataFileSize() const {
  return m_maxDataFileSize;
}

void OptionsImpl::SetMemoryCleanupThreshold(std::size_t valInBytes) {
  m_memCleanupThresholdInBytes = valInBytes;
}

std::size_t OptionsImpl::GetMemoryCleanupThreshold() {
  return m_memCleanupThresholdInBytes;
}
