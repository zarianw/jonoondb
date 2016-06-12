#include "options_impl.h"

using namespace jonoondb_api;

OptionsImpl::OptionsImpl() {
  m_createDBIfMissing = true;
  m_maxDataFileSize = 1024L * 1024L * 512L; // 512 MB
  m_compressionEnabled = false;
  m_synchronous = true;
  m_memCleanupThresholdInBytes = 1024LL * 1024LL * 1024LL * 4LL; // 4 GB
}

OptionsImpl::OptionsImpl(bool createDBIfMissing, size_t maxDataFileSize,
                         bool compressionEnabled, bool synchronous,
                         std::size_t memClenupThresholdInBytes) :
    m_createDBIfMissing(createDBIfMissing), m_maxDataFileSize(maxDataFileSize),
    m_compressionEnabled(compressionEnabled), m_synchronous(synchronous),
    m_memCleanupThresholdInBytes(memClenupThresholdInBytes) {
}

void OptionsImpl::SetCreateDBIfMissing(bool value) {
  m_createDBIfMissing = value;
}

bool OptionsImpl::GetCreateDBIfMissing() const {
  return m_createDBIfMissing;
}

void OptionsImpl::SetCompressionEnabled(bool value) {
  m_compressionEnabled = value;
}

bool OptionsImpl::GetCompressionEnabled() const {
  return m_compressionEnabled;
}

void OptionsImpl::SetMaxDataFileSize(size_t value) {
  m_maxDataFileSize = value;
}

size_t OptionsImpl::GetMaxDataFileSize() const {
  return m_maxDataFileSize;
}

void OptionsImpl::SetSynchronous(bool value) {
  m_synchronous = value;
}

bool OptionsImpl::GetSynchronous() const {
  return m_synchronous;
}

void OptionsImpl::SetMemoryCleanupThreshold(std::size_t valInBytes) {
  m_memCleanupThresholdInBytes = valInBytes;
}

std::size_t OptionsImpl::GetMemoryCleanupThreshold() {
  return m_memCleanupThresholdInBytes;
}
