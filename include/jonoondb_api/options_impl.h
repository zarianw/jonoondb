#pragma once

#include <cstdint>
#include <cstddef>

namespace jonoondb_api {
class OptionsImpl {
 public:
  //Default constructor that sets all the options to their default value
  OptionsImpl();
  OptionsImpl(bool createDBIfMissing, std::size_t maxDataFileSize,
              bool compressionEnabled, bool synchronous,
              std::size_t memClenupThresholdInBytes);

  void SetCreateDBIfMissing(bool value);
  bool GetCreateDBIfMissing() const;

  void SetCompressionEnabled(bool value);
  bool GetCompressionEnabled() const;

  void SetMaxDataFileSize(std::size_t value);
  std::size_t GetMaxDataFileSize() const;

  void SetSynchronous(bool value);
  bool GetSynchronous() const;

  void SetMemoryCleanupThreshold(std::size_t valInBytes);
  std::size_t GetMemoryCleanupThreshold();

 private:
  bool m_createDBIfMissing;
  std::size_t m_maxDataFileSize;
  bool m_compressionEnabled;
  bool m_synchronous;
  std::size_t m_memCleanupThresholdInBytes;
};
}  // namespace jonoondb_api
