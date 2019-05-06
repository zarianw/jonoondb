#pragma once

#include <cstddef>

namespace jonoondb_api {
class OptionsImpl {
 public:
  // Default constructor that sets all the options to their default value
  OptionsImpl();
  OptionsImpl(bool createDBIfMissing, std::size_t maxDataFileSize,
              std::size_t memClenupThresholdInBytes);

  void SetCreateDBIfMissing(bool value);
  bool GetCreateDBIfMissing() const;

  void SetMaxDataFileSize(std::size_t value);
  std::size_t GetMaxDataFileSize() const;

  void SetMemoryCleanupThreshold(std::size_t valInBytes);
  std::size_t GetMemoryCleanupThreshold();

 private:
  bool m_createDBIfMissing;
  std::size_t m_maxDataFileSize;
  std::size_t m_memCleanupThresholdInBytes;
};
}  // namespace jonoondb_api
