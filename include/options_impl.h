#pragma once

#include <cstddef>

namespace jonoondb_api {
class OptionsImpl {
 public:
  //Default constructor that sets all the options to their default value
  OptionsImpl();
  OptionsImpl(bool createDBIfMissing, size_t maxDataFileSize,
          bool compressionEnabled, bool synchronous);  

  void SetCreateDBIfMissing(bool value);
  bool GetCreateDBIfMissing() const;

  void SetCompressionEnabled(bool value);
  bool GetCompressionEnabled() const;

  void SetMaxDataFileSize(size_t value);
  size_t GetMaxDataFileSize() const;

  void SetSynchronous(bool value);
  bool GetSynchronous() const;

 private:
   bool CreateDBIfMissing;
   std::size_t MaxDataFileSize;
   bool CompressionEnabled;
   bool Synchronous;
};

}  // namespace jonoondb_api
