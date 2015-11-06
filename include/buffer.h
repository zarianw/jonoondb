#pragma once

#include "constants.h"

namespace jonoondb_api {
class Buffer {
 public:
  Buffer();
  Buffer(size_t capacity);
  Buffer(Buffer&& other);
  Buffer(const Buffer& other);
  ~Buffer();
  Buffer& operator=(const Buffer& other);
  Buffer& operator=(Buffer&& other);
  bool operator<(const Buffer& other) const;
  Buffer(char* buffer, size_t bufferLengthInBytes,
    size_t bufferCapacityInBytes, DeleterFuncPtr customDeleterFunc);
  Buffer(const char* buffer, size_t bufferLengthInBytes,
    size_t bufferCapacityInBytes);  

  void Resize(size_t newBufferCapacityInBytes);
  void Reset();
  const char* GetData() const;
  char* GetDataForWrite();
  const size_t GetCapacity() const;
  const size_t GetLength() const;
  void SetLength(size_t value);
  void Copy(const char* buffer, size_t bytesToCopy);
private:
  void Copy(const char* buffer, size_t bufferLengthInBytes,
    size_t bufferCapacityInBytes);
  //Forward Declarations
  struct BufferImpl;
  BufferImpl* m_bufferImpl;
};
}  // jonoondb_api

