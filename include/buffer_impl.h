#pragma once

#include "constants.h"

namespace jonoondb_api {
class BufferImpl {
 public:
  BufferImpl();
  BufferImpl(size_t capacity);
  BufferImpl(BufferImpl&& other);
  BufferImpl(const BufferImpl& other);
  ~BufferImpl();
  BufferImpl& operator=(const BufferImpl& other);
  BufferImpl& operator=(BufferImpl&& other);
  bool operator<(const BufferImpl& other) const;
  BufferImpl(char* buffer, size_t bufferLengthInBytes,
    size_t bufferCapacityInBytes, DeleterFuncPtr customDeleterFunc);
  BufferImpl(const char* buffer, size_t bufferLengthInBytes,
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
  struct BufferData;
  BufferData* m_bufferImpl;
};
}  // jonoondb_api

