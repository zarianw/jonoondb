#pragma once

#include "constants.h"

namespace jonoondb_api
{
  //Forward Declarations
  class Status;

  class Buffer
  {
  private:
    //Forward Declarations
    struct BufferImpl;

    BufferImpl* m_bufferImpl;
    Buffer(const Buffer& other);
    Buffer& operator=(const Buffer& other);
  public:
    Buffer();

    Buffer(Buffer&& other);
    ~Buffer();
    Buffer& operator=(Buffer&& other);
    bool operator<(const Buffer& other) const;


    Status Assign(char* buffer, size_t bufferLengthInBytes, size_t bufferCapacityInBytes, DeleterFuncPtr customDeleterFunc);
    Status Assign(Buffer& buffer, DeleterFuncPtr customDeleterFunc);

    Status Copy(const char* buffer, size_t bufferLengthInBytes, size_t bufferCapacityInBytes);
    Status Copy(const Buffer& buffer);
    Status Copy(const char* buffer, size_t bytesToCopy);

    Status Resize(size_t newBufferCapacityInBytes);
    void Reset();
    const char* GetData() const;
    char* GetDataForWrite();
    const size_t GetCapacity() const;
    const size_t GetLength() const;
    Status SetLength(size_t value);
  };
} // jonoondb_api

