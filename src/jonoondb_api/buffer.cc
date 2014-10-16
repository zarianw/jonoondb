#include <memory>
#include <string>
#include <cstring>
#include "buffer.h"
#include "constants.h"
#include "standard_deleters.h"
#include "status.h"

using namespace std;

using namespace jonoondb_api;

struct Buffer::BufferImpl
{
public:
  BufferImpl(char* bufferData, size_t bufferLength, size_t bufferCapacity, DeleterFuncPtr DeleterFuncPtr) : BufferData(bufferData, DeleterFuncPtr),
    BufferLength(bufferLength), BufferCapacity(bufferCapacity)
  {
  }

  unique_ptr<char, DeleterFuncPtr> BufferData;
  size_t BufferLength;
  size_t BufferCapacity;
};

Buffer::Buffer()
{
  m_bufferImpl = nullptr;
}

Buffer::Buffer(Buffer&& other)
{
  if (this != &other)
  {
    this->m_bufferImpl = other.m_bufferImpl;
    other.m_bufferImpl = nullptr;
  }
}

Status Buffer::Assign(Buffer& buffer, DeleterFuncPtr customDeleterFunc)
{
  return Assign(buffer.GetDataForWrite(), buffer.GetLength(), buffer.GetCapacity(), customDeleterFunc);
}

Status Buffer::Assign(char* buffer, size_t bufferLengthInBytes, size_t bufferCapacityInBytes, DeleterFuncPtr customDeleterFunc)
{
  if (bufferCapacityInBytes == 0 || buffer == nullptr)
  {
    Reset();
  }
  else if (bufferCapacityInBytes < bufferLengthInBytes)
  {
    //Capacity cannot be less than length
    string errorMsg = "Argument bufferCapacityInBytes cannot be less than bufferLengthInBytes.";
    return Status(Status::InvalidArgumentCode, errorMsg.c_str(), errorMsg.length());
  }
  else if (customDeleterFunc == nullptr)
  {
    //Capacity cannot be less than length
    string errorMsg = "Argument customDeleterFunc is nullptr.";
    return Status(Status::InvalidArgumentCode, errorMsg.c_str(), errorMsg.length());
  }
  else
  {
    try
    {
      m_bufferImpl = new BufferImpl(buffer, bufferLengthInBytes, bufferCapacityInBytes, customDeleterFunc);
    }
    catch (bad_alloc)
    {
      //Memory allocation failed			
      return Status(Status::OutOfMemoryErrorCode, MemoryAllocationFailedErrorMessage.c_str(), MemoryAllocationFailedErrorMessage.length());
    }
  }

  return Status();
}

Status Buffer::Copy(const Buffer& buffer)
{
  return Copy(buffer.GetData(), buffer.GetLength(), buffer.GetCapacity());
}

Status Buffer::Copy(const char* buffer, size_t bufferLengthInBytes, size_t bufferCapacityInBytes)
{
  if (buffer == nullptr)
  {
    string errorMsg = "Argument buffer is nullptr.";
    return Status(Status::InvalidArgumentCode, errorMsg.c_str(), errorMsg.length());
  }
  else if (bufferCapacityInBytes == 0)
  {
    Reset();
  }
  else if (bufferCapacityInBytes < bufferLengthInBytes)
  {
    //Capacity cannot be less than length
    string errorMsg = "Argument bufferCapacityInBytes cannot be less than bufferLengthInBytes";
    return Status(Status::InvalidArgumentCode, errorMsg.c_str(), errorMsg.length());
  }
  else
  {
    //First check if our existing buffer has the same capacity
    if (GetCapacity() != bufferCapacityInBytes)
    {
      //We have to delete existing buffer, create a new buffer and then copy
      Reset();

      char* data = nullptr;
      try
      {
        data = new char[bufferCapacityInBytes];
        memcpy(data, buffer, bufferCapacityInBytes);
        m_bufferImpl = new BufferImpl(data, bufferLengthInBytes, bufferCapacityInBytes, StandardDelete);
      }
      catch (bad_alloc)
      {
        if (data != nullptr)
        {
          delete data;
        }
        //Memory allocation failed			
        return Status(Status::OutOfMemoryErrorCode, MemoryAllocationFailedErrorMessage.c_str(), MemoryAllocationFailedErrorMessage.length());
      }
    }
    else
    {
      //Our capacity is the same so no need to reallocate the buffer
      memcpy(m_bufferImpl->BufferData.get(), buffer, bufferCapacityInBytes);
      m_bufferImpl->BufferLength = bufferLengthInBytes;
    }
  }

  return Status();
}

Status Buffer::Copy(const char* buffer, size_t bytesToCopy)
{
  if (buffer == nullptr)
  {
    string errorMsg = "Argument buffer is nullptr.";
    return Status(Status::InvalidArgumentCode, errorMsg.c_str(), errorMsg.length());
  }
  else if (bytesToCopy > 0)
  {
    //First check if our existing buffer is big enough
    if (GetCapacity() < bytesToCopy)
    {
      //We have to delete existing buffer, create a new buffer and then copy
      Reset();

      char* data = nullptr;
      try
      {
        data = new char[bytesToCopy];
        memcpy(data, buffer, bytesToCopy);
        m_bufferImpl = new BufferImpl(data, bytesToCopy, bytesToCopy, StandardDelete);
      }
      catch (bad_alloc)
      {
        if (data != nullptr)
        {
          delete data;
        }
        //Memory allocation failed			
        return Status(Status::OutOfMemoryErrorCode, MemoryAllocationFailedErrorMessage.c_str(), MemoryAllocationFailedErrorMessage.length());
      }
    }
    else
    {
      //Our existing bufer is big enough, no need to reallocate
      memcpy(m_bufferImpl->BufferData.get(), buffer, bytesToCopy);
      m_bufferImpl->BufferLength = bytesToCopy;
    }
  }

  return Status();
}

Buffer::~Buffer()
{
  Reset();
}

Buffer& Buffer::operator=(Buffer&& other)
{
  if (this != &other)
  {
    Reset();
    this->m_bufferImpl = other.m_bufferImpl;
    other.m_bufferImpl = nullptr;
  }

  return *this;
}

bool Buffer::operator<(const Buffer& other) const
{
  if (this == &other)
  {
    //For equality, we should return false.
    // a < b = false and b < a = false means they are equal
    return false;
  }
  else
  {
    if (GetLength() != 0 && other.GetLength() != 0)
    {
      size_t bytesToCompare = GetLength();
      if (other.GetLength() < bytesToCompare)
      {
        bytesToCompare = other.GetLength();
      }

      int result = memcmp(GetData(), other.GetData(), bytesToCompare);

      if (result < 0)
      {
        return true;
      }
      else if (result > 0)
      {
        return false;
      }
      else
      {
        //result == 0
        if (GetLength() == other.GetLength())
        {
          //For equality, we should return false.
          // a < b = false and b < a = false means they are equal
          return false;
        }
        else if (GetLength() > other.GetLength())
        {
          return false;
        }
        else
        {
          //This case is: GetLength() < other.GetLength()
          return true;
        }
      }
    }
    else if (GetLength() == 0 && other.GetLength() != 0)
    {
      return true;
    }
    else if (GetLength() != 0 && other.GetLength() == 0)
    {
      return false;
    }
    else
    {
      //This case is: GetLength() == 0 && other.GetLength() == 0
      //For equality, we should return false.
      // a < b = false and b < a = false means they are equal
      return false;
    }
  }
}

Status Buffer::Resize(size_t newBufferCapacityInBytes)
{
  Reset();
  if (newBufferCapacityInBytes == 0)
  {
    m_bufferImpl = nullptr;
  }
  else
  {
    try
    {
      char* data = new char[newBufferCapacityInBytes];
      m_bufferImpl = new BufferImpl(data, newBufferCapacityInBytes, newBufferCapacityInBytes, StandardDelete);
    }
    catch (bad_alloc& ex)
    {
      //Memory allocation failed
      return Status(Status::OutOfMemoryErrorCode, ex.what(), strlen(ex.what()));
    }
  }

  return Status();
}

void Buffer::Reset()
{
  if (m_bufferImpl != nullptr)
  {
    delete m_bufferImpl;
    m_bufferImpl = nullptr;
  }
}

const char* Buffer::GetData() const
{
  if (m_bufferImpl == nullptr)
  {
    return nullptr;
  }
  else
  {
    return m_bufferImpl->BufferData.get();
  }
}

char* Buffer::GetDataForWrite()
{
  if (m_bufferImpl == nullptr)
  {
    return nullptr;
  }
  else
  {
    return m_bufferImpl->BufferData.get();
  }
}

const size_t Buffer::GetCapacity() const
{
  if (m_bufferImpl == nullptr)
  {
    return 0;
  }
  else
  {
    return m_bufferImpl->BufferCapacity;
  }
}

const size_t Buffer::GetLength() const
{
  if (m_bufferImpl == nullptr)
  {
    return 0;
  }
  else
  {
    return m_bufferImpl->BufferLength;
  }
}

Status Buffer::SetLength(size_t value)
{
  if (m_bufferImpl == nullptr)
  {
    if (value != 0)
    {
      string errorMsg = "Specified length is not between 0 and buffer capacity.";
      return Status(Status::GenericErrorCode, errorMsg.c_str(), errorMsg.length());
    }
  }
  else
  {
    if (value > m_bufferImpl->BufferCapacity)
    {
      string errorMsg = "Specified buffer length is not between 0 and buffer capacity.";
      return Status(Status::GenericErrorCode, errorMsg.c_str(), errorMsg.length());
    }
    else
    {
      m_bufferImpl->BufferLength = value;
    }
  }

  return Status();
}
