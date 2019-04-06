#include <memory>
#include <string>
#include <cstring>
#include <sstream>
#include "buffer_impl.h"
#include "standard_deleters.h"
#include "jonoondb_exceptions.h"

using namespace std;

using namespace jonoondb_api;

BufferImpl::BufferImpl() : 
  m_buffer(nullptr, StandardDelete), m_length(0),
  m_capacity(0) {
}

BufferImpl::BufferImpl(size_t capacity) : BufferImpl() {
  if (capacity > 0) {
    m_buffer.reset(new char[capacity]);
    m_length = 0;
    m_capacity = capacity;    
  }
}

BufferImpl::BufferImpl(const char* buffer, size_t bufferLengthInBytes,
                       size_t bufferCapacityInBytes) : BufferImpl() {
  if (buffer == nullptr && bufferLengthInBytes == 0
      && bufferCapacityInBytes == 0) {
    // This is a special case, this kind of buffer is created by default ctor  
  } else if (buffer == nullptr) {
    throw InvalidArgumentException(
        "Argument buffer is nullptr whereas bufferLengthInBytes or bufferCapacityInBytes are greater than 0.",
        __FILE__,
        __func__,
        __LINE__);
  } else if (bufferLengthInBytes == 0 || bufferCapacityInBytes == 0) {
    throw InvalidArgumentException(
        "Argument buffer is a valid pointer but bufferLengthInBytes or bufferCapacityInBytes are 0.",
        __FILE__,
        __func__,
        __LINE__);
  } else if (bufferCapacityInBytes < bufferLengthInBytes) {
    // Capacity cannot be less than length    
    throw InvalidArgumentException(
        "Argument bufferCapacityInBytes cannot be less than bufferLengthInBytes.",
        __FILE__,
        __func__,
        __LINE__);
  } else {
    m_buffer.reset(new char[bufferCapacityInBytes]);
    memcpy(m_buffer.get(), buffer, bufferLengthInBytes);
    m_length = bufferLengthInBytes;
    m_capacity = bufferCapacityInBytes;
  }
}

BufferImpl::BufferImpl(char* buffer,
                       size_t bufferLengthInBytes,
                       size_t bufferCapacityInBytes,
                       void(* customDeleterFunc)(char*)) : BufferImpl() {
  if (buffer == nullptr && bufferLengthInBytes == 0
      && bufferCapacityInBytes == 0) {
    m_buffer =
      std::unique_ptr<char, void(*)(char*)>(
        nullptr,
        (customDeleterFunc == nullptr) ? StandardDeleteNoOp : customDeleterFunc);
    m_length = 0;
    m_capacity = 0;
  } else if (buffer == nullptr) {
    throw InvalidArgumentException(
        "Argument buffer is nullptr whereas bufferLengthInBytes or bufferCapacityInBytes are greater than 0.",
        __FILE__,
        __func__,
        __LINE__);
  } else if (bufferLengthInBytes == 0 || bufferCapacityInBytes == 0) {
    throw InvalidArgumentException(
        "Argument buffer is a valid pointer but bufferLengthInBytes or bufferCapacityInBytes are 0.",
        __FILE__,
        __func__,
        __LINE__);
  } else if (bufferCapacityInBytes < bufferLengthInBytes) {
    // Capacity cannot be less than length    
    throw InvalidArgumentException(
        "Argument bufferCapacityInBytes cannot be less than bufferLengthInBytes.",
        __FILE__,
        __func__,
        __LINE__);
  } else {
    // nullptr for deleter means user does not want to delete this memory on 
    // destruction of this buffer.
    m_buffer =
      std::unique_ptr<char, void(*)(char*)>(
        buffer,
        (customDeleterFunc == nullptr) ? StandardDeleteNoOp : customDeleterFunc);
    m_length = bufferLengthInBytes;
    m_capacity = bufferCapacityInBytes;
  }
}

BufferImpl::BufferImpl(BufferImpl&& other) noexcept :
    m_buffer(std::move(other.m_buffer)),
    m_length(other.GetLength()),
    m_capacity(other.GetCapacity()) {
}

BufferImpl::BufferImpl(const BufferImpl& other) : BufferImpl() {
  if (other.GetData() != nullptr) {
    m_buffer.reset(new char[other.GetCapacity()]);
    memcpy(m_buffer.get(), other.GetData(), other.GetCapacity());
    m_length = other.GetLength();
    m_capacity = other.GetCapacity();
  }
}

BufferImpl& BufferImpl::operator=(const BufferImpl& other) {
  if (this != &other) {
    if (other.GetData() == nullptr) {
      m_buffer.reset(nullptr);
    } else {
      // We have to delete existing buffer, create a new buffer and then copy
      // First check if our existing buffer has the same capacity
      if (GetCapacity() != other.GetCapacity()) {
        std::unique_ptr<char, void (*)(char*)>
            data(new char[other.GetCapacity()], StandardDelete);
        memcpy(data.get(), other.GetData(), other.GetCapacity());        
        m_buffer = std::move(data);             
      } else {
        // Our capacity is the same so no need to reallocate the buffer
        memcpy(m_buffer.get(),
               other.GetData(),
               other.GetCapacity());        
      }
    }

    m_length = other.GetLength();
    m_capacity = other.GetCapacity();
  }

  return *this;
}

BufferImpl& BufferImpl::operator=(BufferImpl&& other) noexcept {
  if (this != &other) {
    this->m_buffer = std::move(other.m_buffer);
    this->m_length = other.GetLength();
    this->m_capacity = other.GetCapacity();
  }

  return *this;
}

bool BufferImpl::operator<(const BufferImpl& other) const {
  if (this == &other) {
    // For equality, we should return false.
    // a < b = false and b < a = false means they are equal
    return false;
  } else {
    if (GetLength() != 0 && other.GetLength() != 0) {
      size_t bytesToCompare = GetLength();
      if (other.GetLength() < bytesToCompare) {
        bytesToCompare = other.GetLength();
      }

      int result = memcmp(GetData(), other.GetData(), bytesToCompare);

      if (result < 0) {
        return true;
      } else if (result > 0) {
        return false;
      } else {
        // result == 0
        if (GetLength() == other.GetLength()) {
          // For equality, we should return false.
          // a < b = false and b < a = false means they are equal
          return false;
        } else if (GetLength() > other.GetLength()) {
          return false;
        } else {
          // This case is: GetLength() < other.GetLength()
          return true;
        }
      }
    } else if (GetLength() == 0 && other.GetLength() != 0) {
      return true;
    } else if (GetLength() != 0 && other.GetLength() == 0) {
      return false;
    } else {
      // This case is: GetLength() == 0 && other.GetLength() == 0
      // For equality, we should return false.
      // a < b = false and b < a = false means they are equal
      return false;
    }
  }
}

bool BufferImpl::operator<=(const BufferImpl& other) const {
  return !(other < *this);
}

bool BufferImpl::operator>(const BufferImpl& other) const {
  return (other < *this);
}

bool BufferImpl::operator>=(const BufferImpl& other) const {
  return !(*this < other);
}

bool BufferImpl::operator==(const BufferImpl & other) const {
  return (!(*this < other) && !(other < *this));
}

bool BufferImpl::operator!=(const BufferImpl & other) const {
  return (*this < other || other < *this);
}

void BufferImpl::Resize(size_t newBufferCapacityInBytes) {
  if (newBufferCapacityInBytes == 0) {
    m_buffer.reset(nullptr);
    m_length = 0;
    m_capacity = 0;
  } else if (newBufferCapacityInBytes == GetCapacity()) {
    return; // no op
  } else {
    std::unique_ptr<char, void (*)(char*)>
        data(new char[newBufferCapacityInBytes], StandardDelete);    
    m_buffer = std::move(data);
    m_length = 0;
    m_capacity = newBufferCapacityInBytes;
  }
}

const char* BufferImpl::GetData() const {
  return m_buffer.get();
}

char* BufferImpl::GetDataForWrite() {
  return m_buffer.get();
}

const size_t BufferImpl::GetCapacity() const {
  return m_capacity;
}

const size_t BufferImpl::GetLength() const {
  return m_length;
}

void BufferImpl::SetLength(size_t val) {
  if (GetCapacity() < val) {
    // Capacity cannot be less than length    
    std::ostringstream ss;
    ss << "Argument val specifying buffer length " << val
        << " cannot be greater than the buffer capacity "
        << GetCapacity() << ".";
    throw InvalidArgumentException(ss.str(), __FILE__, __func__, __LINE__);
  }

  if (GetLength() == val) return; // no op

  m_length = val;
}

void BufferImpl::Copy(const char* buffer, size_t bytesToCopy) {
  if (buffer == nullptr) {
    throw InvalidArgumentException("Argument buffer is nullptr.",
                                   __FILE__,
                                   __func__,
                                   __LINE__);
  } else if (bytesToCopy > 0) {
    // First check if our existing buffer is big enough
    if (GetCapacity() < bytesToCopy) {
      // Our internal buffer is not big enough
      ostringstream ss;
      ss << "Cannot copy " << bytesToCopy << " bytes into a buffer of capacity "
          << GetCapacity() << " bytes.";
      throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
    } else {
      // Our existing bufer is big enough
      memcpy(m_buffer.get(), buffer, bytesToCopy);
      m_length = bytesToCopy;
    }
  }
}
