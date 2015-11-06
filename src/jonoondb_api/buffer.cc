#include <memory>
#include <string>
#include <cstring>
#include <sstream>
#include "buffer.h"
#include "constants.h"
#include "standard_deleters.h"
#include "jonoondb_exceptions.h"

using namespace std;

using namespace jonoondb_api;

struct Buffer::BufferImpl {
 public:
  BufferImpl(std::unique_ptr<char, DeleterFuncPtr> bufferData, size_t bufferLength, size_t bufferCapacity)
      : BufferData(std::move(bufferData)),
        BufferLength(bufferLength),
        BufferCapacity(bufferCapacity) {
  }

  std::unique_ptr<char, DeleterFuncPtr> BufferData;
  size_t BufferLength;
  size_t BufferCapacity;
};

Buffer::Buffer() : m_bufferImpl(nullptr) {
}

Buffer::Buffer(size_t capacity) : m_bufferImpl(nullptr) {
  if (capacity > 0) {
    std::unique_ptr<char, DeleterFuncPtr> data(new char[capacity], StandardDelete);
    m_bufferImpl = new BufferImpl(std::move(data), 0, capacity);
  }
}

Buffer::Buffer(Buffer&& other) {
  if (this != &other) {
    this->m_bufferImpl = other.m_bufferImpl;
    other.m_bufferImpl = nullptr;
  }
}

Buffer::Buffer(const Buffer& other) : m_bufferImpl(nullptr) {
  if (this != &other) {
    if (other.GetData() != nullptr) {
      std::unique_ptr<char, DeleterFuncPtr> data(new char[other.GetCapacity()], StandardDelete);
      memcpy(data.get(), other.GetData(), other.GetCapacity());      
      m_bufferImpl = new BufferImpl(std::move(data), other.GetLength(), other.GetCapacity());      
    }
  }
}

Buffer::Buffer(char* buffer, size_t bufferLengthInBytes,
  size_t bufferCapacityInBytes, DeleterFuncPtr customDeleterFunc) {
  if (buffer == nullptr && bufferLengthInBytes == 0 && bufferCapacityInBytes == 0) {
    // This is a special case, this kind of buffer is created by default ctor
    m_bufferImpl = nullptr;
  } else if (buffer == nullptr) {
    throw InvalidArgumentException("Argument buffer is nullptr whereas bufferLengthInBytes or bufferCapacityInBytes are greater than 0.",
      __FILE__, "", __LINE__);
  } else if (bufferLengthInBytes == 0 || bufferCapacityInBytes == 0) {
    throw InvalidArgumentException("Argument buffer is a valid pointer but bufferLengthInBytes or bufferCapacityInBytes are 0.",
      __FILE__, "", __LINE__);
  } else if (bufferCapacityInBytes < bufferLengthInBytes) {
    // Capacity cannot be less than length    
    throw InvalidArgumentException("Argument bufferCapacityInBytes cannot be less than bufferLengthInBytes.",
      __FILE__, "", __LINE__);
  } else if (customDeleterFunc == nullptr) {    
    throw InvalidArgumentException("Argument customDeleterFunc is nullptr.",
      __FILE__, "", __LINE__);
  } else {
    m_bufferImpl = new BufferImpl(std::unique_ptr<char, DeleterFuncPtr>(buffer, customDeleterFunc),
      bufferLengthInBytes, bufferCapacityInBytes);
  }
}

Buffer::Buffer(const char* buffer, size_t bufferLengthInBytes,
  size_t bufferCapacityInBytes) : m_bufferImpl(nullptr) {
  if (buffer == nullptr && bufferLengthInBytes == 0 && bufferCapacityInBytes == 0) {
    // This is a special case, this kind of buffer is created by default ctor
    m_bufferImpl = nullptr;
  } else if (buffer == nullptr) {
    throw InvalidArgumentException("Argument buffer is nullptr whereas bufferLengthInBytes or bufferCapacityInBytes are greater than 0.",
      __FILE__, "", __LINE__);
  } else if (bufferLengthInBytes == 0 || bufferCapacityInBytes == 0) {
    throw InvalidArgumentException("Argument buffer is a valid pointer but bufferLengthInBytes or bufferCapacityInBytes are 0.",
      __FILE__, "", __LINE__);
  } else if (bufferCapacityInBytes < bufferLengthInBytes) {
    // Capacity cannot be less than length    
    throw InvalidArgumentException("Argument bufferCapacityInBytes cannot be less than bufferLengthInBytes.",
      __FILE__, "", __LINE__);
  } else {
    std::unique_ptr<char, DeleterFuncPtr> data(new char[bufferCapacityInBytes], StandardDelete);
    memcpy(data.get(), buffer, bufferCapacityInBytes);
    m_bufferImpl = new BufferImpl(std::move(data), bufferLengthInBytes, bufferCapacityInBytes);    
  }
}

Buffer::~Buffer() {
  Reset();
}

Buffer& Buffer::operator=(const Buffer& other) {
  if (this != &other) {
    if (other.GetData() == nullptr) {
      Reset();
    } else {
      // We have to delete existing buffer, create a new buffer and then copy
      // First check if our existing buffer has the same capacity
      if (GetCapacity() != other.GetCapacity()) {
        std::unique_ptr<char, DeleterFuncPtr> data(new char[other.GetCapacity()], StandardDelete);
        memcpy(data.get(), other.GetData(), other.GetCapacity());
        // We have to delete existing buffer, create a new buffer and then copy
        Reset();
        m_bufferImpl = new BufferImpl(std::move(data), other.GetLength(), other.GetCapacity());
      } else {
        // Our capacity is the same so no need to reallocate the buffer
        memcpy(m_bufferImpl->BufferData.get(), other.GetData(), other.GetCapacity());
        m_bufferImpl->BufferLength = other.GetLength();
      }

    }
  }

  return *this;
}

Buffer& Buffer::operator=(Buffer&& other) {
  if (this != &other) {
    Reset();
    this->m_bufferImpl = other.m_bufferImpl;
    other.m_bufferImpl = nullptr;
  }

  return *this;
}

void Buffer::Copy(const char* buffer, size_t bufferLengthInBytes,
                  size_t bufferCapacityInBytes) {
  if (buffer == nullptr) {
    throw InvalidArgumentException("Argument buffer is nullptr.",
      __FILE__, "", __LINE__);  
  } else if (bufferCapacityInBytes < bufferLengthInBytes) {
    // Capacity cannot be less than length    
    throw InvalidArgumentException("Argument bufferCapacityInBytes cannot be less than bufferLengthInBytes.",
      __FILE__, "", __LINE__);
  } else {
    // First check if our existing buffer has the same capacity
    if (GetCapacity() != bufferCapacityInBytes) {
      std::unique_ptr<char, DeleterFuncPtr> data(new char[bufferCapacityInBytes], StandardDelete);      
      memcpy(data.get(), buffer, bufferCapacityInBytes);
      // We have to delete existing buffer, create a new buffer and then copy
      Reset();
      m_bufferImpl = new BufferImpl(std::move(data), bufferLengthInBytes, bufferCapacityInBytes);
    } else {
      // Our capacity is the same so no need to reallocate the buffer
      memcpy(m_bufferImpl->BufferData.get(), buffer, bufferCapacityInBytes);
      m_bufferImpl->BufferLength = bufferLengthInBytes;
    }
  }  
}

bool Buffer::operator<(const Buffer& other) const {
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

void Buffer::Resize(size_t newBufferCapacityInBytes) {
  if (newBufferCapacityInBytes == 0) {
    Reset();    
  } else if (newBufferCapacityInBytes == GetCapacity()) {
    return; // no op
  } else {    
      std::unique_ptr<char, DeleterFuncPtr> data(new char[newBufferCapacityInBytes], StandardDelete);
      Reset();
      m_bufferImpl = new BufferImpl(std::move(data), 0,
                                    newBufferCapacityInBytes);         
  }
}

void Buffer::Reset() {
  if (m_bufferImpl != nullptr) {
    delete m_bufferImpl;
    m_bufferImpl = nullptr;
  }
}

const char* Buffer::GetData() const {
  if (m_bufferImpl == nullptr) {
    return nullptr;
  } else {
    return m_bufferImpl->BufferData.get();
  }
}

char* Buffer::GetDataForWrite() {
  if (m_bufferImpl == nullptr) {
    return nullptr;
  } else {
    return m_bufferImpl->BufferData.get();
  }
}

const size_t Buffer::GetCapacity() const {
  if (m_bufferImpl == nullptr) {
    return 0;
  } else {
    return m_bufferImpl->BufferCapacity;
  }
}

const size_t Buffer::GetLength() const {
  if (m_bufferImpl == nullptr) {
    return 0;
  } else {
    return m_bufferImpl->BufferLength;
  }
}

void Buffer::SetLength(size_t value) {
  if (m_bufferImpl == nullptr) {
    if (value != 0) {
      throw JonoonDBException("Specified length is not between 0 and buffer capacity.",
                              __FILE__, "", __LINE__);
    }
  } else {
    if (value > m_bufferImpl->BufferCapacity) {      
      throw JonoonDBException("Specified length is not between 0 and buffer capacity.",
                              __FILE__, "", __LINE__);
    } else {
      m_bufferImpl->BufferLength = value;
    }
  }
}

void Buffer::Copy(const char* buffer, size_t bytesToCopy) {
  if (buffer == nullptr) {
    throw InvalidArgumentException("Argument buffer is nullptr.", __FILE__, "", __LINE__);
  } else if (bytesToCopy > 0) {
    // First check if our existing buffer is big enough
    if (GetCapacity() < bytesToCopy) {
      // Our internal buffer is not big enough
      ostringstream ss;
      ss << "Cannot copy " << bytesToCopy << " bytes into a buffer of capacity " << GetCapacity() << " bytes.";
      throw JonoonDBException(ss.str(), __FILE__, "", __LINE__);
    } else {
      // Our existing bufer is big enough, no need to reallocate
      memcpy(m_bufferImpl->BufferData.get(), buffer, bytesToCopy);
      m_bufferImpl->BufferLength = bytesToCopy;
    }
  }
}
