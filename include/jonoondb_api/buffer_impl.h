#pragma once

#include <memory>

namespace jonoondb_api {
class BufferImpl {
 public:
  BufferImpl();
  BufferImpl(size_t capacity);
  BufferImpl(const char* buffer, size_t bufferLengthInBytes,
             size_t bufferCapacityInBytes);
  BufferImpl(char* buffer, size_t bufferLengthInBytes,
             size_t bufferCapacityInBytes, void (*customDeleterFunc)(char*));
  BufferImpl(BufferImpl&& other) noexcept;
  BufferImpl(const BufferImpl& other);
  BufferImpl& operator=(const BufferImpl& other);
  BufferImpl& operator=(BufferImpl&& other) noexcept;
  bool operator<(const BufferImpl& other) const;
  bool operator<=(const BufferImpl& other) const;
  bool operator>(const BufferImpl& other) const;
  bool operator>=(const BufferImpl& other) const;
  bool operator==(const BufferImpl& other) const;
  bool operator!=(const BufferImpl& other) const;
  void Resize(size_t newBufferCapacityInBytes);

  const char* GetData() const;
  char* GetDataForWrite();
  const size_t GetCapacity() const;
  const size_t GetLength() const;
  void SetLength(size_t val);

  void Copy(const char* buffer, size_t bytesToCopy);

 private:
  std::unique_ptr<char, void (*)(char*)> m_buffer;
  size_t m_length;
  size_t m_capacity;
};
}  // namespace jonoondb_api
