#include "gtest/gtest.h"
#include "buffer.h"
#include "standard_deleters.h"
#include "status.h"

using namespace std;
using namespace jonoondb_api;

Buffer GetBufferRValue(char* source, size_t length) {
  if (source == nullptr) {
    Buffer buffer;
    return buffer;
  } else {
    Buffer buffer;
    buffer.Assign(source, length, length, StandardDeleteNoOp);
    return buffer;
  }
}

TEST(Buffer, Buffer_DefaultConstructor) {
  Buffer buffer;

  ASSERT_TRUE(buffer.GetData() == nullptr);
  ASSERT_TRUE(buffer.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer.GetCapacity() == 0);
  ASSERT_TRUE(buffer.GetLength() == 0);
}

TEST(Buffer, Buffer_MoveConstructor) {
  Buffer buffer1;
  Buffer buffer2 = move(buffer1);  //invoke move ctor

  ASSERT_TRUE(buffer1.GetData() == nullptr);
  ASSERT_TRUE(buffer1.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer1.GetCapacity() == 0);
  ASSERT_TRUE(buffer1.GetLength() == 0);

  ASSERT_TRUE(buffer2.GetData() == nullptr);
  ASSERT_TRUE(buffer2.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer2.GetCapacity() == 0);
  ASSERT_TRUE(buffer2.GetLength() == 0);

  //Now invoke copy ctor on a buffer with some data

  Buffer buffer3;
  buffer3.Resize(100);

  Buffer buffer4 = move(buffer3);  //invoke copy ctor

  //Content of buffer should be null and buffer4 should have the value
  ASSERT_TRUE(buffer3.GetData() == nullptr);
  ASSERT_TRUE(buffer3.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer3.GetCapacity() == 0);
  ASSERT_TRUE(buffer3.GetLength() == 0);

  ASSERT_TRUE(buffer4.GetData() != nullptr);
  ASSERT_TRUE(buffer4.GetDataForWrite() != nullptr);

  ASSERT_TRUE(buffer4.GetCapacity() == 100);
  ASSERT_TRUE(buffer4.GetLength() == 100);

  //Move ctor from a separate func
  Buffer buffer5 = GetBufferRValue(nullptr, 0);

  ASSERT_TRUE(buffer5.GetData() == nullptr);
  ASSERT_TRUE(buffer5.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer5.GetCapacity() == 0);
  ASSERT_TRUE(buffer5.GetLength() == 0);

  //Now invoke move ctor on a buffer with some data
  string str = "Hello";
  Buffer buffer6 = GetBufferRValue(const_cast<char*>(str.data()), str.length());

  ASSERT_TRUE(buffer6.GetData() != nullptr && buffer6.GetData() == str.data());
  ASSERT_TRUE(
      buffer6.GetDataForWrite() != nullptr
          && buffer6.GetDataForWrite() == str.data());

  ASSERT_TRUE(buffer6.GetCapacity() == str.length());
  ASSERT_TRUE(buffer6.GetLength() == str.length());

  ASSERT_TRUE(
      memcmp(buffer6.GetData(), str.data(), buffer6.GetCapacity()) == 0);
  ASSERT_TRUE(
      memcmp(buffer6.GetDataForWrite(), str.data(), buffer6.GetCapacity())
          == 0);
}

TEST(Buffer, Buffer_Resize) {
  Buffer buffer1;
  buffer1.Resize(200);

  ASSERT_TRUE(buffer1.GetData() != nullptr);
  ASSERT_TRUE(buffer1.GetDataForWrite() != nullptr);
  ASSERT_TRUE(buffer1.GetCapacity() == 200);
  ASSERT_TRUE(buffer1.GetLength() == 200);

  Buffer buffer2;
  buffer2.Resize(0);

  ASSERT_TRUE(buffer2.GetData() == nullptr);
  ASSERT_TRUE(buffer2.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer2.GetCapacity() == 0);
  ASSERT_TRUE(buffer2.GetLength() == 0);
}

TEST(Buffer, Buffer_Assign) {
  string str = "Hello";
  //1. This is an error condition because we are saying take ownership but not providing the deleter
  Buffer buffer1;
  ASSERT_TRUE(
      buffer1.Assign(const_cast<char*>(str.data()), str.length(),
                     str.capacity(), nullptr).InvalidArgument());

  //Check if the contents of the buffer are still unchanged
  ASSERT_TRUE(buffer1.GetData() == nullptr);
  ASSERT_TRUE(buffer1.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer1.GetCapacity() == 0);
  ASSERT_TRUE(buffer1.GetLength() == 0);

  //2. Valid condition.
  Buffer buffer2;
  ASSERT_TRUE(
      buffer2.Assign(const_cast<char*>(str.data()), str.length(),
                     str.capacity(), StandardDeleteNoOp).OK());

  ASSERT_TRUE(buffer2.GetData() != nullptr && buffer2.GetData() == str.data());
  ASSERT_TRUE(
      buffer2.GetDataForWrite() != nullptr
          && buffer2.GetDataForWrite() == str.data());

  ASSERT_TRUE(buffer2.GetCapacity() == str.capacity());
  ASSERT_TRUE(buffer2.GetLength() == str.length());

  ASSERT_TRUE(
      memcmp(buffer2.GetData(), str.data(), buffer2.GetCapacity()) == 0);
  ASSERT_TRUE(
      memcmp(buffer2.GetDataForWrite(), str.data(), buffer2.GetCapacity())
          == 0);

  //3. This is an error condition because we are specifying length < capacity
  Buffer buffer3;
  ASSERT_TRUE(
      buffer3.Assign(const_cast<char*>(str.data()), 20, 10, StandardDeleteNoOp)
          .InvalidArgument());

  ASSERT_TRUE(buffer3.GetData() == nullptr);
  ASSERT_TRUE(buffer3.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer3.GetCapacity() == 0);
  ASSERT_TRUE(buffer3.GetLength() == 0);

  //4. Valid. When capacity is 0 then an empty buffer is assigned
  Buffer buffer4;
  ASSERT_TRUE(
      buffer4.Assign(const_cast<char*>(str.data()), 0, 0, StandardDeleteNoOp).OK());

  ASSERT_TRUE(buffer4.GetData() == nullptr);
  ASSERT_TRUE(buffer4.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer4.GetCapacity() == 0);
  ASSERT_TRUE(buffer4.GetLength() == 0);

  //5. Valid. When nullptr buffer is specified then an empty buffer is assigned
  Buffer buffer5;
  ASSERT_TRUE(buffer5.Assign(nullptr, 20, 20, StandardDeleteNoOp).OK());

  ASSERT_TRUE(buffer5.GetData() == nullptr);
  ASSERT_TRUE(buffer5.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer5.GetCapacity() == 0);
  ASSERT_TRUE(buffer5.GetLength() == 0);
}

TEST(Buffer, Buffer_Copy_Exact) {
  //1. Valid condition
  string str = "Hello";

  Buffer buffer1;
  ASSERT_TRUE(
      buffer1.Copy(const_cast<char*>(str.data()), str.length(), str.capacity())
          .OK());

  ASSERT_TRUE(buffer1.GetData() != nullptr && buffer1.GetData() != str.data());
  ASSERT_TRUE(
      buffer1.GetDataForWrite() != nullptr
          && buffer1.GetDataForWrite() != str.data());

  ASSERT_TRUE(buffer1.GetCapacity() == str.capacity());
  ASSERT_TRUE(buffer1.GetLength() == str.length());

  ASSERT_TRUE(
      memcmp(buffer1.GetData(), str.data(), buffer1.GetCapacity()) == 0);
  ASSERT_TRUE(
      memcmp(buffer1.GetDataForWrite(), str.data(), buffer1.GetCapacity())
          == 0);

  //2. Invalid Arguments
  Buffer buffer2;
  ASSERT_TRUE(buffer2.Copy(nullptr, 2, 2).InvalidArgument());
  ASSERT_TRUE(
      buffer2.Copy(const_cast<char*>(str.data()), 4, 2).InvalidArgument());

  //3. A valid buffer should be reset when capacity is 0
  Buffer buffer3;
  ASSERT_TRUE(
      buffer3.Copy(const_cast<char*>(str.data()), str.length(), str.capacity())
          .OK());
  ASSERT_TRUE(buffer3.GetData() != nullptr && buffer3.GetData() != str.data());
  ASSERT_TRUE(
      buffer3.GetDataForWrite() != nullptr
          && buffer3.GetDataForWrite() != str.data());

  ASSERT_TRUE(buffer3.Copy(const_cast<char*>(str.data()), 0, 0).OK());
  ASSERT_TRUE(buffer3.GetData() == nullptr);
  ASSERT_TRUE(buffer3.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer3.GetCapacity() == 0);
  ASSERT_TRUE(buffer3.GetLength() == 0);

  //4. No reallocation should happen when the capacity is of the same size
  Buffer buffer4;
  ASSERT_TRUE(
      buffer4.Copy(const_cast<char*>(str.data()), str.length(), str.capacity())
          .OK());
  const char* internalDataPtr = buffer4.GetData();
  size_t capacity = buffer4.GetCapacity();

  ASSERT_TRUE(
      buffer4.Copy(const_cast<char*>(str.data()), str.length(), str.capacity())
          .OK());

  ASSERT_TRUE(
      buffer4.GetData() != nullptr && buffer4.GetData() == internalDataPtr);
  ASSERT_TRUE(
      buffer4.GetDataForWrite() != nullptr
          && buffer4.GetDataForWrite() == internalDataPtr);
  ASSERT_TRUE(buffer4.GetCapacity() == capacity);
}

TEST(Buffer, Buffer_Copy_Bytes) {
  //1. Valid condition. Reallocation should not happen if the buffer is big enough
  string str = "Hello";
  string strLong = "HelloWorld";
  int originalCapacity = 100;

  Buffer buffer1;
  ASSERT_TRUE(
      buffer1.Copy(const_cast<char*>(str.data()), str.length(),
                   originalCapacity).OK());

  const char* originalDataPtr = buffer1.GetData();

  ASSERT_TRUE(buffer1.GetData() != nullptr && buffer1.GetData() != str.data());
  ASSERT_TRUE(
      buffer1.GetDataForWrite() != nullptr
          && buffer1.GetDataForWrite() != str.data());

  ASSERT_TRUE(buffer1.GetCapacity() == originalCapacity);
  ASSERT_TRUE(buffer1.GetLength() == str.length());

  ASSERT_TRUE(memcmp(buffer1.GetData(), str.data(), buffer1.GetLength()) == 0);
  ASSERT_TRUE(
      memcmp(buffer1.GetDataForWrite(), str.data(), buffer1.GetLength()) == 0);

  //Now do a copy for strLong
  ASSERT_TRUE(
      buffer1.Copy(const_cast<char*>(strLong.data()), strLong.length()).OK());
  ASSERT_TRUE(buffer1.GetData() == originalDataPtr);
  ASSERT_TRUE(buffer1.GetDataForWrite() == originalDataPtr);

  ASSERT_TRUE(buffer1.GetCapacity() == originalCapacity);
  ASSERT_TRUE(buffer1.GetLength() == strLong.length());

  //2. Invalid Arguments
  Buffer buffer2;
  ASSERT_TRUE(buffer2.Copy(nullptr, 2).InvalidArgument());
}

TEST(Buffer, Buffer_OutOfMemory) {
  //1. This is an error condition because we are specifying a very large value that should result in bad_alloc
  string str = "Hello";

  Buffer buffer1;
  ASSERT_TRUE(
      buffer1.Copy(const_cast<char*>(str.data()), str.length(), ((size_t )-1))
          .OutOfMemoryError());  // 1 TB

  ASSERT_TRUE(buffer1.GetData() == nullptr);
  ASSERT_TRUE(buffer1.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer1.GetCapacity() == 0);
  ASSERT_TRUE(buffer1.GetLength() == 0);
}
