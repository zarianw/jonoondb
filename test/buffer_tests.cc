#include "gtest/gtest.h"
#include "buffer_impl.h"
#include "standard_deleters.h"
#include "jonoondb_exceptions.h"

using namespace std;
using namespace jonoondb_api;

BufferImpl GetBufferRValue(char* source, size_t length) {
  if (source == nullptr) {
    BufferImpl buffer;
    return buffer;
  } else {
    BufferImpl buffer(source, length, length, StandardDeleteNoOp);
    return buffer;
  }
}

TEST(BufferImpl, Buffer_DefaultConstructor) {
  BufferImpl buffer;

  ASSERT_TRUE(buffer.GetData() == nullptr);
  ASSERT_TRUE(buffer.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer.GetCapacity() == 0);
  ASSERT_TRUE(buffer.GetLength() == 0);
}

TEST(BufferImpl, Buffer_MoveConstructor) {
  BufferImpl buffer1;
  BufferImpl buffer2 = move(buffer1);  //invoke move ctor

  ASSERT_TRUE(buffer1.GetData() == nullptr);
  ASSERT_TRUE(buffer1.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer1.GetCapacity() == 0);
  ASSERT_TRUE(buffer1.GetLength() == 0);

  ASSERT_TRUE(buffer2.GetData() == nullptr);
  ASSERT_TRUE(buffer2.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer2.GetCapacity() == 0);
  ASSERT_TRUE(buffer2.GetLength() == 0);

  //Now invoke copy ctor on a buffer with some data

  BufferImpl buffer3;
  buffer3.Resize(100);

  BufferImpl buffer4 = move(buffer3);  //invoke move ctor

  //Content of buffer should be null and buffer4 should have the value
  ASSERT_TRUE(buffer3.GetData() == nullptr);
  ASSERT_TRUE(buffer3.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer3.GetCapacity() == 0);
  ASSERT_TRUE(buffer3.GetLength() == 0);

  ASSERT_TRUE(buffer4.GetData() != nullptr);
  ASSERT_TRUE(buffer4.GetDataForWrite() != nullptr);

  ASSERT_TRUE(buffer4.GetCapacity() == 100);
  ASSERT_TRUE(buffer4.GetLength() == 0);

  //Move ctor from a separate func
  BufferImpl buffer5 = GetBufferRValue(nullptr, 0);

  ASSERT_TRUE(buffer5.GetData() == nullptr);
  ASSERT_TRUE(buffer5.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer5.GetCapacity() == 0);
  ASSERT_TRUE(buffer5.GetLength() == 0);

  //Now invoke move ctor on a buffer with some data
  string str = "Hello";
  BufferImpl buffer6 = GetBufferRValue(const_cast<char*>(str.data()), str.length());

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

TEST(BufferImpl, Buffer_Resize) {
  BufferImpl buffer1;
  buffer1.Resize(200);

  ASSERT_TRUE(buffer1.GetData() != nullptr);
  ASSERT_TRUE(buffer1.GetDataForWrite() != nullptr);
  ASSERT_TRUE(buffer1.GetCapacity() == 200);
  ASSERT_TRUE(buffer1.GetLength() == 0);

  BufferImpl buffer2;
  buffer2.Resize(0);

  ASSERT_TRUE(buffer2.GetData() == nullptr);
  ASSERT_TRUE(buffer2.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer2.GetCapacity() == 0);
  ASSERT_TRUE(buffer2.GetLength() == 0);
}

TEST(BufferImpl, Buffer_AssignCtor_InvalidArguments) {
  string str = "Hello";
  //1. This is an error condition because we are saying take ownership but not providing the deleter
  ASSERT_THROW(BufferImpl buffer1(const_cast<char*>(str.data()), str.length(), str.capacity(), nullptr),
    InvalidArgumentException);

  //2. Valid condition.
  BufferImpl buffer2(const_cast<char*>(str.data()), str.length(),
    str.capacity(), StandardDeleteNoOp);
  

  ASSERT_TRUE(buffer2.GetData() != nullptr && buffer2.GetData() == str.data());
  ASSERT_TRUE(buffer2.GetDataForWrite() != nullptr && buffer2.GetDataForWrite() == str.data());

  ASSERT_TRUE(buffer2.GetCapacity() == str.capacity());
  ASSERT_TRUE(buffer2.GetLength() == str.length());

  ASSERT_TRUE(
      memcmp(buffer2.GetData(), str.data(), buffer2.GetCapacity()) == 0);
  ASSERT_TRUE(
      memcmp(buffer2.GetDataForWrite(), str.data(), buffer2.GetCapacity())
          == 0);

  //3. This is an error condition because we are specifying length < capacity
  ASSERT_THROW(BufferImpl buffer3(const_cast<char*>(str.data()), 20, 10, StandardDeleteNoOp), InvalidArgumentException);  

  //4. When capacity or length is 0 then an buffer should be nullptr
  ASSERT_THROW(BufferImpl buffer4(const_cast<char*>(str.data()), 0, 0, StandardDeleteNoOp), InvalidArgumentException);  
  
  //5. When nullptr buffer is nullptr length and capacity should be 0
  ASSERT_THROW(BufferImpl buffer5(nullptr, 20, 20, StandardDeleteNoOp), InvalidArgumentException);  

  // Now valid case
  BufferImpl buffer6(nullptr, 0, 0, StandardDeleteNoOp);
  ASSERT_TRUE(buffer6.GetData() == nullptr);
  ASSERT_TRUE(buffer6.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer6.GetCapacity() == 0);
  ASSERT_TRUE(buffer6.GetLength() == 0);
}

TEST(BufferImpl, Buffer_ManualCopyCtor_InvalidArguments) {
  string str = "Hello";
  
  // Valid condition.
  BufferImpl buffer2(const_cast<char*>(str.data()), str.length(), str.capacity());


  ASSERT_TRUE(buffer2.GetData() != nullptr && buffer2.GetData() != str.data());
  ASSERT_TRUE(buffer2.GetDataForWrite() != nullptr && buffer2.GetDataForWrite() != str.data());
  ASSERT_EQ(buffer2.GetCapacity(), str.capacity());
  ASSERT_EQ(buffer2.GetLength(), str.length());

  ASSERT_TRUE(
    memcmp(buffer2.GetData(), str.data(), buffer2.GetCapacity()) == 0);
  ASSERT_TRUE(
    memcmp(buffer2.GetDataForWrite(), str.data(), buffer2.GetCapacity())
    == 0);

  //3. This is an error condition because we are specifying length < capacity
  ASSERT_THROW(BufferImpl buffer3(const_cast<char*>(str.data()), 20, 10), InvalidArgumentException);

  //4. When capacity or length is 0 then an buffer should be nullptr
  ASSERT_THROW(BufferImpl buffer4(const_cast<char*>(str.data()), 0, 0), InvalidArgumentException);

  //5. When nullptr buffer is nullptr length and capacity should be 0
  ASSERT_THROW(BufferImpl buffer5(nullptr, 20, 20), InvalidArgumentException);

  // Now valid case
  BufferImpl buffer6(nullptr, 0, 0);
  ASSERT_TRUE(buffer6.GetData() == nullptr);
  ASSERT_TRUE(buffer6.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer6.GetCapacity() == 0);
  ASSERT_TRUE(buffer6.GetLength() == 0);
}

TEST(BufferImpl, Buffer_CopyCtor) {
  // Valid condition
  string str = "Hello";

  BufferImpl buffer1(const_cast<char*>(str.data()), str.length(), str.capacity());
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
}

TEST(BufferImpl, Buffer_Copy) {
  //1. Valid condition. Both string should copy successfully
  string str = "Hello";
  string strLong = "HelloWorld";
  int originalCapacity = 100;

  BufferImpl buffer1(const_cast<char*>(str.data()), str.length(), originalCapacity);
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
  buffer1.Copy(const_cast<char*>(strLong.data()), strLong.length());
  ASSERT_EQ(buffer1.GetData(), originalDataPtr);
  ASSERT_EQ(buffer1.GetDataForWrite(), originalDataPtr);

  ASSERT_EQ(buffer1.GetCapacity(), originalCapacity);
  ASSERT_EQ(buffer1.GetLength(), strLong.length());

  //2. Invalid Arguments
  BufferImpl buffer2;
  ASSERT_THROW(buffer2.Copy(nullptr, 2), InvalidArgumentException);  
}

TEST(BufferImpl, Buffer_Copy_SmallBuffer) {
  //1. Valid condition. Both string should copy successfully
  string str = "Hello";
  BufferImpl buffer(2);
  ASSERT_THROW(buffer.Copy(str.data(), str.length()), JonoonDBException);  
}

TEST(BufferImpl, Buffer_OutOfMemory) {
  //1. This is an error condition because we are specifying a very large value that should result in bad_alloc
  string str = "Hello";

  BufferImpl buffer1;
  // Todo: Uncomment this test once the assert dialog issue is fixed for windows 10
  //ASSERT_TRUE(
  //    buffer1.Copy(const_cast<char*>(str.data()), str.length(), ((size_t )-1))
  //        .OutOfMemoryError());  // 1 TB

  ASSERT_TRUE(buffer1.GetData() == nullptr);
  ASSERT_TRUE(buffer1.GetDataForWrite() == nullptr);
  ASSERT_TRUE(buffer1.GetCapacity() == 0);
  ASSERT_TRUE(buffer1.GetLength() == 0);
}
