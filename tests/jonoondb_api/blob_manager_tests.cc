#include <gtest/gtest.h>
#include <memory>
#include <boost/filesystem.hpp>
#include "buffer_impl.h"
#include "blob_manager.h"
#include "filename_manager.h"
#include "blob_metadata.h"
#include "test_utils.h"

using namespace jonoondb_api;
using namespace jonoondb_test;

TEST(BlobManager, Constructor) {
  std::string dbName = "BlobManager_Constructor";
  std::string dbPath = g_TestRootDirectory;
  std::string collectionName = "Collection";
  boost::filesystem::path pathObj(g_TestRootDirectory);
  pathObj += "/" + dbName + "_" + collectionName + ".0";
  auto fileSize = 1024 * 1024;
  auto fnm = std::make_unique<FileNameManager>(dbPath, dbName, collectionName, true);
  BlobManager bm(move(fnm), false, fileSize, true);

  // make sure the file is there and it is the same size
  ASSERT_TRUE(boost::filesystem::exists(pathObj));
  auto actualSize = boost::filesystem::file_size(pathObj);
  ASSERT_EQ(actualSize, fileSize);  
}

TEST(BlobManager, Put) {
  std::string dbName = "BlobManager_Put";
  std::string dbPath = g_TestRootDirectory;  
  std::string collectionName = "Collection";
  auto fileSize = 1024 * 1024;
  auto fnm = std::make_unique<FileNameManager>(dbPath, dbName, collectionName, true);
  BlobManager bm(move(fnm), false, fileSize, true);
  std::string data = "This is the string!";
  BufferImpl buffer(data.c_str(), data.size(), data.size());
  BlobMetadata metadata;  
  bm.Put(buffer, metadata);
  ASSERT_EQ(metadata.fileKey, 0);
  ASSERT_EQ(metadata.offset, 0);  
}

TEST(BlobManager, Putx2) {
  std::string dbName = "BlobManager_Putx2";
  std::string dbPath = g_TestRootDirectory;
  std::string collectionName = "Collection";
  auto fileSize = 1024 * 1024;
  auto fnm = std::make_unique<FileNameManager>(dbPath, dbName, collectionName, true);
  BlobManager bm(move(fnm), false, fileSize, true);
  std::string data = "This is the string!";
  BufferImpl buffer(data.c_str(), data.size(), data.size());
  BlobMetadata metadata;
  bm.Put(buffer, metadata);
  ASSERT_EQ(metadata.fileKey, 0);
  ASSERT_EQ(metadata.offset, 0);

  // Do another put and make sure the offset changes
  bm.Put(buffer, metadata);  
  ASSERT_GT(metadata.offset, 0);
}

void ExecuteGetTest(const std::string& dbName, bool enableCompression) {
  std::string dbPath = g_TestRootDirectory;
  std::string collectionName = "Collection";
  auto fileSize = 1024 * 1024;
  auto fnm = std::make_unique<FileNameManager>(dbPath, dbName, collectionName, true);
  BlobManager bm(move(fnm), enableCompression, fileSize, true);
  std::string data = "This is the string!";
  BufferImpl buffer(data.c_str(), data.size(), data.size());
  BlobMetadata metadata;
  bm.Put(buffer, metadata);
  BufferImpl outBuffer;
  bm.Get(metadata, outBuffer);
  ASSERT_EQ(buffer.GetLength(), outBuffer.GetLength());
  ASSERT_EQ(memcmp(buffer.GetData(), outBuffer.GetData(), buffer.GetLength()), 0);

  // Add one more blob and retrieve/test it
  data = "This is the string2!";
  buffer.Resize(data.size());
  buffer.Copy(data.c_str(), data.size());
  bm.Put(buffer, metadata);
  bm.Get(metadata, outBuffer);
  ASSERT_EQ(buffer.GetLength(), outBuffer.GetLength());
  ASSERT_EQ(memcmp(buffer.GetData(), outBuffer.GetData(), buffer.GetLength()), 0);
}

TEST(BlobManager, Get) {
  std::string dbName = "BlobManager_Get";
  ExecuteGetTest(dbName, false);
}

TEST(BlobManager, Get_Compressed) {
  std::string dbName = "BlobManager_Get_Compressed";
  ExecuteGetTest(dbName, true);
}

void ExecuteMultiputTest(const std::string& dbName, bool enableCompression) {
  std::string dbPath = g_TestRootDirectory;
  std::string collectionName = "Collection";
  auto fileSize = 1024 * 1024;
  auto fnm = std::make_unique<FileNameManager>(dbPath, dbName, collectionName, true);
  BlobManager bm(move(fnm), enableCompression, fileSize, true);

  const int SIZE = 10;
  std::vector<BlobMetadata> metadataArray(SIZE);
  std::vector<BufferImpl> bufferArray;
  std::vector<const BufferImpl*> bufferPtrArray;
  std::string data;

  for (size_t i = 0; i < SIZE; i++) {
    data = "This is the string " + std::to_string(i);
    BufferImpl buf(data.c_str(), data.size(), data.size());
    bufferArray.push_back(buf);
  }

  for (auto& buf : bufferArray) {
    bufferPtrArray.push_back(&buf);
  }

  bm.MultiPut(bufferPtrArray, metadataArray);

  BufferImpl outBuffer;
  for (size_t i = 0; i < SIZE; i++) {
    data = "This is the string " + std::to_string(i);
    bm.Get(metadataArray[i], outBuffer);
    ASSERT_EQ(data.size(), outBuffer.GetLength());
    ASSERT_EQ(memcmp(data.data(), outBuffer.GetData(), outBuffer.GetLength()), 0);
  }
}

TEST(BlobManager, Multiput) {
  std::string dbName = "BlobManager_Multiput";
  ExecuteMultiputTest(dbName, false);
}

TEST(BlobManager, Multiput_Compressed) {
  std::string dbName = "BlobManager_Multiput_Compressed";
  ExecuteMultiputTest(dbName, true);
}

void ExecuteMultiput_SwitchFileTest(const std::string& dbName, bool enableCompression) {
  std::string dbPath = g_TestRootDirectory;
  std::string collectionName = "Collection";
  // Small file size to make sure we end up with multiple data files
  auto fileSize = 128;
  auto fnm = std::make_unique<FileNameManager>(dbPath, dbName, collectionName, true);
  BlobManager bm(move(fnm), false, fileSize, true);

  const int SIZE = 20;
  std::vector<BlobMetadata> metadataArray(SIZE);
  std::vector<BufferImpl> bufferArray;
  std::vector<const BufferImpl*> bufferPtrArray;
  std::string data;

  for (size_t i = 0; i < SIZE; i++) {
    data = "This is the string " + std::to_string(i);
    BufferImpl buf(data.c_str(), data.size(), data.size());
    bufferArray.push_back(buf);
  }

  for (auto& buf : bufferArray) {
    bufferPtrArray.push_back(&buf);
  }

  bm.MultiPut(bufferPtrArray, metadataArray);

  // Make sure that we wrote to multiple files
  bool hasMutipleFiles = false;
  for (auto& md : metadataArray) {
    if (md.fileKey > 1) {
      hasMutipleFiles = true;
      break;
    }
  }
  ASSERT_TRUE(hasMutipleFiles);

  BufferImpl outBuffer;
  for (size_t i = 0; i < SIZE; i++) {
    data = "This is the string " + std::to_string(i);
    bm.Get(metadataArray[i], outBuffer);
    ASSERT_EQ(data.size(), outBuffer.GetLength());
    ASSERT_EQ(memcmp(data.data(), outBuffer.GetData(), outBuffer.GetLength()), 0);
  }
}

TEST(BlobManager, Multiput_SwitchFile) {
  std::string dbName = "BlobManager_Multiput_SwitchFile";
  ExecuteMultiput_SwitchFileTest(dbName, false);
}

TEST(BlobManager, Multiput_SwitchFile_Compressed) {
  std::string dbName = "BlobManager_Multiput_SwitchFile";
  ExecuteMultiput_SwitchFileTest(dbName, true);
}