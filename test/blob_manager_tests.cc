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
  boost::filesystem::path pathObj(g_TestRootDirectory);
  pathObj += "BlobManager_Constructor.0";
  auto fileSize = 1024 * 1024;
  auto fnm = std::make_unique<FileNameManager>(dbPath, dbName, true);
  BlobManager bm(move(fnm), false, fileSize, true);

  // make sure the file is there and it is the same size
  ASSERT_TRUE(boost::filesystem::exists(pathObj));
  auto actualSize = boost::filesystem::file_size(pathObj);
  ASSERT_EQ(actualSize, fileSize);  
}

TEST(BlobManager, Put) {
  std::string dbName = "BlobManager_Put";
  std::string dbPath = g_TestRootDirectory;  
  auto fileSize = 1024 * 1024;
  auto fnm = std::make_unique<FileNameManager>(dbPath, dbName, true);
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
  auto fileSize = 1024 * 1024;
  auto fnm = std::make_unique<FileNameManager>(dbPath, dbName, true);
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
