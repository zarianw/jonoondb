#include <boost/filesystem.hpp>
#include "gtest/gtest.h"
#include "test_utils.h"
#include "filename_manager.h"
#include "jonoondb_exceptions.h"
#include "file_info.h"

using namespace std;
using namespace jonoondb_api;
using namespace jonoondb_test;

TEST(FileNameManager, Initialize_MissingDatabaseFile) {
  string dbName = "FileNameManager_Initialize_MissingDatabaseFile";
  string dbPath = g_TestRootDirectory;
  string collectionName = "Collection";
  ASSERT_THROW(FileNameManager fileNameManager(dbPath, dbName, collectionName, false), MissingDatabaseFileException);
}

TEST(FileNameManager, GetCurrentDataFileInfo) {
  string dbName = "FileNameManager_GetCurrentDataFileInfo";
  string dbPath = g_TestRootDirectory;
  string collectionName = "Collection";
  FileNameManager fileNameManager(dbPath, dbName, collectionName, true);

  FileInfo fileInfo;
  fileNameManager.GetCurrentDataFileInfo(true, fileInfo);
  ASSERT_EQ(fileInfo.fileKey, 0);
  ASSERT_STREQ(fileInfo.fileName.c_str(), "FileNameManager_GetCurrentDataFileInfo_Collection.0");
  std::string completePath = dbPath + "FileNameManager_GetCurrentDataFileInfo_Collection.0";
  ASSERT_STREQ(fileInfo.fileNameWithPath.c_str(), completePath.c_str());

  //Again get the same thing and make sure we get the same file
  FileInfo fileInfo2;
  fileNameManager.GetCurrentDataFileInfo(true, fileInfo2);
  ASSERT_EQ(fileInfo.fileKey, fileInfo2.fileKey);
  ASSERT_STREQ(fileInfo.fileName.c_str(), fileInfo2.fileName.c_str());
  ASSERT_STREQ(fileInfo.fileNameWithPath.c_str(), fileInfo2.fileNameWithPath.c_str());
}

TEST(FileNameManager, GetNextDataFileInfo) {
  string dbName = "FileNameManager_GetNextDataFileInfo";
  string dbPath = g_TestRootDirectory;
  string collectionName = "Collection";
  FileNameManager fileNameManager(dbPath, dbName, collectionName, true);

  FileInfo fileInfo;
  string fileNamePattern = "FileNameManager_GetNextDataFileInfo_Collection.%d";
  string fileNameWithPathPattern = (boost::filesystem::path(dbPath) / fileNamePattern).generic_string();
  char fileName[200];
  char fileNameWithPath[1024];

  //Add the first FileInfo record
  fileNameManager.GetCurrentDataFileInfo(true, fileInfo);

  int count = 5;

  //Get next file in a loop
  for (int i = 1; i < count; i++) {
    fileNameManager.GetNextDataFileInfo(fileInfo);

    ASSERT_EQ(fileInfo.fileKey, i);

    sprintf(fileName, fileNamePattern.c_str(), i);
    sprintf(fileNameWithPath, fileNameWithPathPattern.c_str(), i);

    ASSERT_STREQ(fileInfo.fileName.c_str(), fileName);
    ASSERT_STREQ(fileInfo.fileNameWithPath.c_str(), fileNameWithPath);
  }
}

TEST(FileNameManager, GetCurrentAndNextFileInfoCombined) {
  string dbName = "FileNameManager_GetCurrentAndNextFileInfoCombined";
  string dbPath = g_TestRootDirectory;
  string collectionName = "Collection";
  FileNameManager fileNameManager(dbPath, dbName, collectionName, true);

  FileInfo fileInfo;
  fileNameManager.GetCurrentDataFileInfo(true, fileInfo);

  ASSERT_EQ(fileInfo.fileKey, 0);
  ASSERT_STREQ(fileInfo.fileName.c_str(), "FileNameManager_GetCurrentAndNextFileInfoCombined_Collection.0");
  boost::filesystem::path completePath(dbPath);
  completePath /= "FileNameManager_GetCurrentAndNextFileInfoCombined_Collection.0";
  ASSERT_STREQ(fileInfo.fileNameWithPath.c_str(), completePath.generic_string().c_str());

  //Again get the same thing and make sure we get the same file
  FileInfo fileInfo2;
  fileNameManager.GetCurrentDataFileInfo(true, fileInfo2);
  ASSERT_EQ(fileInfo.fileKey, fileInfo2.fileKey);
  ASSERT_STREQ(fileInfo.fileName.c_str(), fileInfo2.fileName.c_str());
  ASSERT_STREQ(fileInfo.fileNameWithPath.c_str(), fileInfo2.fileNameWithPath.c_str());

  string fileNamePattern = "FileNameManager_GetCurrentAndNextFileInfoCombined_Collection.%d";
  string fileNameWithPathPattern = (completePath.parent_path() / fileNamePattern).generic_string();
  char fileName[200];
  char fileNameWithPath[1024];

  //Get next file in a loop
  for (int i = 1; i < 11; i++) {
    fileNameManager.GetNextDataFileInfo(fileInfo);

    ASSERT_EQ(fileInfo.fileKey, i);

    sprintf(fileName, fileNamePattern.c_str(), i);
    sprintf(fileNameWithPath, fileNameWithPathPattern.c_str(), i);

    ASSERT_STREQ(fileInfo.fileName.c_str(), fileName);
    ASSERT_STREQ(fileInfo.fileNameWithPath.c_str(), fileNameWithPath);
  }
}

TEST(FileNameManager, GetCurrentAndNextFileInfoCombined_NoEndSlash) {
  string dbName = "FileNameManager_GetCurrentAndNextFileInfoCombined_NoEndSlash";
  string dbPath = g_TestRootDirectory.substr(0, g_TestRootDirectory.size() - 1);
  string collectionName = "Collection";
  FileNameManager fileNameManager(dbPath, dbName, collectionName, true);

  FileInfo fileInfo;
  fileNameManager.GetCurrentDataFileInfo(true, fileInfo);

  ASSERT_EQ(fileInfo.fileKey, 0);
  ASSERT_STREQ(fileInfo.fileName.c_str(), "FileNameManager_GetCurrentAndNextFileInfoCombined_NoEndSlash_Collection.0");
  boost::filesystem::path completePath(dbPath);
  completePath /= "FileNameManager_GetCurrentAndNextFileInfoCombined_NoEndSlash_Collection.0";
  ASSERT_STREQ(fileInfo.fileNameWithPath.c_str(), completePath.generic_string().c_str());

  //Again get the same thing and make sure we get the same file
  FileInfo fileInfo2;
  fileNameManager.GetCurrentDataFileInfo(true, fileInfo2);
  ASSERT_EQ(fileInfo.fileKey, fileInfo2.fileKey);
  ASSERT_STREQ(fileInfo.fileName.c_str(), fileInfo2.fileName.c_str());
  ASSERT_STREQ(fileInfo.fileNameWithPath.c_str(), fileInfo2.fileNameWithPath.c_str());

  string fileNamePattern = "FileNameManager_GetCurrentAndNextFileInfoCombined_NoEndSlash_Collection.%d";
  string fileNameWithPathPattern = (completePath.parent_path() / fileNamePattern).generic_string();
  char fileName[200];
  char fileNameWithPath[1024];

  //Get next file in a loop
  for (int i = 1; i < 11; i++) {
    fileNameManager.GetNextDataFileInfo(fileInfo);

    ASSERT_EQ(fileInfo.fileKey, i);

    sprintf(fileName, fileNamePattern.c_str(), i);
    sprintf(fileNameWithPath, fileNameWithPathPattern.c_str(), i);

    ASSERT_STREQ(fileInfo.fileName.c_str(), fileName);
    ASSERT_STREQ(fileInfo.fileNameWithPath.c_str(), fileNameWithPath);
  }
}

TEST(FileNameManager, GetDataFileInfo) {
  string dbName = "FileNameManager_GetDataFileInfo";
  string dbPath = g_TestRootDirectory;
  string collectionName = "Collection";
  FileNameManager fileNameManager(dbPath, dbName, collectionName, true);

  FileInfo fileInfo;
  string fileNamePattern = "FileNameManager_GetDataFileInfo_Collection.%d";
  string fileNameWithPathPattern = (boost::filesystem::path(dbPath) / fileNamePattern).generic_string();
  char fileName[200];
  char fileNameWithPath[1024];

  //Add the first FileInfo record
  fileNameManager.GetCurrentDataFileInfo(true, fileInfo);

  int count = 5;

  //Insert some data
  for (int i = 1; i < count; i++) {
    fileNameManager.GetNextDataFileInfo(fileInfo);
  }

  //Now retrieve these file info objects
  shared_ptr<FileInfo> fileInfoOut(new FileInfo());
  for (int i = 0; i < count; i++) {
    fileNameManager.GetFileInfo(i, fileInfoOut);

    ASSERT_EQ(fileInfoOut->fileKey, i);

    sprintf(fileName, fileNamePattern.c_str(), i);
    sprintf(fileNameWithPath, fileNameWithPathPattern.c_str(), i);

    ASSERT_STREQ(fileInfoOut->fileName.c_str(), fileName);
    ASSERT_STREQ(fileInfoOut->fileNameWithPath.c_str(), fileNameWithPath);
  }
}