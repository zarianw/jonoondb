#include <gtest/gtest.h>
#include <boost/filesystem.hpp>  // NOLINT
#include "buffer.h"
#include "status.h"
#include "memory_mapped_file.h"
#include "test_utils.h"

using namespace std;
using namespace boost::filesystem;
using namespace jonoondb_api;
using namespace jonoondb_test;

TEST(MemoryMappedFile, MemoryMappedFile_OpenExistingFile) {
  MemoryMappedFile* memMapFile;
  path pathObj(g_TestRootDirectory);
  pathObj += "MemoryMappedFile_OpenExistingFile";
  RemoveAndCreateFile(pathObj.string().c_str(), 1024);

  Status status = MemoryMappedFile::Open(pathObj.string().c_str(),
                                         MemoryMappedFileMode::ReadWrite, 0,
                                         false, &memMapFile);
  ASSERT_TRUE(status.OK());
}

TEST(MemoryMappedFile, MemoryMappedFile_OpenMissingFile) {
  MemoryMappedFile* memMapFile;
  path pathObj(g_TestRootDirectory);
  pathObj += "MemoryMappedFile_OpenMissingFile";

  Status status = MemoryMappedFile::Open(pathObj.string().c_str(),
                                         MemoryMappedFileMode::ReadWrite, 0,
                                         false, &memMapFile);
  ASSERT_TRUE(status.FileIOError());
}

