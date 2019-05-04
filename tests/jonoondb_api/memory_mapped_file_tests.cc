#include <gtest/gtest.h>
#include <boost/filesystem.hpp>
#include "buffer_impl.h"
#include "memory_mapped_file.h"
#include "test_utils.h"

using namespace std;
using namespace boost::filesystem;
using namespace jonoondb_api;
using namespace jonoondb_test;

TEST(MemoryMappedFile, MemoryMappedFile_OpenExistingFile) {
  path pathObj(g_TestRootDirectory);
  pathObj += "MemoryMappedFile_OpenExistingFile";
  RemoveAndCreateFile(pathObj.string().c_str(), 1024);

  MemoryMappedFile mmFile(pathObj.string(), MemoryMappedFileMode::ReadWrite, 0,
                          false);
}

TEST(MemoryMappedFile, MemoryMappedFile_OpenMissingFile) {
  path pathObj(g_TestRootDirectory);
  pathObj += "MemoryMappedFile_OpenMissingFile";

  ASSERT_ANY_THROW({
    MemoryMappedFile mmFile(pathObj.string(), MemoryMappedFileMode::ReadWrite,
                            0, false);
  });
}
