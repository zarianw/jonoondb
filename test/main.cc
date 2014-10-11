#include "gtest/gtest.h"
#include "boost/exception/exception.hpp"
#include "boost/filesystem.hpp"
#include "test_utils.h"

std::string g_TestRootDirectory;

using namespace boost::filesystem;

bool SetUpDirectory(const char* directoryPath)
{
  try
  {
    auto a = remove_all(directoryPath);
    auto b = create_directory(directoryPath);
  }
  catch (boost::exception& ex)
  {
    // Print error
    return false;
  }

  return true;
}

int main(int argc, char **argv)
{
  auto tempPath = current_path();
  tempPath += "/unittests/";
  g_TestRootDirectory = tempPath.generic_string();
  SetUpDirectory(g_TestRootDirectory.c_str());
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}