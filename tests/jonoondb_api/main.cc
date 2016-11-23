#include <iostream>
#include <exception>
#include <gtest/gtest.h>
#include <cstdio>
#include <string>
#include <memory>
#include <fstream>
#include <streambuf>
#include <boost/exception/exception.hpp>
#include <boost/exception_ptr.hpp>
#include <boost/filesystem.hpp>
#include "test_utils.h"
#include "test/test_config_generated.h"

using namespace std;
using namespace boost::filesystem;
using namespace jonoondb_test;

namespace jonoondb_test {
string g_TestRootDirectory;
string g_ResourcesFolderPath;

std::string GetSchemaFilePath(const std::string& fileName) {
  return g_ResourcesFolderPath + "/jonoondb_api_test/" + fileName;
}

void RemoveAndCreateFile(const char* path, size_t fileSize) {
  std::remove(path);
  std::unique_ptr<FILE, int (*)(FILE*)> file(std::fopen(path, "wb"),
                                             std::fclose);  // unique_ptr will call fclose on destruction
  std::unique_ptr<char> buffer(new char[fileSize]);
  std::fwrite(buffer.get(), sizeof(char), fileSize, file.get());
}
}  // namespace jonoondb_test

bool SetUpDirectory(const char* directoryPath) {
  try {
    auto a = remove_all(directoryPath);
    auto b = create_directory(directoryPath);
  } catch (boost::exception& ex) {
    cout
        << "Error occured while trying to remove and recreate the unit test folder at path "
        << directoryPath << endl;
    cout << "Error: " << diagnostic_information(ex) << endl;
    return false;
  } catch (std::exception& ex) {
    cout
        << "Error occured while trying to remove and recreate the unit test folder at path "
        << directoryPath << endl;
    cout << "Error: " << ex.what() << endl;
    return false;
  }

  return true;
}

int main(int argc, char** argv) {
  path tempPath = TEST_FOLDER_PATH;
  g_TestRootDirectory = tempPath.generic_string();
  tempPath = RESOURCES_FOLDER_PATH;
  g_ResourcesFolderPath = tempPath.generic_string();
  if (SetUpDirectory(g_TestRootDirectory.c_str())) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
  } else {
    return -1;
  }
}
