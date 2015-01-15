#include <string>
#include "gtest/gtest.h"
#include "test_utils.h"
#include "database_metadata_manager.h"
#include "status.h"
#include "index_info.h"
#include "enums.h"

using namespace std;
using namespace jonoondb_api;
using namespace jonoondb_test;

TEST(DatabaseMetadataManager, Open_New) {
  string dbName = "DatabaseMetadataManager_Open_New";
  string dbPath = g_TestRootDirectory;
  DatabaseMetadataManager* mgr;
  auto status = DatabaseMetadataManager::Open(g_TestRootDirectory.c_str(),
                                              dbName.c_str(), true, mgr);
  ASSERT_TRUE(status.OK());
}

TEST(DatabaseMetadataManager, AddCollection_NewSingleIndex) {
  string dbName = "DatabaseMetadataManager_AddCollection_NewSingleIndex";
  string dbPath = g_TestRootDirectory;
  DatabaseMetadataManager* mgr;
  auto status = DatabaseMetadataManager::Open(g_TestRootDirectory.c_str(),
                                              dbName.c_str(), true, mgr);
  ASSERT_TRUE(status.OK());
  const char* columns[] = { "MyColumn" };
  IndexInfo indexInfo(string("MyIndex").c_str(),
                      IndexType::EWAHCompressedBitmap, columns, 1, true);
  status = mgr->AddCollection("MyCollection", SchemaType::FLAT_BUFFERS, "",
                              &indexInfo, 1);
  ASSERT_TRUE(status.OK());
}
