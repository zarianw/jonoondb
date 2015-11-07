#include <string>
#include <memory>
#include <vector>
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
  ASSERT_NO_THROW({
    std::unique_ptr<DatabaseMetadataManager> databaseMetadataManager =
    std::make_unique<DatabaseMetadataManager>(g_TestRootDirectory, dbName, true);
  });
}

TEST(DatabaseMetadataManager, AddCollection_NewSingleIndex) {
  string dbName = "DatabaseMetadataManager_AddCollection_NewSingleIndex";
  string dbPath = g_TestRootDirectory;
  ASSERT_NO_THROW({
    std::unique_ptr<DatabaseMetadataManager> databaseMetadataManager =
      std::make_unique<DatabaseMetadataManager>(g_TestRootDirectory, dbName, true);

    IndexInfoImpl indexInfo("MyIndex", IndexType::EWAHCompressedBitmap, "MyColumn", true);
    std::vector<IndexInfoImpl*> vec;
    vec.push_back(&indexInfo);
    databaseMetadataManager->AddCollection("MyCollection", SchemaType::FLAT_BUFFERS, "", vec);    
  }); 
}
