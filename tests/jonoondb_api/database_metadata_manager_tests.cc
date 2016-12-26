#include <string>
#include <memory>
#include <vector>
#include "gtest/gtest.h"
#include "test_utils.h"
#include "database_metadata_manager.h"
#include "index_info_impl.h"
#include "enums.h"

using namespace std;
using namespace jonoondb_api;
using namespace jonoondb_test;

TEST(DatabaseMetadataManager, Ctor_New) {
  string dbName = "DatabaseMetadataManager_Ctor_New";
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

    IndexInfoImpl indexInfo("MyIndex", IndexType::INVERTED_COMPRESSED_BITMAP, "MyColumn", true);
    std::vector<IndexInfoImpl*> vec;
    vec.push_back(&indexInfo);
    databaseMetadataManager->AddCollection("MyCollection", SchemaType::FLAT_BUFFERS, "", vec);    
  }); 
}

TEST(DatabaseMetadataManager, AddCollection_DiffCollSameIndexName) {
  string dbName = "DatabaseMetadataManager_AddCollection_DiffCollSameIndexName";
  string dbPath = g_TestRootDirectory;
  std::unique_ptr<DatabaseMetadataManager> databaseMetadataManager =
    std::make_unique<DatabaseMetadataManager>(g_TestRootDirectory, dbName, true);

  IndexInfoImpl indexInfo("MyIndex", IndexType::INVERTED_COMPRESSED_BITMAP, "MyColumn", true);
  std::vector<IndexInfoImpl*> indexes;
  indexes.push_back(&indexInfo);
  
  ASSERT_NO_THROW({    
  databaseMetadataManager->AddCollection("MyCollection", SchemaType::FLAT_BUFFERS, "", indexes);
  databaseMetadataManager->AddCollection("MyCollection2", SchemaType::FLAT_BUFFERS, "", indexes);
  });
}
