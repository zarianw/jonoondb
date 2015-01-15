#include <string>
#include "gtest/gtest.h"
#include "test_utils.h"
#include "database.h"
#include "status.h"
#include "index_info.h"
#include "enums.h"
#include "options.h"

using namespace std;
using namespace jonoondb_api;
using namespace jonoondb_test;

TEST(Database, Open_NullArguments) {
  Options options;
  Database* db = nullptr;

  Status sts = Database::Open(nullptr, "", options, db);
  ASSERT_TRUE(sts.InvalidArgument());

  sts = Database::Open("", nullptr, options, db);
  ASSERT_TRUE(db == nullptr);
  ASSERT_TRUE(sts.InvalidArgument());
}

TEST(Database, Open_MissingDatabaseFile) {
  Options options;
  Database* db = nullptr;

  Status sts = Database::Open(g_TestRootDirectory.c_str(), "name", options, db);
  ASSERT_TRUE(db == nullptr);
  ASSERT_TRUE(sts.MissingDatabaseFile());
}

TEST(Database, Open_New) {
  string dbName = "Database_Open_New";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  auto status = Database::Open(g_TestRootDirectory.c_str(), dbName.c_str(),
                               options, db);
  ASSERT_TRUE(status.OK());
  ASSERT_TRUE(db->Close().OK());
}

TEST(Database, Open_Existing) {
  string dbName = "Database_Open_Existing";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  auto status = Database::Open(g_TestRootDirectory.c_str(), dbName.c_str(),
                               options, db);
  ASSERT_TRUE(status.OK());
  ASSERT_TRUE(db->Close().OK());

  status = Database::Open(g_TestRootDirectory.c_str(), dbName.c_str(), options,
                          db);
  ASSERT_TRUE(status.OK());
}

TEST(Database, Open_CreateIfMissing) {
  //First remove the file if it exists
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  Status sts = Database::Open(g_TestRootDirectory.c_str(),
                              "Database_Open_CreateIfMissing", options, db);
  ASSERT_TRUE(db != nullptr);
  ASSERT_TRUE(sts.OK());
  ASSERT_TRUE(db->Close().OK());  //Checking if database closed successfully			
}

TEST(Database, CreateCollection_New) {
  string dbName = "CreateCollection_New";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  auto sts = Database::Open(dbPath.c_str(), dbName.c_str(), options, db);
  ASSERT_TRUE(sts.OK());
  sts = db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS,
                             "Schema IDL", nullptr, 0);
  ASSERT_TRUE(sts.OK());
  ASSERT_TRUE(db->Close().OK());
}

TEST(Database, CreateCollection_CollectionAlreadyExist) {
  string dbName = "CreateCollection_CollectionAlreadyExist";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  auto sts = Database::Open(dbPath.c_str(), dbName.c_str(), options, db);
  ASSERT_TRUE(sts.OK());
  sts = db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS,
                             "Schema IDL", nullptr, 0);
  ASSERT_TRUE(sts.OK());
  sts = db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS,
                             "Schema IDL", nullptr, 0);
  ASSERT_TRUE(sts.CollectionAlreadyExist());
  ASSERT_TRUE(db->Close().OK());
}
