#include <string>
#include "gtest/gtest.h"
#include "flatbuffers/flatbuffers.h"
#include "test_utils.h"
#include "database.h"
#include "status.h"
#include "index_info.h"
#include "enums.h"
#include "options.h"
#include "buffer.h"
#include "schemas/flatbuffers/tweet_generated.h"
#include "schemas/flatbuffers/all_field_type_generated.h"
#include "resultset.h"

using namespace std;
using namespace flatbuffers;
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

TEST(Database, CreateCollection_InvalidSchema) {
  string dbName = "CreateCollection_New";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  auto sts = Database::Open(dbPath.c_str(), dbName.c_str(), options, db);
  ASSERT_TRUE(sts.OK());
  sts = db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS,
                             "Schema IDL", nullptr, 0);
  ASSERT_TRUE(sts.SchemaParseError());
  ASSERT_TRUE(db->Close().OK());
}

TEST(Database, CreateCollection_New) {
  string dbName = "CreateCollection_New";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  Database* db;
  auto sts = Database::Open(dbPath.c_str(), dbName.c_str(), options, db);
  ASSERT_TRUE(sts.OK());
  sts = db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS,
                             schema.c_str(), nullptr, 0);
  ASSERT_TRUE(sts.OK());
  ASSERT_TRUE(db->Close().OK());
}

TEST(Database, CreateCollection_CollectionAlreadyExist) {
  string dbName = "CreateCollection_CollectionAlreadyExist";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  Database* db;
  auto sts = Database::Open(dbPath.c_str(), dbName.c_str(), options, db);
  ASSERT_TRUE(sts.OK());
  sts = db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS,
                             schema.c_str(), nullptr, 0);
  ASSERT_TRUE(sts.OK());
  sts = db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS,
                             schema.c_str(), nullptr, 0);
  ASSERT_TRUE(sts.CollectionAlreadyExist());
  ASSERT_TRUE(db->Close().OK());
}

TEST(Database, Insert_NoIndex) {
  string dbName = "Insert_NoIndex";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  auto sts = Database::Open(dbPath.c_str(), dbName.c_str(), options, db);
  ASSERT_TRUE(sts.OK());

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  sts = db->CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS,
                             schema.c_str(), nullptr, 0);
  ASSERT_TRUE(sts.OK());

  Buffer documentData;
  ASSERT_TRUE(GetTweetObject(documentData).OK());
  ASSERT_TRUE(db->Insert(collectionName.c_str(), documentData).OK());
  ASSERT_TRUE(db->Close().OK());
}

TEST(Database, Insert_SingleIndex) {
  string dbName = "Insert_SingleIndex";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  auto sts = Database::Open(dbPath.c_str(), dbName.c_str(), options, db);
  ASSERT_TRUE(sts.OK());

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  IndexInfo indexes[1];
  indexes[0].SetName("IndexName1");
  indexes[0].SetType(IndexType::EWAHCompressedBitmap);
  indexes[0].SetIsAscending(true);
  indexes[0].SetColumnsLength(1);
  indexes[0].SetColumn(0, "user.name");

  sts = db->CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS,
                             schema.c_str(), indexes, 1);
  ASSERT_TRUE(sts.OK());

  Buffer documentData;
  ASSERT_TRUE(GetTweetObject(documentData).OK());
  ASSERT_TRUE(db->Insert(collectionName.c_str(), documentData).OK());
  ASSERT_TRUE(db->Close().OK());
}

Status GetAllFieldTypeObject(Buffer& buffer) {
  FlatBufferBuilder fbb;
  // create nested object
  auto text1 = fbb.CreateString("Say hello to my little friend!");  
  auto nestedObj = CreateNestedAllFieldType(fbb, 1, 2, 3, 4, 5, 6, 7, 8.0f, 9,
                                            10, 11.0, text1);
  // create parent object
  auto text2 = fbb.CreateString("Say hello to my little friend!");
  auto parentObj = CreateAllFieldType(fbb, 1, 2, 3, 4, 5, 6, 7, 8.0f, 9,
                                      10, 11.0, text2, nestedObj);
  fbb.Finish(parentObj);

  return buffer.Copy((char*)fbb.GetBufferPointer(), fbb.GetSize());
}

TEST(Database, Insert_AllIndexTypes) {
  string dbName = "Insert_AllIndexTypes";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  auto sts = Database::Open(dbPath.c_str(), dbName.c_str(), options, db);
  ASSERT_TRUE(sts.OK());

  string filePath = g_SchemaFolderPath + "all_field_type.fbs";
  string schema = ReadTextFile(filePath.c_str());

  const int indexLength = 24;
  IndexInfo indexes[indexLength];
  for (auto i = 0; i < indexLength; i++) {
    auto indexName = "IndexName_" + std::to_string(i);
    indexes[i].SetName(indexName.c_str());
    indexes[i].SetType(IndexType::EWAHCompressedBitmap);
    indexes[i].SetIsAscending(true);
    indexes[i].SetColumnsLength(1);
    string fieldName;
    if (i < 12) {
      fieldName = "field" + to_string(i + 1);
    } else {
      fieldName = "nestedField.field" + to_string(i - 11);
    }
    indexes[i].SetColumn(0, fieldName.c_str());
  } 

  sts = db->CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS,
    schema.c_str(), indexes, indexLength);
  ASSERT_TRUE(sts.OK());

  Buffer documentData;
  ASSERT_TRUE(GetAllFieldTypeObject(documentData).OK());
  ASSERT_TRUE(db->Insert(collectionName.c_str(), documentData).OK());
  ASSERT_TRUE(db->Close().OK());
}

TEST(Database, ExecuteSelect_MissingCollection) {
  string dbName = "ExecuteSelect_EmptyDB";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  auto sts = Database::Open(dbPath.c_str(), dbName.c_str(), options, db);
  ASSERT_TRUE(sts.OK());

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());

  sts = db->CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS,
    schema.c_str(), nullptr, 0);
  ASSERT_TRUE(sts.OK());

  ResultSet* rs;
  sts = db->ExecuteSelect("select * from missingTable where text = 'hello'", rs);
  ASSERT_FALSE(sts.OK());  
  ASSERT_TRUE(db->Close().OK());
}

TEST(Database, ExecuteSelect_NoIndex) {
  string dbName = "ExecuteSelect_EmptyDB";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  auto sts = Database::Open(dbPath.c_str(), dbName.c_str(), options, db);
  ASSERT_TRUE(sts.OK());

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());

  sts = db->CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS,
    schema.c_str(), nullptr, 0);
  ASSERT_TRUE(sts.OK());

  ResultSet* rs;
  sts = db->ExecuteSelect("select * from tweet where text = 'hello'", rs);
  ASSERT_TRUE(sts.OK());
  ASSERT_TRUE(db->Close().OK());
}