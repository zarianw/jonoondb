#include <string>
#include "gtest/gtest.h"
#include "flatbuffers/flatbuffers.h"
#include "test_utils.h"
#include "database.h"
#include "ex_database.h"
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

void CreateInsertTweet(Database* db, std::string& collectionName, bool createIndexes, int numToInsert) {
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  IndexInfo indexes[1];
  indexes[0].SetName("IndexName1");
  indexes[0].SetType(IndexType::EWAHCompressedBitmap);
  indexes[0].SetIsAscending(true);
  indexes[0].SetColumnName("user.name");

  int indexLength = createIndexes ? 1 : 0;

  auto sts = db->CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS,
    schema.c_str(), indexes, indexLength);
  ASSERT_TRUE(sts.OK());

  Buffer documentData;
  ASSERT_TRUE(GetTweetObject(documentData).OK());
  ASSERT_TRUE(db->Insert(collectionName.c_str(), documentData).OK());
}

TEST(Database, Open_InvalidArguments) {
  ex_Options options;
  ex_Database* db = nullptr;
  ASSERT_THROW(db = ex_Database::Open("somePath", "", options), InvalidArgumentException);
  ASSERT_EQ(db, nullptr);
  ASSERT_THROW(db = ex_Database::Open("", "someDbName", options), InvalidArgumentException);
  ASSERT_EQ(db, nullptr);
}

TEST(Database, Open_MissingDatabaseFile) {
  string dbName = "Database_Open_New";
  string dbPath = g_TestRootDirectory;
  ex_Options options;
  ex_Database* db = nullptr;
  ASSERT_THROW(db = ex_Database::Open(g_TestRootDirectory, dbName, options), MissingDatabaseFileException);
  ASSERT_EQ(db, nullptr);
}

TEST(Database, Open_MissingDatabaseFolder) {
  string dbName = "Database_Open_New";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db = nullptr;
  ASSERT_THROW(Database::Open(g_TestRootDirectory + "missing_folder", dbName, options, db), MissingDatabaseFolderException);
}

TEST(Database, Open_New) {
  string dbName = "Database_Open_New";
  string dbPath = g_TestRootDirectory;
  ex_Options options;
  options.SetCreateDBIfMissing(true);
  ex_Database* db = ex_Database::Open(g_TestRootDirectory, dbName,
                               options);
  //db.Close();
}

TEST(Database, Open_Existing) {
  string dbName = "Database_Open_Existing";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  Database::Open(g_TestRootDirectory.c_str(), dbName.c_str(),
                               options, db); 
  db->Close();

  Database::Open(g_TestRootDirectory.c_str(), dbName.c_str(), options,
                 db);  
}

TEST(Database, Open_CreateIfMissing) {
  //First remove the file if it exists
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db = nullptr;
  Database::Open(g_TestRootDirectory.c_str(),
                              "Database_Open_CreateIfMissing", options, db);
  ASSERT_TRUE(db != nullptr);  
  db->Close();	
}

TEST(Database, CreateCollection_InvalidSchema) {
  string dbName = "CreateCollection_New";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  Database::Open(dbPath, dbName, options, db);
  auto sts = db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS,
                             "Schema IDL", nullptr, 0);
  ASSERT_TRUE(sts.SchemaParseError());
  db->Close();
}

TEST(Database, CreateCollection_New) {
  string dbName = "CreateCollection_New";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  Database* db;
  Database::Open(dbPath, dbName, options, db);
  auto sts = db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS,
                             schema.c_str(), nullptr, 0);
  ASSERT_TRUE(sts.OK());
  db->Close();
}

TEST(Database, CreateCollection_CollectionAlreadyExist) {
  string dbName = "CreateCollection_CollectionAlreadyExist";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  Database* db;
  Database::Open(dbPath, dbName, options, db);
  auto sts = db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS,
                             schema.c_str(), nullptr, 0);
  ASSERT_TRUE(sts.OK());
  sts = db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS,
                             schema.c_str(), nullptr, 0);
  ASSERT_TRUE(sts.CollectionAlreadyExist());
  db->Close();
}

TEST(Database, Insert_NoIndex) {
  string dbName = "Insert_NoIndex";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  Database::Open(dbPath, dbName, options, db);  

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  auto sts = db->CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS,
                             schema.c_str(), nullptr, 0);
  ASSERT_TRUE(sts.OK());

  Buffer documentData;
  ASSERT_TRUE(GetTweetObject(documentData).OK());
  ASSERT_TRUE(db->Insert(collectionName.c_str(), documentData).OK());
  db->Close();
}

TEST(Database, Insert_SingleIndex) {
  string dbName = "Insert_SingleIndex";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  Database::Open(dbPath, dbName, options, db);  

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  IndexInfo indexes[1];
  indexes[0].SetName("IndexName1");
  indexes[0].SetType(IndexType::EWAHCompressedBitmap);
  indexes[0].SetIsAscending(true);
  indexes[0].SetColumnName("user.name");

  auto sts = db->CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS,
                             schema.c_str(), indexes, 1);
  ASSERT_TRUE(sts.OK());

  Buffer documentData;
  ASSERT_TRUE(GetTweetObject(documentData).OK());
  ASSERT_TRUE(db->Insert(collectionName.c_str(), documentData).OK());
  db->Close();
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
  Database::Open(dbPath, dbName, options, db);  

  string filePath = g_SchemaFolderPath + "all_field_type.fbs";
  string schema = ReadTextFile(filePath.c_str());

  const int indexLength = 24;
  IndexInfo indexes[indexLength];
  for (auto i = 0; i < indexLength; i++) {
    auto indexName = "IndexName_" + std::to_string(i);
    indexes[i].SetName(indexName.c_str());
    indexes[i].SetType(IndexType::EWAHCompressedBitmap);
    indexes[i].SetIsAscending(true);
    string fieldName;
    if (i < 12) {
      fieldName = "field" + to_string(i + 1);
    } else {
      fieldName = "nestedField.field" + to_string(i - 11);
    }
    indexes[i].SetColumnName(fieldName.c_str());
  } 

  auto sts = db->CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS,
    schema.c_str(), indexes, indexLength);
  ASSERT_TRUE(sts.OK());

  Buffer documentData;
  ASSERT_TRUE(GetAllFieldTypeObject(documentData).OK());
  ASSERT_TRUE(db->Insert(collectionName.c_str(), documentData).OK());
  db->Close();
}

TEST(Database, ExecuteSelect_MissingCollection) {
  string dbName = "ExecuteSelect_MissingCollection";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  Database::Open(dbPath, dbName, options, db);  

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());

  auto sts = db->CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS,
    schema.c_str(), nullptr, 0);
  ASSERT_TRUE(sts.OK());

  ResultSet* rs;
  sts = db->ExecuteSelect("select * from missingTable where text = 'hello'", rs);
  ASSERT_FALSE(sts.OK());  
  db->Close();
}

TEST(Database, ExecuteSelect_EmptyDB_NoIndex) {
  string dbName = "ExecuteSelect_EmptyDB_NoIndex";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  Database::Open(dbPath, dbName, options, db);  

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());

  auto sts = db->CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS,
    schema.c_str(), nullptr, 0);
  ASSERT_TRUE(sts.OK());

  ResultSet* rs;
  sts = db->ExecuteSelect("select * from tweet where text = 'hello'", rs);
  ASSERT_TRUE(sts.OK());
  db->Close();
}

TEST(Database, ExecuteSelect_NonEmptyDB_NoIndex) {
  string dbName = "ExecuteSelect_NonEmptyDB_NoIndex";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  Database::Open(dbPath, dbName, options, db);  

  CreateInsertTweet(db, collectionName, true, 1);
  ResultSet* rs;
  auto sts = db->ExecuteSelect("select * from tweet where [user.name] = 'zarian'", rs);
  ASSERT_TRUE(sts.OK());
  db->Close();
}