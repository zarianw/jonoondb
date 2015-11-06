#include <string>
#include "gtest/gtest.h"
#include "flatbuffers/flatbuffers.h"
#include "test_utils.h"
#include "database.h"
#include "enums.h"
#include "buffer.h"
#include "schemas/flatbuffers/tweet_generated.h"
#include "schemas/flatbuffers/all_field_type_generated.h"

using namespace std;
using namespace flatbuffers;
using namespace jonoondb_api;
using namespace jonoondb_test;

void CreateInsertTweet(Database* db, std::string& collectionName, bool createIndexes, int numToInsert) {
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  std::vector<ex_IndexInfo> indexes;
  if (createIndexes) {
    ex_IndexInfo index;
    index.SetIndexName("IndexName1");
    index.SetType(IndexType::EWAHCompressedBitmap);
    index.SetIsAscending(true);
    index.SetColumnName("user.name");
    indexes.push_back(index);
  }

  db->CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);
  
  ex_Buffer documentData = GetTweetObject2();
  db->Insert(collectionName, documentData);
}

TEST(Database, Open_InvalidArguments) {
  ex_Options options;
  Database* db = nullptr;
  ASSERT_THROW(db = Database::Open("somePath", "", options), InvalidArgumentException);
  ASSERT_EQ(db, nullptr);
  ASSERT_THROW(db = Database::Open("", "someDbName", options), InvalidArgumentException);
  ASSERT_EQ(db, nullptr);
}

TEST(Database, Open_MissingDatabaseFile) {
  string dbName = "Database_Open_New";
  string dbPath = g_TestRootDirectory;
  ex_Options options;
  Database* db = nullptr;
  ASSERT_THROW(db = Database::Open(g_TestRootDirectory, dbName, options), MissingDatabaseFileException);
  ASSERT_EQ(db, nullptr);
}

TEST(Database, Open_MissingDatabaseFolder) {
  string dbName = "Database_Open_New";
  string dbPath = g_TestRootDirectory;
  ex_Options options;
  options.SetCreateDBIfMissing(true);
  Database* db = nullptr;
  ASSERT_THROW(Database::Open(g_TestRootDirectory + "missing_folder", dbName, options), MissingDatabaseFolderException);
}

TEST(Database, Open_New) {
  string dbName = "Database_Open_New";
  string dbPath = g_TestRootDirectory;
  ex_Options options;
  options.SetCreateDBIfMissing(true);
  Database* db = Database::Open(g_TestRootDirectory, dbName, options);
  db->Close();
}

TEST(Database, Open_Existing) {
  string dbName = "Database_Open_Existing";
  string dbPath = g_TestRootDirectory;
  ex_Options options;
  options.SetCreateDBIfMissing(true);
  Database* db;
  db = Database::Open(g_TestRootDirectory.c_str(), dbName, options); 
  db->Close();

  db = Database::Open(g_TestRootDirectory.c_str(), dbName.c_str(), options);  
  db->Close();
}

TEST(Database, Open_CreateIfMissing) {
  //First remove the file if it exists
  ex_Options options;
  options.SetCreateDBIfMissing(true);
  Database* db = Database::Open(g_TestRootDirectory.c_str(), "Database_Open_CreateIfMissing", options);  
  db->Close();	
}

TEST(Database, CreateCollection_InvalidSchema) {
  string dbName = "CreateCollection_New";
  string dbPath = g_TestRootDirectory;
  ex_Options options;
  options.SetCreateDBIfMissing(true);
  Database* db = Database::Open(dbPath, dbName, options);
  std::vector<ex_IndexInfo> indexes;
  ASSERT_THROW(db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS, "Schema IDL", indexes), SchemaParseException);
  db->Close();
}

TEST(Database, CreateCollection_New) {
  string dbName = "CreateCollection_New";
  string dbPath = g_TestRootDirectory;
  ex_Options options;
  options.SetCreateDBIfMissing(true);
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  Database* db = Database::Open(dbPath, dbName, options);
  std::vector<ex_IndexInfo> indexes;
  db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS, schema, indexes);  
  db->Close();
}

TEST(Database, CreateCollection_CollectionAlreadyExist) {
  string dbName = "CreateCollection_CollectionAlreadyExist";
  string dbPath = g_TestRootDirectory;
  ex_Options options;
  options.SetCreateDBIfMissing(true);
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  Database* db = Database::Open(dbPath, dbName, options);
  std::vector<ex_IndexInfo> indexes;
  db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS, schema, indexes);  
  ASSERT_THROW(db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS, schema, indexes), CollectionAlreadyExistException);  
  db->Close();
}

TEST(Database, Insert_NoIndex) {
  string dbName = "Insert_NoIndex";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  ex_Options options;
  options.SetCreateDBIfMissing(true);
  Database* db = Database::Open(dbPath, dbName, options);  

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  
  std::vector<ex_IndexInfo> indexes;
  db->CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);  

  ex_Buffer documentData = GetTweetObject2();
  db->Insert(collectionName, documentData);
  db->Close();
}

TEST(Database, Insert_SingleIndex) {
  string dbName = "Insert_SingleIndex";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  ex_Options options;
  options.SetCreateDBIfMissing(true);
  Database* db = Database::Open(dbPath, dbName, options);  

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  ex_IndexInfo index;
  index.SetIndexName("IndexName1");
  index.SetType(IndexType::EWAHCompressedBitmap);
  index.SetIsAscending(true);
  index.SetColumnName("user.name");
  std::vector<ex_IndexInfo> indexes;
  indexes.push_back(index);

  db->CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS,
                                  schema.c_str(), indexes);
  
  ex_Buffer documentData = GetTweetObject2();
  db->Insert(collectionName, documentData);
  db->Close();
}

ex_Buffer GetAllFieldTypeObject() {
  ex_Buffer buffer;
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
  buffer.Resize(fbb.GetSize());

  buffer.Copy((char*)fbb.GetBufferPointer(), fbb.GetSize());
  return buffer;
}

TEST(Database, Insert_AllIndexTypes) {
  string dbName = "Insert_AllIndexTypes";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  ex_Options options;
  options.SetCreateDBIfMissing(true);
  Database* db = Database::Open(dbPath, dbName, options);  

  string filePath = g_SchemaFolderPath + "all_field_type.fbs";
  string schema = ReadTextFile(filePath.c_str());

  const int indexLength = 24;
  std::vector<ex_IndexInfo> indexes;
  ex_IndexInfo index;
  for (auto i = 0; i < indexLength; i++) {    
    auto indexName = "IndexName_" + std::to_string(i);
    index.SetIndexName(indexName);
    index.SetType(IndexType::EWAHCompressedBitmap);
    index.SetIsAscending(true);
    string fieldName;
    if (i < 12) {
      fieldName = "field" + to_string(i + 1);
    } else {
      fieldName = "nestedField.field" + to_string(i - 11);
    }
    index.SetColumnName(fieldName.c_str());

    indexes.push_back(index);
  } 

  db->CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);

  ex_Buffer documentData = GetAllFieldTypeObject();
  db->Insert(collectionName, documentData);
  db->Close();
}

TEST(Database, ExecuteSelect_MissingCollection) {
  string dbName = "ExecuteSelect_MissingCollection";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  ex_Options options;
  options.SetCreateDBIfMissing(true);
  
  Database* db = Database::Open(dbPath, dbName, options);

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  db->CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, std::vector<ex_IndexInfo>());
  ASSERT_ANY_THROW(ResultSet rs = db->ExecuteSelect("select * from missingTable where text = 'hello'"));
  db->Close();
}

TEST(Database, ExecuteSelect_EmptyDB_NoIndex) {
  string dbName = "ExecuteSelect_EmptyDB_NoIndex";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  ex_Options options;
  options.SetCreateDBIfMissing(true);
  Database* db = Database::Open(dbPath, dbName, options);
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  db->CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS, schema.c_str(), std::vector<ex_IndexInfo>());

  ResultSet rs = db->ExecuteSelect("select * from tweet where text = 'hello'");  
  db->Close();
}

TEST(Database, ExecuteSelect_NonEmptyDB_NoIndex) {
  string dbName = "ExecuteSelect_NonEmptyDB_NoIndex";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  ex_Options options;
  options.SetCreateDBIfMissing(true);
  Database* db = Database::Open(dbPath, dbName, options);

  CreateInsertTweet(db, collectionName, true, 1);
  ResultSet* rs;
  db->ExecuteSelect("select * from tweet where [user.name] = 'zarian'");  
  db->Close();
}