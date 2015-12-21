#include <string>
#include "gtest/gtest.h"
#include "flatbuffers/flatbuffers.h"
#include "test_utils.h"
#include "database.h"
#include "enums.h"
#include "buffer_impl.h"
#include "schemas/flatbuffers/tweet_generated.h"
#include "schemas/flatbuffers/all_field_type_generated.h"

using namespace std;
using namespace flatbuffers;
using namespace jonoondb_api;
using namespace jonoondb_test;

void CreateInsertTweet(Database* db, std::string& collectionName, bool createIndexes, int numToInsert) {
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  std::vector<IndexInfo> indexes;
  if (createIndexes) {
    IndexInfo index;
    index.SetIndexName("IndexName1");
    index.SetType(IndexType::EWAHCompressedBitmap);
    index.SetIsAscending(true);
    index.SetColumnName("user.name");
    indexes.push_back(index);
  }

  db->CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);
  
  Buffer documentData = GetTweetObject2();
  db->Insert(collectionName, documentData);
}

TEST(Database, Open_InvalidArguments) {
  Options options;
  Database* db = nullptr;
  ASSERT_THROW(db = Database::Open("somePath", "", options), InvalidArgumentException);
  ASSERT_EQ(db, nullptr);
  ASSERT_THROW(db = Database::Open("", "someDbName", options), InvalidArgumentException);
  ASSERT_EQ(db, nullptr);
}

TEST(Database, Open_MissingDatabaseFile) {
  string dbName = "Database_Open_New";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(false);
  Database* db = nullptr;
  ASSERT_THROW(db = Database::Open(dbPath, dbName, options), MissingDatabaseFileException);
  ASSERT_EQ(db, nullptr);
}

TEST(Database, Open_MissingDatabaseFolder) {
  string dbName = "Database_Open_New";
  string dbPath = g_TestRootDirectory;
  Options options;  
  Database* db = nullptr;
  ASSERT_THROW(Database::Open(dbPath + "missing_folder", dbName, options), MissingDatabaseFolderException);
}

TEST(Database, Open_New) {
  string dbName = "Database_Open_New";
  string dbPath = g_TestRootDirectory;
  Options options;  
  Database* db = Database::Open(dbPath, dbName, options);
  db->Close();
}

TEST(Database, Open_Existing) {
  string dbName = "Database_Open_Existing";
  string dbPath = g_TestRootDirectory;
  Options options;  
  Database* db;
  db = Database::Open(dbPath, dbName, options);
  db->Close();

  db = Database::Open(dbPath, dbName.c_str(), options);
  db->Close();
}

TEST(Database, Open_CreateIfMissing) {
  //First remove the file if it exists
  Options options;  
  Database* db = Database::Open(g_TestRootDirectory.c_str(), "Database_Open_CreateIfMissing", options);  
  db->Close();	
}

TEST(Database, CreateCollection_InvalidSchema) {
  string dbName = "CreateCollection_New";
  string dbPath = g_TestRootDirectory;
  Options options;  
  Database* db = Database::Open(dbPath, dbName, options);
  std::vector<IndexInfo> indexes;
  ASSERT_THROW(db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS, "Schema IDL", indexes), SchemaParseException);
  db->Close();
}

TEST(Database, CreateCollection_New) {
  string dbName = "CreateCollection_New";
  string dbPath = g_TestRootDirectory;
  Options options;  
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  Database* db = Database::Open(dbPath, dbName, options);
  std::vector<IndexInfo> indexes;
  db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS, schema, indexes);  
  db->Close();
}

TEST(Database, CreateCollection_CollectionAlreadyExist) {
  string dbName = "CreateCollection_CollectionAlreadyExist";
  string dbPath = g_TestRootDirectory;
  Options options;  
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  Database* db = Database::Open(dbPath, dbName, options);
  std::vector<IndexInfo> indexes;
  db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS, schema, indexes);  
  ASSERT_THROW(db->CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS, schema, indexes), CollectionAlreadyExistException);  
  db->Close();
}

TEST(Database, Insert_NoIndex) {
  string dbName = "Insert_NoIndex";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Options options;  
  Database* db = Database::Open(dbPath, dbName, options);  

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  
  std::vector<IndexInfo> indexes;
  db->CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);  

  Buffer documentData = GetTweetObject2();
  db->Insert(collectionName, documentData);
  db->Close();
}

TEST(Database, Insert_SingleIndex) {
  string dbName = "Insert_SingleIndex";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Options options;  
  Database* db = Database::Open(dbPath, dbName, options);  

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  IndexInfo index;
  index.SetIndexName("IndexName1");
  index.SetType(IndexType::EWAHCompressedBitmap);
  index.SetIsAscending(true);
  index.SetColumnName("user.name");
  std::vector<IndexInfo> indexes;
  indexes.push_back(index);

  db->CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS,
                                  schema.c_str(), indexes);
  
  Buffer documentData = GetTweetObject2();
  db->Insert(collectionName, documentData);
  db->Close();
}

Buffer GetAllFieldTypeObject() {
  Buffer buffer;
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
  Options options;  
  Database* db = Database::Open(dbPath, dbName, options);  

  string filePath = g_SchemaFolderPath + "all_field_type.fbs";
  string schema = ReadTextFile(filePath.c_str());

  const int indexLength = 24;
  std::vector<IndexInfo> indexes;
  IndexInfo index;
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

  Buffer documentData = GetAllFieldTypeObject();
  db->Insert(collectionName, documentData);
  db->Close();
}

TEST(Database, ExecuteSelect_MissingCollection) {
  string dbName = "ExecuteSelect_MissingCollection";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Options options;  
  Database* db = Database::Open(dbPath, dbName, options);

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  db->CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, std::vector<IndexInfo>());
  ASSERT_ANY_THROW(ResultSet rs = db->ExecuteSelect("select * from missingTable where text = 'hello'"));
  db->Close();
}

TEST(Database, ExecuteSelect_EmptyDB_NoIndex) {
  string dbName = "ExecuteSelect_EmptyDB_NoIndex";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  Options options;  
  Database* db = Database::Open(dbPath, dbName, options);
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  db->CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS, schema.c_str(), std::vector<IndexInfo>());

  ResultSet rs = db->ExecuteSelect("select * from tweet where text = 'hello'"); 
  rs.Close();
  db->Close();
}

TEST(Database, ExecuteSelect_NonEmptyDB_SingleIndex) {
  string dbName = "ExecuteSelect_NonEmptyDB_SingleIndex";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  Options options;  
  Database* db = Database::Open(dbPath, dbName, options);

  CreateInsertTweet(db, collectionName, true, 1);
  ResultSet rs = db->ExecuteSelect("select * from tweet where [user.name] = 'zarian'");  
  rs.Close();
  db->Close();
}

TEST(Database, ExecuteSelect_Testing) {
  string dbName = "ExecuteSelect_Testing";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  Options options;  
  Database* db = Database::Open(dbPath, dbName, options);

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  std::vector<IndexInfo> indexes;

  IndexInfo index;
  index.SetIndexName("IndexName1");
  index.SetType(IndexType::EWAHCompressedBitmap);
  index.SetIsAscending(true);
  index.SetColumnName("user.name");
  indexes.push_back(index);

  index.SetIndexName("IndexName2");
  index.SetType(IndexType::EWAHCompressedBitmap);
  index.SetIsAscending(true);
  index.SetColumnName("text");
  indexes.push_back(index);

  index.SetIndexName("IndexName3");
  index.SetType(IndexType::EWAHCompressedBitmap);
  index.SetIsAscending(true);
  index.SetColumnName("id");
  indexes.push_back(index);
  db->CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);

  Buffer documentData = GetTweetObject2();
  db->Insert(collectionName, documentData);

  ResultSet rs = db->ExecuteSelect("SELECT id, text, 'user.id', 'user.name' FROM tweet WHERE id = 1;");
  while (rs.Next()) {
    
  }

  rs = db->ExecuteSelect("SELECT id, text, 'user.id', 'user.name' FROM tweet WHERE [user.name] = 'Zarian' AND text = 'hello'");
  rs = db->ExecuteSelect("SELECT id, text, 'user.id', 'user.name' FROM tweet WHERE [user.name] = 'Zarian' OR text = 'hello'");

  rs.Close();
  db->Close();
}