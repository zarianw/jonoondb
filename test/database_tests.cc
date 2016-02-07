#include <string>
#include <fstream>
#include <cstdio>
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

void CreateInsertTweet(Database& db, std::string& collectionName, bool createIndexes, int numToInsert) {
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath);
  std::vector<IndexInfo> indexes;
  if (createIndexes) {
    IndexInfo index;
    index.SetIndexName("IndexName1");
    index.SetType(IndexType::EWAH_COMPRESSED_BITMAP);
    index.SetIsAscending(true);
    index.SetColumnName("user.name");
    indexes.push_back(index);
  }

  db.CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);
  
  Buffer documentData = GetTweetObject2();
  db.Insert(collectionName, documentData);
}

TEST(Database, Ctor_InvalidArguments) {
  Options options;
  ASSERT_THROW(Database db("somePath", "", options), InvalidArgumentException);
  ASSERT_THROW(Database db("", "someDbName", options), InvalidArgumentException);  
}

TEST(Database, Ctor_MissingDatabaseFile) {
  string dbName = "Database_Ctor_MissingDatabaseFile";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(false);
  ASSERT_THROW(Database db(dbPath, dbName, options), MissingDatabaseFileException);  
}

TEST(Database, Ctor_MissingDatabaseFolder) {
  string dbName = "Database_Ctor_MissingDatabaseFolder";
  string dbPath = g_TestRootDirectory;
  Options options;  
  ASSERT_THROW(Database db(dbPath + "missing_folder", dbName, options), MissingDatabaseFolderException);
}

Options GetDefaultDBOptions() {
  Options opt;
  opt.SetMaxDataFileSize(1024 * 1024);
  return opt;
}

TEST(Database, Ctor_New) {
  string dbName = "Database_Ctor_New";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, GetDefaultDBOptions());
}

TEST(Database, Ctor_Existing) {
  string dbName = "Database_Ctor_Existing";
  string dbPath = g_TestRootDirectory;
  {
    Database db(dbPath, dbName, GetDefaultDBOptions());
  }
  
  {
    Database db(dbPath, dbName, GetDefaultDBOptions());
  }
}

TEST(Database, Ctor_CreateIfMissing) {
  Database db(g_TestRootDirectory, "Database_Ctor_CreateIfMissing", GetDefaultDBOptions());  
}

TEST(Database, CreateCollection_InvalidSchema) {
  string dbName = "Database_CreateCollection_InvalidSchema";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, GetDefaultDBOptions());
  std::vector<IndexInfo> indexes;
  ASSERT_THROW(db.CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS, "Schema IDL", indexes), SchemaParseException);
}

TEST(Database, CreateCollection_New) {
  string dbName = "Database_CreateCollection_New";
  string dbPath = g_TestRootDirectory;
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath);
  Database db(dbPath, dbName, GetDefaultDBOptions());
  std::vector<IndexInfo> indexes;
  db.CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS, schema, indexes);  
}

TEST(Database, CreateCollection_CollectionAlreadyExist) {
  string dbName = "Database_CreateCollection_CollectionAlreadyExist";
  string dbPath = g_TestRootDirectory;
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath);
  Database db(dbPath, dbName, GetDefaultDBOptions());
  std::vector<IndexInfo> indexes;
  db.CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS, schema, indexes);  
  ASSERT_THROW(db.CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS, schema, indexes), CollectionAlreadyExistException);  
}

TEST(Database, CreateCollection_DuplicateIndex) {
  string dbName = "Database_CreateCollection_DuplicateIndex";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, GetDefaultDBOptions());

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath);
  std::vector<IndexInfo> indexes;
  indexes.push_back(IndexInfo("index1", IndexType::EWAH_COMPRESSED_BITMAP, "id", true));
  indexes.push_back(IndexInfo("index1", IndexType::EWAH_COMPRESSED_BITMAP, "user.name", true));
  ASSERT_THROW(db.CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes), IndexAlreadyExistException);

  // Now try to create the collection without duplicate index
  indexes.pop_back();
  ASSERT_NO_THROW(db.CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes));
}

TEST(Database, Insert_NoIndex) {
  string dbName = "Database_Insert_NoIndex";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, GetDefaultDBOptions());  

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath);
  
  std::vector<IndexInfo> indexes;
  db.CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);  

  Buffer documentData = GetTweetObject2();
  db.Insert(collectionName, documentData);
}

TEST(Database, Insert_SingleIndex) {
  string dbName = "Database_Insert_SingleIndex";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, GetDefaultDBOptions());  

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath);
  IndexInfo index;
  index.SetIndexName("IndexName1");
  index.SetType(IndexType::EWAH_COMPRESSED_BITMAP);
  index.SetIsAscending(true);
  index.SetColumnName("user.name");
  std::vector<IndexInfo> indexes;
  indexes.push_back(index);

  db.CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS,
                                  schema.c_str(), indexes);
  
  Buffer documentData = GetTweetObject2();
  db.Insert(collectionName, documentData);
}

Buffer GetAllFieldTypeObjectBuffer() {
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
  string dbName = "Database_Insert_AllIndexTypes";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, GetDefaultDBOptions());  

  string filePath = g_SchemaFolderPath + "all_field_type.fbs";
  string schema = ReadTextFile(filePath);

  const int indexLength = 24;
  std::vector<IndexInfo> indexes;
  IndexInfo index;
  for (auto i = 0; i < indexLength; i++) {    
    auto indexName = "IndexName_" + std::to_string(i);
    index.SetIndexName(indexName);
    index.SetType(IndexType::EWAH_COMPRESSED_BITMAP);
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

  db.CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);

  Buffer documentData = GetAllFieldTypeObjectBuffer();
  db.Insert(collectionName, documentData);
}

TEST(Database, ExecuteSelect_MissingCollection) {
  string dbName = "Database_ExecuteSelect_MissingCollection";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, GetDefaultDBOptions());

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath);
  db.CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, std::vector<IndexInfo>());
  ASSERT_ANY_THROW(ResultSet rs = db.ExecuteSelect("select * from missingTable where text = 'hello'"));
}

TEST(Database, ExecuteSelect_EmptyDB_NoIndex) {
  string dbName = "Database_ExecuteSelect_EmptyDB_NoIndex";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, GetDefaultDBOptions());
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath);
  db.CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS, schema.c_str(), std::vector<IndexInfo>());

  int rows = 0;
  ResultSet rs = db.ExecuteSelect("select * from tweet where text = 'hello'"); 
  while (rs.Next()) {
    ++rows;
  }
  ASSERT_EQ(rows, 0);
  rs.Close();
}

TEST(Database, ExecuteSelect_NonEmptyDB_SingleIndex) {
  string dbName = "Database_ExecuteSelect_NonEmptyDB_SingleIndex";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, GetDefaultDBOptions());

  CreateInsertTweet(db, collectionName, true, 1);
  int rows = 0;
  ResultSet rs = db.ExecuteSelect("select * from tweet where [user.name] = 'Zarian'");
  while (rs.Next()) {
    ++rows;
  }
  ASSERT_EQ(rows, 1);
  rs.Close();
}

TEST(Database, ExecuteSelect_Testing) {
  string dbName = "Database_ExecuteSelect_Testing";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, GetDefaultDBOptions());

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath);
  std::vector<IndexInfo> indexes;

  IndexInfo index;
  index.SetIndexName("IndexName1");
  index.SetType(IndexType::EWAH_COMPRESSED_BITMAP);
  index.SetIsAscending(true);
  index.SetColumnName("user.name");
  indexes.push_back(index);

  index.SetIndexName("IndexName2");
  index.SetType(IndexType::EWAH_COMPRESSED_BITMAP);
  index.SetIsAscending(true);
  index.SetColumnName("text");
  indexes.push_back(index);

  index.SetIndexName("IndexName3");
  index.SetType(IndexType::EWAH_COMPRESSED_BITMAP);
  index.SetIsAscending(true);
  index.SetColumnName("id");
  indexes.push_back(index);
  db.CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);

  Buffer documentData = GetTweetObject2();
  db.Insert(collectionName, documentData);
  
  int rows = 0;
  ResultSet rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name] FROM tweet WHERE id = 1;");
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), 1);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("text")).str(), "Say hello to my little friend!");
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("user.id")), 1);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("user.name")).str(), "Zarian");
    ++rows;
  }
  ASSERT_EQ(rows, 1);

  rows = 0;
  rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name] FROM tweet WHERE [user.name] = 'Zarian' AND text = 'hello'");
  while (rs.Next()) {
    ++rows;
  }
  ASSERT_EQ(rows, 0);

  rows = 0;
  rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name] FROM tweet WHERE [user.name] = 'Zarian' OR text = 'hello'");
  while (rs.Next()) {
    ++rows;
  }
  ASSERT_EQ(rows, 1);

  rows = 0;
  rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name] FROM tweet WHERE [user.name] = 'Zarian' AND text = 'Say hello to my little friend!'");
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), 1);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("text")).str(), "Say hello to my little friend!");
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("user.id")), 1);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("user.name")).str(), "Zarian");
    ++rows;
  }
  ASSERT_EQ(rows, 1);  

  rs.Close();
}

TEST(Database, MultiInsert) {
  string dbName = "Database_MultiInsert";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, GetDefaultDBOptions());

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath);
  std::vector<IndexInfo> indexes;

  IndexInfo index;
  index.SetIndexName("IndexName1");
  index.SetType(IndexType::EWAH_COMPRESSED_BITMAP);
  index.SetIsAscending(true);
  index.SetColumnName("user.name");
  indexes.push_back(index);

  index.SetIndexName("IndexName2");
  index.SetType(IndexType::EWAH_COMPRESSED_BITMAP);
  index.SetIsAscending(true);
  index.SetColumnName("text");
  indexes.push_back(index);

  index.SetIndexName("IndexName3");
  index.SetType(IndexType::EWAH_COMPRESSED_BITMAP);
  index.SetIsAscending(true);
  index.SetColumnName("id");
  indexes.push_back(index);
  db.CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    documents.push_back(GetTweetObject2(i, i, name, text));
  }

  db.MultiInsert(collectionName, documents);

  // Now see if they were inserted correctly
  auto rs = db.ExecuteSelect("SELECT [user.name] from tweet;");
  auto rowCnt = 0;
  while (rs.Next()) {
    std::string name = "zarian_" + std::to_string(rowCnt);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("user.name")).str(), name.c_str());
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 10);
}

/*TEST(Database, Insert_100K) {
  string dbName = "Database_Insert_100K";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  Options opt;
  Database db(dbPath, dbName, opt);

  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath);
  std::vector<IndexInfo> indexes;

  IndexInfo index;
  index.SetIndexName("IndexName1");
  index.SetType(IndexType::EWAH_COMPRESSED_BITMAP);
  index.SetIsAscending(true);
  index.SetColumnName("user.name");
  indexes.push_back(index);

  index.SetIndexName("IndexName2");
  index.SetType(IndexType::EWAH_COMPRESSED_BITMAP);
  index.SetIsAscending(true);
  index.SetColumnName("text");
  indexes.push_back(index);

  index.SetIndexName("IndexName3");
  index.SetType(IndexType::EWAH_COMPRESSED_BITMAP);
  index.SetIsAscending(true);
  index.SetColumnName("id");
  indexes.push_back(index);
  db.CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);

  std::string name = "Zarian";
  std::string text = "Say hello to my little friend!";

  const size_t count = 1000 * 1000;
  std::vector<Buffer> documents;
  for (size_t i = 0; i < count; i++) {
    //if (i % 2) {
      name = "zarian_" + std::to_string(i);
      text = "hello_" + std::to_string(i);
    //}

    documents.push_back(GetTweetObject2(i, i, name, text));
  }

  // dump to file
  {
    std::remove("tweets.fb");
    std::ofstream file("tweet.fb", ios::binary);
    std::uint32_t size = 0;
    for (auto& doc : documents) {
      size = doc.GetLength();
      file.write((const char *)&size, sizeof(std::uint32_t));
      file.write(doc.GetData(), doc.GetLength());
    }
  }

  db.MultiInsert(collectionName, documents);  
}*/