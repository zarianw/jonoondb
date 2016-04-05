#include <string>
#include <fstream>
#include <cstdio>
#include "gtest/gtest.h"
#include "flatbuffers/flatbuffers.h"
#include "test_utils.h"
#include "database.h"
#include "enums.h"
#include "buffer_impl.h"
#include "tweet_generated.h"
#include "all_field_type_generated.h"
#include "file.h"

using namespace std;
using namespace flatbuffers;
using namespace jonoondb_api;
using namespace jonoondb_test;

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
  ASSERT_THROW(db.CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS, "Schema IDL", indexes), InvalidSchemaException);
}

TEST(Database, CreateCollection_New) {
  string dbName = "Database_CreateCollection_New";
  string dbPath = g_TestRootDirectory;
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  Database db(dbPath, dbName, GetDefaultDBOptions());
  std::vector<IndexInfo> indexes;
  db.CreateCollection("CollectionName", SchemaType::FLAT_BUFFERS, schema, indexes);  
}

TEST(Database, CreateCollection_CollectionAlreadyExist) {
  string dbName = "Database_CreateCollection_CollectionAlreadyExist";
  string dbPath = g_TestRootDirectory;
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
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

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
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

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  
  std::vector<IndexInfo> indexes;
  db.CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);  
  std::string name = "Zarian";
  std::string text = "Say hello to my little friend!";
  Buffer documentData = GetTweetObject2(1, 1, &name, &text, 2.0);
  db.Insert(collectionName, documentData);
}

TEST(Database, Insert_SingleIndex) {
  string dbName = "Database_Insert_SingleIndex";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, GetDefaultDBOptions());  

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  IndexInfo index;
  index.SetIndexName("IndexName1");
  index.SetType(IndexType::EWAH_COMPRESSED_BITMAP);
  index.SetIsAscending(true);
  index.SetColumnName("user.name");
  std::vector<IndexInfo> indexes;
  indexes.push_back(index);

  db.CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS,
                                  schema, indexes);
  std::string name = "Zarian";
  std::string text = "Say hello to my little friend!";
  Buffer documentData = GetTweetObject2(1, 1, &name, &text, 2.0);  
  
  db.Insert(collectionName, documentData);
}

Buffer GetAllFieldTypeObjectBuffer() {
  Buffer buffer;
  FlatBufferBuilder fbb;
  // create nested object
  auto text1 = fbb.CreateString("Say hello to my little friend!");  
  auto nestedObj = CreateNestedAllFieldType(fbb, 1, 2, 3, 4, 5, 6, 7, 8.0f, 9,
                                            10.0, text1);
  // create parent object
  auto text2 = fbb.CreateString("Say hello to my little friend!");
  auto parentObj = CreateAllFieldType(fbb, 1, 2, 3, 4, 5, 6, 7, 8.0f, 9,
                                      10.0, text2, nestedObj);
  fbb.Finish(parentObj);
  buffer.Resize(fbb.GetSize());

  buffer.Copy((char*)fbb.GetBufferPointer(), fbb.GetSize());
  return buffer;
}

Buffer GetAllFieldTypeObjectBuffer(char field1, unsigned char field2, bool field3, int16_t field4,
                                   uint16_t field5,int32_t field6, uint32_t field7,float field8,int64_t field9,
                                   double field10, const std::string& field11) {
  FlatBufferBuilder fbb;
  // create nested object
  auto str = fbb.CreateString(field11);
  auto nestedObj = CreateNestedAllFieldType(fbb, field1, field2, field3, field4, field5, field6,field7,
                                            field8, field9,field10, str);
  // create parent object
  auto str2 = fbb.CreateString(field11);
  auto parentObj = CreateAllFieldType(fbb, field1, field2, field3, field4, field5, field6, field7, field8,
                                      field9,field10, str2, nestedObj);
  fbb.Finish(parentObj);

  return Buffer((char*)fbb.GetBufferPointer(), fbb.GetSize(), fbb.GetSize());
}

TEST(Database, Insert_AllIndexTypes) {
  string dbName = "Database_Insert_AllIndexTypes";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, GetDefaultDBOptions());  

  string filePath = GetSchemaFilePath("all_field_type.bfbs");
  string schema = File::Read(filePath);

  const int indexLength = 22;
  std::vector<IndexInfo> indexes;
  IndexInfo index;
  for (auto i = 0; i < indexLength; i++) {    
    auto indexName = "IndexName_" + std::to_string(i);
    index.SetIndexName(indexName);
    index.SetType(IndexType::EWAH_COMPRESSED_BITMAP);
    index.SetIsAscending(true);
    string fieldName;
    if (i < 11) {
      fieldName = "field" + to_string(i + 1);
    } else {
      fieldName = "nestedField.field" + to_string(i - 10);
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

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  db.CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, std::vector<IndexInfo>());
  ASSERT_ANY_THROW(ResultSet rs = db.ExecuteSelect("select * from missingTable where text = 'hello'"));
}

TEST(Database, ExecuteSelect_EmptyDB_NoIndex) {
  string dbName = "Database_ExecuteSelect_EmptyDB_NoIndex";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  db.CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS, schema, std::vector<IndexInfo>());

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

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes{ IndexInfo("IndexName1", IndexType::EWAH_COMPRESSED_BITMAP, "user.name", true) };
  db.CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);

  std::string name = "Zarian";
  std::string text = "Say hello to my little friend!";
  Buffer documentData = GetTweetObject2(1, 1, &name, &text, 2.0);
  db.Insert(collectionName, documentData);

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

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
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
  Buffer documentData = GetTweetObject2(1, 1, &name, &text, 2.0);
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

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
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
    documents.push_back(GetTweetObject2(i, i, &name, &text, (double)i));
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

TEST(Database, Ctor_ReOpen) {
  string dbName = "Database_Ctor_ReOpen";
  string collectionName1 = "tweet1";
  string collectionName2 = "tweet2";
  string dbPath = g_TestRootDirectory;
  
  {
    //scope for database
    Database db(dbPath, dbName, GetDefaultDBOptions());
    string filePath = GetSchemaFilePath("tweet.bfbs");
    string schema = File::Read(filePath);
    std::vector<IndexInfo> indexes;
    indexes.push_back(IndexInfo("IndexName1", IndexType::EWAH_COMPRESSED_BITMAP, "id", true));
    indexes.push_back(IndexInfo("IndexName2", IndexType::EWAH_COMPRESSED_BITMAP, "text", true));
    indexes.push_back(IndexInfo("IndexName3", IndexType::EWAH_COMPRESSED_BITMAP, "user.id", true));
    indexes.push_back(IndexInfo("IndexName4", IndexType::EWAH_COMPRESSED_BITMAP, "user.name", true));

    db.CreateCollection(collectionName1, SchemaType::FLAT_BUFFERS, schema, indexes);
    db.CreateCollection(collectionName2, SchemaType::FLAT_BUFFERS, schema, indexes);

    std::vector<Buffer> documents;
    for (size_t i = 0; i < 10; i++) {
      std::string name = "zarian_" + std::to_string(i);
      std::string text = "hello_" + std::to_string(i);
      documents.push_back(GetTweetObject2(i, i, &name, &text, (double)i));
    }

    db.MultiInsert(collectionName1, documents);
    db.MultiInsert(collectionName2, documents);
    // db will be closed on next line because of scope
  }

  //lets reopen the db
  Options opt = GetDefaultDBOptions();
  opt.SetCreateDBIfMissing(false);
  Database db(dbPath, dbName, opt);

  // Now see if we can read all the inserted data correctly
  std::string sqlStmt = "SELECT id, text, [user.id], [user.name] FROM " + collectionName1 + ";";
  auto rs = db.ExecuteSelect(sqlStmt);
  auto rowCnt = 0;
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), rowCnt);
    std::string text = "hello_" + std::to_string(rowCnt);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("text")).str(), text.c_str());
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("user.id")), rowCnt);
    std::string name = "zarian_" + std::to_string(rowCnt);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("user.name")).str(), name.c_str());
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 10);

  // Now check collection2
  sqlStmt = "SELECT id, text, [user.id], [user.name] FROM " + collectionName2 + ";";
  rs = db.ExecuteSelect(sqlStmt);
  rowCnt = 0;
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), rowCnt);
    std::string text = "hello_" + std::to_string(rowCnt);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("text")).str(), text.c_str());
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("user.id")), rowCnt);
    std::string name = "zarian_" + std::to_string(rowCnt);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("user.name")).str(), name.c_str());
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 10);
}

TEST(Database, ExecuteSelect_Indexed_LessThanInteger) {
  Database db(g_TestRootDirectory, "ExecuteSelect_LessThanInteger", GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes{ IndexInfo("IndexName1", IndexType::EWAH_COMPRESSED_BITMAP, "id", true) };
  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    documents.push_back(GetTweetObject2(i, i, &name, &text, (double)i));
  }
  db.MultiInsert("tweet", documents);

  int rowCnt = 0;
  ResultSet rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name] FROM tweet WHERE id < 5;");
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), rowCnt);
    std::string text = "hello_" + std::to_string(rowCnt);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("text")).str(), text.c_str());    
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("user.id")), rowCnt);
    std::string name = "zarian_" + std::to_string(rowCnt);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("user.name")).str(), name.c_str());    
    rowCnt++;
  }

  ASSERT_EQ(rowCnt, 5);
}

TEST(Database, ExecuteSelect_VectorIndexer) {
  Database db(g_TestRootDirectory, "ExecuteSelect_VectorIndexer", GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes{ IndexInfo("IndexName1", IndexType::VECTOR, "id", true),
    IndexInfo("IndexName2", IndexType::VECTOR, "rating", true),
    IndexInfo("IndexName3", IndexType::VECTOR, "user.id", true) };
  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    documents.push_back(GetTweetObject2(i, i, &name, &text, (double)i));
  }
  db.MultiInsert("tweet", documents);

  int rowCnt = 0;
  ResultSet rs = db.ExecuteSelect("SELECT id, rating FROM tweet WHERE id < 5;");
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), rowCnt);
    ASSERT_EQ(rs.GetDouble(rs.GetColumnIndex("rating")), double(rowCnt));
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 5);

  rowCnt = 0;
  rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating FROM tweet WHERE rating < 5.0;");
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), rowCnt);
    ASSERT_EQ(rs.GetDouble(rs.GetColumnIndex("rating")), double(rowCnt));
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 5);

  rowCnt = 0;
  rs = db.ExecuteSelect("SELECT id, rating, [user.id] FROM tweet WHERE [user.id] < 5;");
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), rowCnt);
    ASSERT_EQ(rs.GetDouble(rs.GetColumnIndex("rating")), double(rowCnt));
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("user.id")), rowCnt);
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 5);
}

void ExecuteAndValidateResultset(Database& db, const std::string& fieldName,
                                 const std::string& op, const std::string& valueToCompare,
                                 int expectedRowCount) {
  int rowCnt = 0;
  std::string sqlStr = "SELECT * FROM all_field_collection WHERE ";
  sqlStr.append(fieldName).append(" ").append(op)
    .append(" ").append(valueToCompare).append(";");

  ResultSet rs = db.ExecuteSelect(sqlStr);
  while (rs.Next()) {
    rowCnt++;
  }
  ASSERT_EQ(expectedRowCount, rowCnt);
}

TEST(Database, ExecuteSelect_LT_LTE) {
  Database db(g_TestRootDirectory, "ExecuteSelect_LT_LTE", GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("all_field_type.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes{};
  db.CreateCollection("all_field_collection", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string str = std::to_string(i);
    documents.push_back(GetAllFieldTypeObjectBuffer(static_cast<int8_t>(i), static_cast<uint8_t>(i),
                                                    true, static_cast<int16_t>(i),
                                                    static_cast<uint16_t>(i), static_cast<int32_t>(i),
                                                    static_cast<uint32_t>(i), (float)i, static_cast<int64_t>(i),
                                                    (double)i, str));
  }

  db.MultiInsert("all_field_collection", documents);    

  const size_t fieldCount = 11;
  for (size_t i = 1; i < fieldCount; i++) {
    if (i == 3) {
      // field 3 is bool and this test does not make sense for bool
      continue;
    }
    std::string fieldName = "field" + std::to_string(i);
    ExecuteAndValidateResultset(db, fieldName, "<", "0", 0);
    ExecuteAndValidateResultset(db, fieldName, "<", "10", 10);
    ExecuteAndValidateResultset(db, fieldName, "<", "5", 5);
  }

  ExecuteAndValidateResultset(db, "field11", "<", "'0'", 0);
  ExecuteAndValidateResultset(db, "field11", "<", "'99'", 10);
  ExecuteAndValidateResultset(db, "field11", "<", "'5'", 5);    
 
  for (size_t i = 1; i < fieldCount; i++) {
    if (i == 3) {
      // field 3 is bool and this test does not make sense for bool
      continue;
    }
    std::string fieldName = "[nestedField.field" + std::to_string(i) + "]";
    ExecuteAndValidateResultset(db, fieldName, "<", "0", 0);
    ExecuteAndValidateResultset(db, fieldName, "<", "10", 10);
    ExecuteAndValidateResultset(db, fieldName, "<", "5", 5);
  }

  ExecuteAndValidateResultset(db, "[nestedField.field11]", "<", "'0'", 0);
  ExecuteAndValidateResultset(db, "[nestedField.field11]", "<", "'99'", 10);
  ExecuteAndValidateResultset(db, "[nestedField.field11]", "<", "'5'", 5);

  // Now test the same thing with <= operator
  for (size_t i = 1; i < fieldCount; i++) {
    if (i == 3) {
      // field 3 is bool and this test does not make sense for bool
      continue;
    }
    std::string fieldName = "field" + std::to_string(i);
    ExecuteAndValidateResultset(db, fieldName, "<=", "0", 1);
    ExecuteAndValidateResultset(db, fieldName, "<=", "9", 10);
    ExecuteAndValidateResultset(db, fieldName, "<=", "5", 6);
  }

  ExecuteAndValidateResultset(db, "field11", "<=", "'0'", 1);
  ExecuteAndValidateResultset(db, "field11", "<=", "'9'", 10);
  ExecuteAndValidateResultset(db, "field11", "<=", "'5'", 6);

  for (size_t i = 1; i < fieldCount; i++) {
    if (i == 3) {
      // field 3 is bool and this test does not make sense for bool
      continue;
    }
    std::string fieldName = "[nestedField.field" + std::to_string(i) + "]";
    ExecuteAndValidateResultset(db, fieldName, "<=", "0", 1);
    ExecuteAndValidateResultset(db, fieldName, "<=", "9", 10);
    ExecuteAndValidateResultset(db, fieldName, "<=", "5", 6);
  }

  ExecuteAndValidateResultset(db, "[nestedField.field11]", "<=", "'0'", 1);
  ExecuteAndValidateResultset(db, "[nestedField.field11]", "<=", "'99'", 10);
  ExecuteAndValidateResultset(db, "[nestedField.field11]", "<=", "'5'", 6);
}

TEST(Database, ExecuteSelect_GT_GTE) {
  Database db(g_TestRootDirectory, "ExecuteSelect_GT_GTE", GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("all_field_type.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes{};
  db.CreateCollection("all_field_collection", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string str = std::to_string(i);
    documents.push_back(GetAllFieldTypeObjectBuffer(static_cast<int8_t>(i), static_cast<uint8_t>(i),
                                                    true, static_cast<int16_t>(i),
                                                    static_cast<uint16_t>(i), static_cast<int32_t>(i),
                                                    static_cast<uint32_t>(i), (float)i, static_cast<int64_t>(i),
                                                    (double)i, str));
  }

  db.MultiInsert("all_field_collection", documents);

  const size_t fieldCount = 11;
  for (size_t i = 1; i < fieldCount; i++) {
    if (i == 3) {
      // field 3 is bool and this test does not make sense for bool
      continue;
    }
    std::string fieldName = "field" + std::to_string(i);
    ExecuteAndValidateResultset(db, fieldName, ">", "-1", 10);
    ExecuteAndValidateResultset(db, fieldName, ">", "10", 0);
    ExecuteAndValidateResultset(db, fieldName, ">", "5", 4);
  }

  ExecuteAndValidateResultset(db, "field11", ">", "''", 10);
  ExecuteAndValidateResultset(db, "field11", ">", "'99'", 0);
  ExecuteAndValidateResultset(db, "field11", ">", "'5'", 4);
    
  for (size_t i = 1; i < fieldCount; i++) {
    if (i == 3) {
      // field 3 is bool and this test does not make sense for bool
      continue;
    }
    std::string fieldName = "[nestedField.field" + std::to_string(i) + "]";
    ExecuteAndValidateResultset(db, fieldName, ">", "-1", 10);
    ExecuteAndValidateResultset(db, fieldName, ">", "10", 0);
    ExecuteAndValidateResultset(db, fieldName, ">", "5", 4);
  }

  ExecuteAndValidateResultset(db, "[nestedField.field11]", ">", "''", 10);
  ExecuteAndValidateResultset(db, "[nestedField.field11]", ">", "'99'", 0);
  ExecuteAndValidateResultset(db, "[nestedField.field11]", ">", "'5'", 4);

  // Now execute these tests with >= operator
  for (size_t i = 1; i < fieldCount; i++) {
    if (i == 3) {
      // field 3 is bool and this test does not make sense for bool
      continue;
    }
    std::string fieldName = "field" + std::to_string(i);
    ExecuteAndValidateResultset(db, fieldName, ">=", "0", 10);
    ExecuteAndValidateResultset(db, fieldName, ">=", "10", 0);
    ExecuteAndValidateResultset(db, fieldName, ">=", "5", 5);
  }

  ExecuteAndValidateResultset(db, "field11", ">=", "''", 10);
  ExecuteAndValidateResultset(db, "field11", ">=", "'99'", 0);
  ExecuteAndValidateResultset(db, "field11", ">=", "'5'", 5);

  for (size_t i = 1; i < fieldCount; i++) {
    if (i == 3) {
      // field 3 is bool and this test does not make sense for bool
      continue;
    }
    std::string fieldName = "[nestedField.field" + std::to_string(i) + "]";
    ExecuteAndValidateResultset(db, fieldName, ">=", "0", 10);
    ExecuteAndValidateResultset(db, fieldName, ">=", "10", 0);
    ExecuteAndValidateResultset(db, fieldName, ">=", "5", 5);
  }

  ExecuteAndValidateResultset(db, "[nestedField.field11]", ">=", "''", 10);
  ExecuteAndValidateResultset(db, "[nestedField.field11]", ">=", "'99'", 0);
  ExecuteAndValidateResultset(db, "[nestedField.field11]", ">=", "'5'", 5);
}

TEST(Database, ExecuteSelect_VECTORIndexed_DoubleExpression) {
  Database db(g_TestRootDirectory, "ExecuteSelect_VECTORIndexed_DoubleExpression", GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes{ IndexInfo("IndexName1", IndexType::VECTOR, "rating", true) };
  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    documents.push_back(GetTweetObject2(i, i, &name, &text, (double)i/100.0));
  }
  db.MultiInsert("tweet", documents);

  int rowCnt = 0;
  auto rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                             "FROM tweet WHERE rating "
                             "between round(0.06 - 0.01, 2) and round(0.06 + 0.01, 2);");
  double ratingVal = 0.05;
  while (rs.Next()) {
    ASSERT_DOUBLE_EQ(ratingVal, rs.GetDouble(rs.GetColumnIndex("rating")));
    ratingVal += 0.01;
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 3);  
}
TEST(Database, ExecuteSelect_DoubleExpression) {
  Database db(g_TestRootDirectory, "ExecuteSelect_DoubleExpression", GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes;
  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    documents.push_back(GetTweetObject2(i, i, &name, &text, (double)i / 100.0));
  }
  db.MultiInsert("tweet", documents);

  int rowCnt = 0;
  auto rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                             "FROM tweet WHERE rating "
                             "between round(0.06 - 0.01, 2) and round(0.06 + 0.01, 2);");
  double ratingVal = 0.05;
  while (rs.Next()) {
    ASSERT_DOUBLE_EQ(ratingVal, rs.GetDouble(rs.GetColumnIndex("rating")));
    ratingVal += 0.01;
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 3);
}

TEST(Database, ExecuteSelect_EWAHIndexed_String_GTE) {
  Database db(g_TestRootDirectory, "ExecuteSelect_EWAHIndexed_String_GTE", GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes{ IndexInfo("IndexName1", IndexType::EWAH_COMPRESSED_BITMAP, "user.name", true) };
  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    documents.push_back(GetTweetObject2(i, i, &name, &text, (double)i / 100.0));
  }
  db.MultiInsert("tweet", documents);

  auto rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                             "FROM tweet WHERE [user.name] >= 'zarian_3';");
  int counter = 3;
  while (rs.Next()) {
    std::string expectedName = "zarian_" + std::to_string(counter);
    ASSERT_STREQ(expectedName.c_str(), rs.GetString(rs.GetColumnIndex("user.name")).str());
    counter++;
  }
  ASSERT_EQ(counter, 10);
}

void ValidateTweetResultSet(Database& db, int lowerCount, int upperCount,
                            const std::string& lowerColName, const std::string& lowerOp, const std::string& lowerColVal,
                            const std::string& upperColName, const std::string& upperOp, const std::string& upperColVal) {
  std::string sql = "SELECT id, text, [user.id], [user.name], rating FROM tweet WHERE ";
  sql.append(lowerColName).append(" ").append(lowerOp).append(" ").
    append(lowerColVal).append(" AND ").append(upperColName).append(" ").
    append(upperOp).append(" ").append(upperColVal).append(";");
  auto rs = db.ExecuteSelect(sql);
  while (rs.Next()) {
    std::string expectedName = "zarian_" + std::to_string(lowerCount);
    ASSERT_STREQ(expectedName.c_str(), rs.GetString(rs.GetColumnIndex("user.name")).str());
    ASSERT_EQ(lowerCount, rs.GetInteger(rs.GetColumnIndex("id")));
    ASSERT_DOUBLE_EQ(double(lowerCount), rs.GetDouble(rs.GetColumnIndex("rating")));
    ASSERT_EQ(lowerCount, rs.GetInteger(rs.GetColumnIndex("user.id")));
    lowerCount++;
  }
  ASSERT_EQ(upperCount, lowerCount);
}

TEST(Database, ExecuteSelect_EWAHIndexed_Range) {
  Database db(g_TestRootDirectory, "ExecuteSelect_EWAHIndexed_Range", GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes{ IndexInfo("IndexName1", IndexType::EWAH_COMPRESSED_BITMAP, "id", true),
    IndexInfo("IndexName2", IndexType::EWAH_COMPRESSED_BITMAP, "rating", true),
    IndexInfo("IndexName3", IndexType::EWAH_COMPRESSED_BITMAP, "user.id", true),
    IndexInfo("IndexName4", IndexType::EWAH_COMPRESSED_BITMAP, "user.name", true) };
  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    documents.push_back(GetTweetObject2(i, i, &name, &text, static_cast<double>(i)));
  }
  db.MultiInsert("tweet", documents);

  ValidateTweetResultSet(db, 3, 8, "[user.name]", ">=", "'zarian_3'", "[user.name]", "<=", "'zarian_7'");
  ValidateTweetResultSet(db, 3, 7, "[user.name]", ">=", "'zarian_3'", "[user.name]", "<", "'zarian_7'");
  ValidateTweetResultSet(db, 4, 8, "[user.name]", ">", "'zarian_3'", "[user.name]", "<=", "'zarian_7'");
  ValidateTweetResultSet(db, 4, 7, "[user.name]", ">", "'zarian_3'", "[user.name]", "<", "'zarian_7'");

  ValidateTweetResultSet(db, 3, 8, "id", ">=", "3", "id", "<=", "7");
  ValidateTweetResultSet(db, 3, 7, "id", ">=", "3", "id", "<", "7");
  ValidateTweetResultSet(db, 4, 8, "id", ">", "3", "id", "<=", "7");
  ValidateTweetResultSet(db, 4, 7, "id", ">", "3", "id", "<", "7");

  ValidateTweetResultSet(db, 3, 8, "[user.id]", ">=", "3", "[user.id]", "<=", "7");
  ValidateTweetResultSet(db, 3, 7, "[user.id]", ">=", "3", "[user.id]", "<", "7");
  ValidateTweetResultSet(db, 4, 8, "[user.id]", ">", "3", "[user.id]", "<=", "7");
  ValidateTweetResultSet(db, 4, 7, "[user.id]", ">", "3", "[user.id]", "<", "7");

  ValidateTweetResultSet(db, 3, 8, "rating", ">=", "3.0", "rating", "<=", "7.0");
  ValidateTweetResultSet(db, 3, 7, "rating", ">=", "3.0", "rating", "<", "7.0");
  ValidateTweetResultSet(db, 4, 8, "rating", ">", "3.0", "rating", "<=", "7.0");
  ValidateTweetResultSet(db, 4, 7, "rating", ">", "3.0", "rating", "<", "7.0"); 
}

TEST(Database, ExecuteSelect_VECTORIndexed_Range) {
  Database db(g_TestRootDirectory, "ExecuteSelect_VECTORIndexed_Range", GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  // Todo: Enable tests for string Vector indexer once we have implemented that.
  std::vector<IndexInfo> indexes{ IndexInfo("IndexName1", IndexType::VECTOR, "id", true),
    IndexInfo("IndexName2", IndexType::VECTOR, "rating", true),
    IndexInfo("IndexName3", IndexType::VECTOR, "user.id", true),
    IndexInfo("IndexName4", IndexType::VECTOR, "user.name", true) };
  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    documents.push_back(GetTweetObject2(i, i, &name, &text, static_cast<double>(i)));
  }
  db.MultiInsert("tweet", documents);

  ValidateTweetResultSet(db, 3, 8, "[user.name]", ">=", "'zarian_3'", "[user.name]", "<=", "'zarian_7'");
  ValidateTweetResultSet(db, 3, 7, "[user.name]", ">=", "'zarian_3'", "[user.name]", "<", "'zarian_7'");
  ValidateTweetResultSet(db, 4, 8, "[user.name]", ">", "'zarian_3'", "[user.name]", "<=", "'zarian_7'");
  ValidateTweetResultSet(db, 4, 7, "[user.name]", ">", "'zarian_3'", "[user.name]", "<", "'zarian_7'");

  ValidateTweetResultSet(db, 3, 8, "id", ">=", "3", "id", "<=", "7");
  ValidateTweetResultSet(db, 3, 7, "id", ">=", "3", "id", "<", "7");
  ValidateTweetResultSet(db, 4, 8, "id", ">", "3", "id", "<=", "7");
  ValidateTweetResultSet(db, 4, 7, "id", ">", "3", "id", "<", "7");

  ValidateTweetResultSet(db, 3, 8, "[user.id]", ">=", "3", "[user.id]", "<=", "7");
  ValidateTweetResultSet(db, 3, 7, "[user.id]", ">=", "3", "[user.id]", "<", "7");
  ValidateTweetResultSet(db, 4, 8, "[user.id]", ">", "3", "[user.id]", "<=", "7");
  ValidateTweetResultSet(db, 4, 7, "[user.id]", ">", "3", "[user.id]", "<", "7");

  ValidateTweetResultSet(db, 3, 8, "rating", ">=", "3.0", "rating", "<=", "7.0");
  ValidateTweetResultSet(db, 3, 7, "rating", ">=", "3.0", "rating", "<", "7.0");
  ValidateTweetResultSet(db, 4, 8, "rating", ">", "3.0", "rating", "<=", "7.0");
  ValidateTweetResultSet(db, 4, 7, "rating", ">", "3.0", "rating", "<", "7.0");
}

TEST(Database, ExecuteSelect_ScanForIDSeq) {
  // This test checks the boundary conditions for IDSeq
  std::vector<int> idCounts = {0, 1, 50, 100, 101, 200, 201};
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);

  for (auto idCnt: idCounts) {
    string dbName = "ExecuteSelect_ScanForIDSeq_" + to_string(idCnt);    
    Database db(dbPath, dbName, GetDefaultDBOptions());    
    std::vector<IndexInfo> indexes;    
    db.CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);

    std::vector<Buffer> documents;
    std::string text = "hello";
    std::string name = "zarian";
    for (size_t i = 0; i < idCnt; i++) {      
      documents.push_back(GetTweetObject2(i, i, &name, &text, (double)i));      
    }

    db.MultiInsert(collectionName, documents);

    auto rs = db.ExecuteSelect("SELECT id FROM tweet;");
    auto rowCnt = 0;    
    while (rs.Next()) {
      ASSERT_EQ(rowCnt, rs.GetInteger(rs.GetColumnIndex("id")));
      rowCnt++;
    }
    ASSERT_EQ(idCnt, rowCnt);
  }  
}

TEST(Database, ExecuteSelect_Aggregation_Indexed) {
  // This test checks the boundary conditions for IDSeq
  std::vector<int> idCounts = { 0, 1, 50, 100, 101, 200, 201 };
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);

  for (auto idCnt : idCounts) {
    string dbName = "ExecuteSelect_Aggregation_Indexed_" + to_string(idCnt);
    Database db(dbPath, dbName, GetDefaultDBOptions());    
    std::vector<IndexInfo> indexes { IndexInfo("IndexName1", IndexType::VECTOR, "id", true),
      IndexInfo("IndexName2", IndexType::VECTOR, "rating", true),
      IndexInfo("IndexName3", IndexType::VECTOR, "user.id", true),
      IndexInfo("IndexName4", IndexType::VECTOR, "user.name", true) };
    db.CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);

    std::vector<Buffer> documents;
    std::string text = "hello";
    std::string name = "zarian";
    std::int64_t expectedSum = 0;
    for (size_t i = 0; i < idCnt; i++) {
      documents.push_back(GetTweetObject2(i, i, &name, &text, (double)i));
      expectedSum += i;
    }

    db.MultiInsert(collectionName, documents);

    auto rs = db.ExecuteSelect(
      "SELECT SUM(id) as sum_id, SUM(rating) as sum_rating, "
      "SUM([user.id]) as sum_user_id, "
      "AVG(id) as avg_id, AVG(rating) as avg_rating, "
      "AVG([user.id]) as avg_user_id "
      "FROM tweet;");
    auto rowCnt = 0;
    while (rs.Next()) {
      ASSERT_EQ(expectedSum, rs.GetInteger(rs.GetColumnIndex("sum_id")));
      ASSERT_DOUBLE_EQ((double)expectedSum, rs.GetDouble(rs.GetColumnIndex("sum_rating")));
      ASSERT_EQ(expectedSum, rs.GetInteger(rs.GetColumnIndex("sum_user_id")));
      if (idCnt > 0) {
        ASSERT_EQ(expectedSum / idCnt, rs.GetInteger(rs.GetColumnIndex("avg_id")));
        ASSERT_DOUBLE_EQ((double)expectedSum / (double)idCnt, rs.GetDouble(rs.GetColumnIndex("avg_rating")));
        ASSERT_EQ(expectedSum / idCnt, rs.GetInteger(rs.GetColumnIndex("avg_user_id")));
      }
      rowCnt++;
    }
    ASSERT_EQ(1, rowCnt);
  }
}

TEST(Database, ExecuteSelect_Aggregation) {
  // This test checks the boundary conditions for IDSeq
  std::vector<int> idCounts = { 0, 1, 50, 100, 101, 200, 201 };
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);

  for (auto idCnt : idCounts) {
    string dbName = "ExecuteSelect_Aggregation_" + to_string(idCnt);
    Database db(dbPath, dbName, GetDefaultDBOptions());
    std::vector<IndexInfo> indexes;
    db.CreateCollection(collectionName, SchemaType::FLAT_BUFFERS, schema, indexes);

    std::vector<Buffer> documents;
    std::string text = "hello";
    std::string name = "zarian";
    std::int64_t expectedSum = 0;
    for (size_t i = 0; i < idCnt; i++) {
      documents.push_back(GetTweetObject2(i, i, &name, &text, (double)i));
      expectedSum += i;
    }

    db.MultiInsert(collectionName, documents);

    auto rs = db.ExecuteSelect(
      "SELECT SUM(id) as sum_id, SUM(rating) as sum_rating, "
      "SUM([user.id]) as sum_user_id, "
      "AVG(id) as avg_id, AVG(rating) as avg_rating, "
      "AVG([user.id]) as avg_user_id "
      "FROM tweet;");
    auto rowCnt = 0;
    while (rs.Next()) {
      ASSERT_EQ(expectedSum, rs.GetInteger(rs.GetColumnIndex("sum_id")));
      ASSERT_DOUBLE_EQ((double)expectedSum, rs.GetDouble(rs.GetColumnIndex("sum_rating")));
      ASSERT_EQ(expectedSum, rs.GetInteger(rs.GetColumnIndex("sum_user_id")));
      if (idCnt > 0) {
        ASSERT_EQ(expectedSum / idCnt, rs.GetInteger(rs.GetColumnIndex("avg_id")));
        ASSERT_DOUBLE_EQ((double)expectedSum / (double)idCnt, rs.GetDouble(rs.GetColumnIndex("avg_rating")));
        ASSERT_EQ(expectedSum / idCnt, rs.GetInteger(rs.GetColumnIndex("avg_user_id")));
      }
      rowCnt++;
    }
    ASSERT_EQ(1, rowCnt);
  }
}

TEST(Database, ExecuteSelect_NullStrFields) {
  Database db(g_TestRootDirectory, "ExecuteSelect_NullStrFields", GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes;
  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    if (i % 2 == 0) {
      documents.push_back(GetTweetObject2(i, i, &name, &text, (double)i / 100.0));
    } else {
      documents.push_back(GetTweetObject2(i, i, nullptr, nullptr, (double)i / 100.0));
    }
  }
  db.MultiInsert("tweet", documents);

  auto rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                             "FROM tweet;");
  int rowCnt = 0;
  while (rs.Next()) {
    if (rowCnt % 2 == 0) {
      ASSERT_FALSE(rs.IsNull(rs.GetColumnIndex("user.name")));
      ASSERT_FALSE(rs.IsNull(rs.GetColumnIndex("text")));
      std::string expectedName = "zarian_" + std::to_string(rowCnt);
      std::string expectedText = "hello_" + std::to_string(rowCnt);
      ASSERT_STREQ(expectedName.c_str(), rs.GetString(rs.GetColumnIndex("user.name")).str());
      ASSERT_STREQ(expectedText.c_str(), rs.GetString(rs.GetColumnIndex("text")).str());
    } else {
      ASSERT_TRUE(rs.IsNull(rs.GetColumnIndex("user.name")));
      ASSERT_TRUE(rs.IsNull(rs.GetColumnIndex("text")));
      ASSERT_STREQ("", rs.GetString(rs.GetColumnIndex("user.name")).str());
      ASSERT_STREQ("", rs.GetString(rs.GetColumnIndex("text")).str());
    }
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

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
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
