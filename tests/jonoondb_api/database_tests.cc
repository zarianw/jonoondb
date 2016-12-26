#include <string>
#include <fstream>
#include <cstdio>
#include "gtest/gtest.h"
#include "flatbuffers/flatbuffers.h"
#include "test_utils.h"
#include "jonoondb_api_vx_test_utils.h"
#include "database.h"
#include "enums.h"
#include "buffer_impl.h"
#include "tweet_generated.h"
#include "file.h"

using namespace std;
using namespace flatbuffers;
using namespace jonoondb_api;
using namespace jonoondb_test;
using namespace jonoondb_api_vx_test;

TEST(Database, Ctor_InvalidArguments) {
  Options options;
  ASSERT_THROW(Database db("somePath", "", options), InvalidArgumentException);
  ASSERT_THROW(Database db("", "someDbName", options),
               InvalidArgumentException);
}

TEST(Database, Ctor_MissingDatabaseFile) {
  string dbName = "Database_Ctor_MissingDatabaseFile";
  string dbPath = g_TestRootDirectory;
  Options options;
  options.SetCreateDBIfMissing(false);
  ASSERT_THROW(Database db(dbPath, dbName, options),
               MissingDatabaseFileException);
}

TEST(Database, Ctor_MissingDatabaseFolder) {
  string dbName = "Database_Ctor_MissingDatabaseFolder";
  string dbPath = g_TestRootDirectory;
  Options options;
  ASSERT_THROW(Database db(dbPath + "missing_folder", dbName, options),
               MissingDatabaseFolderException);
}

TEST(Database, Ctor_New) {
  string dbName = "Database_Ctor_New";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());
}

TEST(Database, Ctor_Existing) {
  string dbName = "Database_Ctor_Existing";
  string dbPath = g_TestRootDirectory;
  {
    Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());
  }

  {
    Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());
  }
}

TEST(Database, Ctor2_New) {
  string dbName = "Database_Ctor2_New";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName);
}

TEST(Database, Ctor2_Existing) {
  string dbName = "Database_Ctor2_Existing";
  string dbPath = g_TestRootDirectory;
  {
    Database db(dbPath, dbName);
  }

  {
    Database db(dbPath, dbName);
  }
}

TEST(Database, Ctor_CreateIfMissing) {
  Database db(g_TestRootDirectory,
              "Database_Ctor_CreateIfMissing",
              TestUtils::GetDefaultDBOptions());
}

TEST(Database, CreateCollection_InvalidSchema) {
  string dbName = "Database_CreateCollection_InvalidSchema";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());
  std::vector<IndexInfo> indexes;
  ASSERT_THROW(db.CreateCollection("CollectionName",
                                   SchemaType::FLAT_BUFFERS,
                                   "Schema IDL",
                                   indexes), InvalidSchemaException);
}

TEST(Database, CreateCollection_New) {
  string dbName = "Database_CreateCollection_New";
  string dbPath = g_TestRootDirectory;
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());
  std::vector<IndexInfo> indexes;
  db.CreateCollection("CollectionName",
                      SchemaType::FLAT_BUFFERS,
                      schema,
                      indexes);
}

TEST(Database, CreateCollection_CollectionAlreadyExist) {
  string dbName = "Database_CreateCollection_CollectionAlreadyExist";
  string dbPath = g_TestRootDirectory;
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());
  std::vector<IndexInfo> indexes;
  db.CreateCollection("CollectionName",
                      SchemaType::FLAT_BUFFERS,
                      schema,
                      indexes);
  ASSERT_THROW(db.CreateCollection("CollectionName",
                                   SchemaType::FLAT_BUFFERS,
                                   schema,
                                   indexes), CollectionAlreadyExistException);
}

TEST(Database, CreateCollection_DuplicateIndex) {
  string dbName = "Database_CreateCollection_DuplicateIndex";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes;
  indexes.push_back(IndexInfo("index1",
                              IndexType::INVERTED_COMPRESSED_BITMAP,
                              "id",
                              true));
  indexes.push_back(IndexInfo("index1",
                              IndexType::INVERTED_COMPRESSED_BITMAP,
                              "user.name",
                              true));
  ASSERT_THROW(db.CreateCollection(collectionName,
                                   SchemaType::FLAT_BUFFERS,
                                   schema,
                                   indexes), IndexAlreadyExistException);

  // Now try to create the collection without duplicate index
  indexes.pop_back();
  ASSERT_NO_THROW(db.CreateCollection(collectionName,
                                      SchemaType::FLAT_BUFFERS,
                                      schema,
                                      indexes));
}

TEST(Database, Insert_NoIndex) {
  string dbName = "Database_Insert_NoIndex";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);

  std::vector<IndexInfo> indexes;
  db.CreateCollection(collectionName,
                      SchemaType::FLAT_BUFFERS,
                      schema,
                      indexes);
  std::string name = "Zarian";
  std::string text = "Say hello to my little friend!";
  std::string binData = "some_data";
  Buffer documentData = TestUtils::TestUtils::GetTweetObject(1, 1, &name, &text, 2.0, &binData);
  db.Insert(collectionName, documentData);
}

TEST(Database, Insert_SingleIndex) {
  string dbName = "Database_Insert_SingleIndex";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  IndexInfo index;
  index.SetIndexName("IndexName1");
  index.SetType(IndexType::INVERTED_COMPRESSED_BITMAP);
  index.SetIsAscending(true);
  index.SetColumnName("user.name");
  std::vector<IndexInfo> indexes;
  indexes.push_back(index);

  db.CreateCollection(collectionName.c_str(), SchemaType::FLAT_BUFFERS,
                      schema, indexes);
  std::string name = "Zarian";
  std::string text = "Say hello to my little friend!";
  std::string binData = "some_data";
  Buffer documentData = TestUtils::GetTweetObject(1, 1, &name, &text, 2.0, &binData);

  db.Insert(collectionName, documentData);
}

vector<IndexInfo> CreateAllTypeIndexes(IndexType indexType) {
  vector<IndexInfo> indexes;
  const int indexLength = 26;
  IndexInfo index;
  for (auto i = 1; i <= indexLength; i++) {
    auto indexName = "IndexName_" + std::to_string(i);
    index.SetIndexName(indexName);
    index.SetType(indexType);
    index.SetIsAscending(true);
    string fieldName;
    if (i < 14) {
      fieldName = "field" + to_string(i);
    } else {
      fieldName = "nestedField.field" + to_string(i - 13);
    }
    index.SetColumnName(fieldName.c_str());

    indexes.push_back(index);
  }

  return indexes;
}

void Execute_Insert_AllIndexTypes_Test(const std::string& dbName,
                                       IndexType indexType, bool nullifyNestedField) {
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());

  string filePath = GetSchemaFilePath("all_field_type.bfbs");
  string schema = File::Read(filePath);
  auto indexes = CreateAllTypeIndexes(indexType);

  db.CreateCollection(collectionName,
                      SchemaType::FLAT_BUFFERS,
                      schema,
                      indexes);

  Buffer
      documentData = TestUtils::GetAllFieldTypeObjectBuffer(1, 2, true, 4, 5,
                                                            6, 7, 8.0f, 9,
                                                            10.0, "test",
                                                            "test1", "test2",
                                                            nullifyNestedField);
  db.Insert(collectionName, documentData);
}

TEST(Database, Insert_AllIndexTypes_EWAHCB) {
  string dbName = "Database_Insert_AllIndexTypes_EWAHCB";
  Execute_Insert_AllIndexTypes_Test(dbName, IndexType::INVERTED_COMPRESSED_BITMAP, false);
}

TEST(Database, Insert_AllIndexTypes_Vector) {
  string dbName = "Database_Insert_AllIndexTypes_Vector";
  Execute_Insert_AllIndexTypes_Test(dbName, IndexType::VECTOR, false);
}

TEST(Database, Insert_AllIndexTypes_EWAHCB_NestedNull) {
  string dbName = "Insert_AllIndexTypes_EWAHCB_NestedNull";
  Execute_Insert_AllIndexTypes_Test(dbName, IndexType::INVERTED_COMPRESSED_BITMAP, true);
}

TEST(Database, ExecuteSelect_MissingCollection) {
  string dbName = "Database_ExecuteSelect_MissingCollection";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  db.CreateCollection(collectionName,
                      SchemaType::FLAT_BUFFERS,
                      schema,
                      std::vector<IndexInfo>());
  ASSERT_ANY_THROW(ResultSet rs =
      db.ExecuteSelect("select * from missingTable where text = 'hello'"));
}

TEST(Database, ExecuteSelect_EmptyDB_NoIndex) {
  string dbName = "Database_ExecuteSelect_EmptyDB_NoIndex";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  db.CreateCollection(collectionName.c_str(),
                      SchemaType::FLAT_BUFFERS,
                      schema,
                      std::vector<IndexInfo>());

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
  Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes{IndexInfo("IndexName1",
                                           IndexType::INVERTED_COMPRESSED_BITMAP,
                                           "user.name",
                                           true)};
  db.CreateCollection(collectionName,
                      SchemaType::FLAT_BUFFERS,
                      schema,
                      indexes);

  std::string name = "Zarian";
  std::string text = "Say hello to my little friend!";
  std::string binData = "some_data";
  Buffer documentData = TestUtils::GetTweetObject(1, 1, &name, &text, 2.0, &binData);
  db.Insert(collectionName, documentData);

  int rows = 0;
  ResultSet
      rs = db.ExecuteSelect("select * from tweet where [user.name] = 'Zarian'");
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
  Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes;

  IndexInfo index;
  index.SetIndexName("IndexName1");
  index.SetType(IndexType::INVERTED_COMPRESSED_BITMAP);
  index.SetIsAscending(true);
  index.SetColumnName("user.name");
  indexes.push_back(index);

  index.SetIndexName("IndexName2");
  index.SetType(IndexType::INVERTED_COMPRESSED_BITMAP);
  index.SetIsAscending(true);
  index.SetColumnName("text");
  indexes.push_back(index);

  index.SetIndexName("IndexName3");
  index.SetType(IndexType::INVERTED_COMPRESSED_BITMAP);
  index.SetIsAscending(true);
  index.SetColumnName("id");
  indexes.push_back(index);
  db.CreateCollection(collectionName,
                      SchemaType::FLAT_BUFFERS,
                      schema,
                      indexes);

  std::string name = "Zarian";
  std::string text = "Say hello to my little friend!";
  std::string binData = "some_data";
  Buffer documentData = TestUtils::GetTweetObject(1, 1, &name, &text, 2.0, &binData);
  db.Insert(collectionName, documentData);

  int rows = 0;
  ResultSet rs = db.ExecuteSelect(
      "SELECT id, text, [user.id], [user.name], rating, binData FROM tweet WHERE id = 1;");
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), 1);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("text")).str(),
                 "Say hello to my little friend!");
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("user.id")), 1);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("user.name")).str(), "Zarian");
    ASSERT_DOUBLE_EQ(rs.GetDouble(rs.GetColumnIndex("rating")), 2.0);
    auto blob = rs.GetBlob(rs.GetColumnIndex("binData"));
    ASSERT_EQ(blob.GetLength(), binData.size());
    ASSERT_EQ(memcmp(blob.GetData(), binData.data(), blob.GetLength()), 0);
    ++rows;
  }
  ASSERT_EQ(rows, 1);

  rows = 0;
  rs = db.ExecuteSelect(
      "SELECT id, text, [user.id], [user.name] FROM tweet WHERE [user.name] = 'Zarian' AND text = 'hello'");
  while (rs.Next()) {
    ++rows;
  }
  ASSERT_EQ(rows, 0);

  rows = 0;
  rs = db.ExecuteSelect(
      "SELECT id, text, [user.id], [user.name] FROM tweet WHERE [user.name] = 'Zarian' OR text = 'hello'");
  while (rs.Next()) {
    ++rows;
  }
  ASSERT_EQ(rows, 1);

  rows = 0;
  rs = db.ExecuteSelect(
      "SELECT id, text, [user.id], [user.name] FROM tweet WHERE [user.name] = 'Zarian' AND text = 'Say hello to my little friend!'");
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), 1);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("text")).str(),
                 "Say hello to my little friend!");
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("user.id")), 1);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("user.name")).str(), "Zarian");
    ++rows;
  }
  ASSERT_EQ(rows, 1);

  rs.Close();
}

void ExecuteMultiInsertTest(std::string& dbName, bool enableCompression,
                            IndexType indexType) {
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  auto opt = TestUtils::GetDefaultDBOptions();
  Database db(dbPath, dbName, opt);

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes;

  IndexInfo index;
  index.SetIndexName("IndexName1");
  index.SetType(indexType);
  index.SetIsAscending(true);
  index.SetColumnName("user.name");
  indexes.push_back(index);

  index.SetIndexName("IndexName2");
  index.SetType(indexType);
  index.SetIsAscending(true);
  index.SetColumnName("text");
  indexes.push_back(index);

  index.SetIndexName("IndexName3");
  index.SetType(indexType);
  index.SetIsAscending(true);
  index.SetColumnName("id");
  indexes.push_back(index);
  db.CreateCollection(collectionName,
                      SchemaType::FLAT_BUFFERS,
                      schema,
                      indexes);

  index.SetIndexName("IndexName4");
  index.SetType(indexType);
  index.SetIsAscending(true);
  index.SetColumnName("binData");
  indexes.push_back(index);  

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {

    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    std::string binData = "some_data_" + std::to_string(i);
    documents.push_back(
        TestUtils::GetTweetObject(i, i, &name, &text, (double)i, &binData));
  }

  WriteOptions wo;
  wo.Compress(enableCompression);
  db.MultiInsert(collectionName, documents, wo);

  // Now see if they were inserted correctly
  auto rs = db.ExecuteSelect("SELECT [user.name], binData from tweet;");
  auto rowCnt = 0;
  while (rs.Next()) {
    std::string name = "zarian_" + std::to_string(rowCnt);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("user.name")).str(),
                 name.c_str());
    std::string binData = "some_data_" + std::to_string(rowCnt);
    auto blob = rs.GetBlob(rs.GetColumnIndex("binData"));
    ASSERT_EQ(blob.GetLength(), binData.size());
    ASSERT_EQ(memcmp(blob.GetData(), binData.data(), blob.GetLength()), 0);
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 10);
}

TEST(Database, MultiInsert) {
  string dbName = "Database_MultiInsert";
  ExecuteMultiInsertTest(dbName, false, IndexType::INVERTED_COMPRESSED_BITMAP);
}

TEST(Database, MultiInsert_Compressed) {
  string dbName = "Database_MultiInsert_Compressed";
  ExecuteMultiInsertTest(dbName, true, IndexType::INVERTED_COMPRESSED_BITMAP);
}

TEST(Database, MultiInsert_Vector) {
  string dbName = "Database_MultiInsert_Vector";
  ExecuteMultiInsertTest(dbName, false, IndexType::VECTOR);
}

void ExecuteCtor_ReopenTest(std::string& dbName, bool enableCompression,
                            IndexType indexType) {
  string collectionName1 = "tweet1";
  string collectionName2 = "tweet2";
  string dbPath = g_TestRootDirectory;

  {
    //scope for database
    auto opt = TestUtils::GetDefaultDBOptions();
    Database db(dbPath, dbName, opt);
    string filePath = GetSchemaFilePath("tweet.bfbs");
    string schema = File::Read(filePath);
    std::vector<IndexInfo> indexes;
    indexes.push_back(IndexInfo("IndexName1", indexType, "id", true));
    indexes.push_back(IndexInfo("IndexName2", indexType, "text", true));
    indexes.push_back(IndexInfo("IndexName3", indexType, "user.id", true));
    indexes.push_back(IndexInfo("IndexName4", indexType, "user.name", true));
    indexes.push_back(IndexInfo("IndexName5", indexType, "binData", true));

    db.CreateCollection(collectionName1,
                        SchemaType::FLAT_BUFFERS,
                        schema,
                        indexes);
    db.CreateCollection(collectionName2,
                        SchemaType::FLAT_BUFFERS,
                        schema,
                        indexes);

    std::vector<Buffer> documents;
    for (size_t i = 0; i < 10; i++) {
      std::string name = "zarian_" + std::to_string(i);
      std::string text = "hello_" + std::to_string(i);
      std::string binData = "some_data_" + std::to_string(i);
      documents.push_back(
          TestUtils::GetTweetObject(i, i, &name, &text, (double)i, &binData));
    }

    WriteOptions wo;
    wo.Compress(enableCompression);
    db.MultiInsert(collectionName1, documents, wo);
    db.MultiInsert(collectionName2, documents, wo);
    // db will be closed on next line because of scope
  }

  //lets reopen the db
  Options opt = TestUtils::GetDefaultDBOptions();
  opt.SetCreateDBIfMissing(false);
  Database db(dbPath, dbName, opt);

  // Now see if we can read all the inserted data correctly
  std::string sqlStmt =
      "SELECT id, text, [user.id], [user.name], rating, binData FROM " + collectionName1 + ";";
  auto rs = db.ExecuteSelect(sqlStmt);
  auto rowCnt = 0;
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), rowCnt);
    std::string text = "hello_" + std::to_string(rowCnt);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("text")).str(), text.c_str());
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("user.id")), rowCnt);
    std::string name = "zarian_" + std::to_string(rowCnt);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("user.name")).str(),
                 name.c_str());
    ASSERT_DOUBLE_EQ(rs.GetInteger(rs.GetColumnIndex("rating")), (double)rowCnt);
    std::string binData = "some_data_" + std::to_string(rowCnt);
    auto blob = rs.GetBlob(rs.GetColumnIndex("binData"));
    ASSERT_EQ(blob.GetLength(), binData.size());
    ASSERT_EQ(memcmp(blob.GetData(), binData.data(), blob.GetLength()), 0);
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 10);

  // Now check collection2
  sqlStmt =
      "SELECT id, text, [user.id], [user.name], rating, binData FROM " + collectionName2 + ";";
  rs = db.ExecuteSelect(sqlStmt);
  rowCnt = 0;
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), rowCnt);
    std::string text = "hello_" + std::to_string(rowCnt);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("text")).str(), text.c_str());
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("user.id")), rowCnt);
    std::string name = "zarian_" + std::to_string(rowCnt);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("user.name")).str(),
                 name.c_str());
    ASSERT_DOUBLE_EQ(rs.GetInteger(rs.GetColumnIndex("rating")), (double)rowCnt);
    std::string binData = "some_data_" + std::to_string(rowCnt);
    auto blob = rs.GetBlob(rs.GetColumnIndex("binData"));
    ASSERT_EQ(blob.GetLength(), binData.size());
    ASSERT_EQ(memcmp(blob.GetData(), binData.data(), blob.GetLength()), 0);
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 10);
}

TEST(Database, Ctor_ReOpen) {
  string dbName = "Database_Ctor_ReOpen";
  ExecuteCtor_ReopenTest(dbName, false, IndexType::INVERTED_COMPRESSED_BITMAP);
}

TEST(Database, Ctor_ReOpen_Compressed) {
  string dbName = "Ctor_ReOpen_Compressed";
  ExecuteCtor_ReopenTest(dbName, true, IndexType::INVERTED_COMPRESSED_BITMAP);
}

TEST(Database, Ctor_ReOpen_Vector) {
  string dbName = "Ctor_ReOpen_Vector";
  ExecuteCtor_ReopenTest(dbName, false, IndexType::VECTOR);
}

TEST(Database, ExecuteSelect_Indexed_LessThanInteger) {
  Database db(g_TestRootDirectory,
              "ExecuteSelect_LessThanInteger",
              TestUtils::GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes
      {IndexInfo("IndexName1", IndexType::INVERTED_COMPRESSED_BITMAP, "id", true)};
  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    std::string binData = "some_data_" + std::to_string(i);
    documents.push_back(
        TestUtils::GetTweetObject(i, i, &name, &text, (double)i, &binData));
  }
  db.MultiInsert("tweet", documents);

  int rowCnt = 0;
  ResultSet rs = db.ExecuteSelect(
      "SELECT id, text, [user.id], [user.name], rating, binData FROM tweet WHERE id < 5;");
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), rowCnt);
    std::string text = "hello_" + std::to_string(rowCnt);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("text")).str(), text.c_str());
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("user.id")), rowCnt);
    std::string name = "zarian_" + std::to_string(rowCnt);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("user.name")).str(),
                 name.c_str());
    ASSERT_DOUBLE_EQ(rs.GetInteger(rs.GetColumnIndex("rating")), (double)rowCnt);
    std::string binData = "some_data_" + std::to_string(rowCnt);
    auto blob = rs.GetBlob(rs.GetColumnIndex("binData"));
    ASSERT_EQ(blob.GetLength(), binData.size());
    ASSERT_EQ(memcmp(blob.GetData(), binData.data(), blob.GetLength()), 0);
    rowCnt++;
  }

  ASSERT_EQ(rowCnt, 5);
}

TEST(Database, ExecuteSelect_VectorIndexer) {
  Database db(g_TestRootDirectory,
              "ExecuteSelect_VectorIndexer",
              TestUtils::GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo>
      indexes{IndexInfo("IndexName1", IndexType::VECTOR, "id", true),
              IndexInfo("IndexName2", IndexType::VECTOR, "rating", true),
              IndexInfo("IndexName3", IndexType::VECTOR, "user.id", true)};
  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    std::string binData = "some_data_" + std::to_string(i);
    documents.push_back(
        TestUtils::GetTweetObject(i, i, &name, &text, (double)i, &binData));
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
  rs = db.ExecuteSelect(
      "SELECT id, text, [user.id], [user.name], rating FROM tweet WHERE rating < 5.0;");
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), rowCnt);
    ASSERT_EQ(rs.GetDouble(rs.GetColumnIndex("rating")), double(rowCnt));
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 5);

  rowCnt = 0;
  rs = db.ExecuteSelect(
      "SELECT id, rating, [user.id] FROM tweet WHERE [user.id] < 5;");
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), rowCnt);
    ASSERT_EQ(rs.GetDouble(rs.GetColumnIndex("rating")), double(rowCnt));
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("user.id")), rowCnt);
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 5);
}

void ExecuteAndValidateResultset(Database& db,
                                 const std::string& fieldName,
                                 const std::string& op,
                                 const std::string& valueToCompare,
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


void Execute_ExecuteSelect_LT_LTE_Test(Database& db, vector<IndexInfo>& indexes) {
  string filePath = GetSchemaFilePath("all_field_type.bfbs");
  string schema = File::Read(filePath);
  db.CreateCollection("all_field_collection",
                      SchemaType::FLAT_BUFFERS,
                      schema,
                      indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string str = std::to_string(i);
    documents.push_back(TestUtils::GetAllFieldTypeObjectBuffer(static_cast<int8_t>(i),
                                                               static_cast<uint8_t>(i),
                                                               true,
                                                               static_cast<int16_t>(i),
                                                               static_cast<uint16_t>(i),
                                                               static_cast<int32_t>(i),
                                                               static_cast<uint32_t>(i),
                                                               (float)i,
                                                               static_cast<int64_t>(i),
                                                               (double)i,
                                                               str, str, str));
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

  ExecuteAndValidateResultset(db, "field12", "<", "x'30'", 0); // x'30' = 0
  ExecuteAndValidateResultset(db, "field12", "<", "x'3939'", 10); // x'3939' = 99
  ExecuteAndValidateResultset(db, "field12", "<", "x'35'", 5); // x'35' = 5

  ExecuteAndValidateResultset(db, "field13", "<", "x'30'", 0); // x'30' = 0
  ExecuteAndValidateResultset(db, "field13", "<", "x'3939'", 10); // x'3939' = 99
  ExecuteAndValidateResultset(db, "field13", "<", "x'35'", 5); // x'35' = 5

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

  ExecuteAndValidateResultset(db, "[nestedField.field12]", "<", "x'30'", 0); // x'30' = 0
  ExecuteAndValidateResultset(db, "[nestedField.field12]", "<", "x'3939'", 10); // x'3939' = 99
  ExecuteAndValidateResultset(db, "[nestedField.field12]", "<", "x'35'", 5); // x'35' = 5

  ExecuteAndValidateResultset(db, "[nestedField.field13]", "<", "x'30'", 0); // x'30' = 0
  ExecuteAndValidateResultset(db, "[nestedField.field13]", "<", "x'3939'", 10); // x'3939' = 99
  ExecuteAndValidateResultset(db, "[nestedField.field13]", "<", "x'35'", 5); // x'35' = 5

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

  ExecuteAndValidateResultset(db, "field12", "<=", "x'30'", 1); // x'30' = 0
  ExecuteAndValidateResultset(db, "field12", "<=", "x'39'", 10); // x'39' = 9
  ExecuteAndValidateResultset(db, "field12", "<=", "x'35'", 6); // x'35' = 5

  ExecuteAndValidateResultset(db, "field13", "<=", "x'30'", 1); // x'30' = 0
  ExecuteAndValidateResultset(db, "field13", "<=", "x'39'", 10); // x'39' = 9
  ExecuteAndValidateResultset(db, "field13", "<=", "x'35'", 6); // x'35' = 5

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
  ExecuteAndValidateResultset(db, "[nestedField.field11]", "<=", "'9'", 10);
  ExecuteAndValidateResultset(db, "[nestedField.field11]", "<=", "'5'", 6);

  ExecuteAndValidateResultset(db, "[nestedField.field12]", "<=", "x'30'", 1); // x'30' = 0
  ExecuteAndValidateResultset(db, "[nestedField.field12]", "<=", "x'39'", 10); // x'39' = 9
  ExecuteAndValidateResultset(db, "[nestedField.field12]", "<=", "x'35'", 6); // x'35' = 5

  ExecuteAndValidateResultset(db, "[nestedField.field13]", "<=", "x'30'", 1); // x'30' = 0
  ExecuteAndValidateResultset(db, "[nestedField.field13]", "<=", "x'39'", 10); // x'39' = 9
  ExecuteAndValidateResultset(db, "[nestedField.field13]", "<=", "x'35'", 6); // x'35' = 5
}

TEST(Database, ExecuteSelect_LT_LTE) {
  Database
      db(g_TestRootDirectory, "ExecuteSelect_LT_LTE", TestUtils::GetDefaultDBOptions());  
  std::vector<IndexInfo> indexes{};
  Execute_ExecuteSelect_LT_LTE_Test(db, indexes);
}

TEST(Database, ExecuteSelect_LT_LTE_EWAHIndexed) {
  Database db(g_TestRootDirectory, "ExecuteSelect_LT_LTE_EWAHIndexed",
              TestUtils::GetDefaultDBOptions());
  auto indexes = CreateAllTypeIndexes(IndexType::INVERTED_COMPRESSED_BITMAP); 
  Execute_ExecuteSelect_LT_LTE_Test(db, indexes); 
}

TEST(Database, ExecuteSelect_LT_LTE_VECTORIndexed) {
  Database db(g_TestRootDirectory, "ExecuteSelect_LT_LTE_VECTORIndexed",
              TestUtils::GetDefaultDBOptions());
  auto indexes = CreateAllTypeIndexes(IndexType::VECTOR);
  Execute_ExecuteSelect_LT_LTE_Test(db, indexes);
}

void Execute_ExecuteSelect_GT_GTE_Test(Database& db,
                                       const vector<IndexInfo>& indexes) {
  string filePath = GetSchemaFilePath("all_field_type.bfbs");
  string schema = File::Read(filePath);  
  db.CreateCollection("all_field_collection",
                      SchemaType::FLAT_BUFFERS,
                      schema,
                      indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string str = std::to_string(i);
    documents.push_back(TestUtils::GetAllFieldTypeObjectBuffer(static_cast<int8_t>(i),
                                                               static_cast<uint8_t>(i),
                                                               true,
                                                               static_cast<int16_t>(i),
                                                               static_cast<uint16_t>(i),
                                                               static_cast<int32_t>(i),
                                                               static_cast<uint32_t>(i),
                                                               (float)i,
                                                               static_cast<int64_t>(i),
                                                               (double)i,
                                                               str, str, str));
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

  ExecuteAndValidateResultset(db, "field12", ">", "x'2f'", 10); // x'2f' = / (which is less than 0)
  ExecuteAndValidateResultset(db, "field12", ">", "x'3939'", 0); // x'3939' = 99
  ExecuteAndValidateResultset(db, "field12", ">", "x'35'", 4); // x'35' = 5 

  ExecuteAndValidateResultset(db, "field13", ">", "x'2f'", 10); // x'2f' = / (which is less than 0)
  ExecuteAndValidateResultset(db, "field13", ">", "x'3939'", 0); // x'3939' = 99
  ExecuteAndValidateResultset(db, "field13", ">", "x'35'", 4); // x'35' = 5 

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

  ExecuteAndValidateResultset(db, "[nestedField.field12]", ">", "x'2f'", 10); // x'2f' = / (which is less than 0)
  ExecuteAndValidateResultset(db, "[nestedField.field12]", ">", "x'3939'", 0); // x'3939' = 99
  ExecuteAndValidateResultset(db, "[nestedField.field12]", ">", "x'35'", 4); // x'35' = 5 

  ExecuteAndValidateResultset(db, "[nestedField.field13]", ">", "x'2f'", 10); // x'2f' = / (which is less than 0)
  ExecuteAndValidateResultset(db, "[nestedField.field13]", ">", "x'3939'", 0); // x'3939' = 99
  ExecuteAndValidateResultset(db, "[nestedField.field13]", ">", "x'35'", 4); // x'35' = 5 

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

  ExecuteAndValidateResultset(db, "field11", ">=", "'0'", 10);
  ExecuteAndValidateResultset(db, "field11", ">=", "'99'", 0);
  ExecuteAndValidateResultset(db, "field11", ">=", "'5'", 5);

  ExecuteAndValidateResultset(db, "field12", ">=", "x'30'", 10); // x'30' = 0
  ExecuteAndValidateResultset(db, "field12", ">=", "x'3939'", 0); // x'3939' = 99
  ExecuteAndValidateResultset(db, "field12", ">=", "x'35'", 5); // x'35' = 5 

  ExecuteAndValidateResultset(db, "field13", ">=", "x'30'", 10); // x'30' = 0
  ExecuteAndValidateResultset(db, "field13", ">=", "x'3939'", 0); // x'3939' = 99
  ExecuteAndValidateResultset(db, "field13", ">=", "x'35'", 5); // x'35' = 5 

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

  ExecuteAndValidateResultset(db, "[nestedField.field11]", ">=", "'0'", 10);
  ExecuteAndValidateResultset(db, "[nestedField.field11]", ">=", "'99'", 0);
  ExecuteAndValidateResultset(db, "[nestedField.field11]", ">=", "'5'", 5);

  ExecuteAndValidateResultset(db, "[nestedField.field12]", ">=", "x'30'", 10); // x'30' = 0
  ExecuteAndValidateResultset(db, "[nestedField.field12]", ">=", "x'3939'", 0); // x'3939' = 99
  ExecuteAndValidateResultset(db, "[nestedField.field12]", ">=", "x'35'", 5); // x'35' = 5 

  ExecuteAndValidateResultset(db, "[nestedField.field13]", ">=", "x'30'", 10); // x'30' = 0
  ExecuteAndValidateResultset(db, "[nestedField.field13]", ">=", "x'3939'", 0); // x'3939' = 99
  ExecuteAndValidateResultset(db, "[nestedField.field13]", ">=", "x'35'", 5); // x'35' = 5 
}

TEST(Database, ExecuteSelect_GT_GTE) {
  Database db(g_TestRootDirectory, "ExecuteSelect_GT_GTE",
              TestUtils::GetDefaultDBOptions());
  vector<IndexInfo> indexes;  
  Execute_ExecuteSelect_GT_GTE_Test(db, indexes);
}

TEST(Database, ExecuteSelect_GT_GTE_EWAHIndexes) {
  Database db(g_TestRootDirectory, "ExecuteSelect_GT_GTE_EWAHIndexes",
              TestUtils::GetDefaultDBOptions());
  auto indexes = CreateAllTypeIndexes(IndexType::INVERTED_COMPRESSED_BITMAP);
  Execute_ExecuteSelect_GT_GTE_Test(db, indexes);
}

TEST(Database, ExecuteSelect_GT_GTE_VECTORIndexed) {
  Database db(g_TestRootDirectory, "ExecuteSelect_GT_GTE_VECTORIndexed",
              TestUtils::GetDefaultDBOptions());
  auto indexes = CreateAllTypeIndexes(IndexType::VECTOR);
  Execute_ExecuteSelect_GT_GTE_Test(db, indexes);
}

TEST(Database, ExecuteSelect_VECTORIndexed_DoubleExpression) {
  Database db(g_TestRootDirectory,
              "ExecuteSelect_VECTORIndexed_DoubleExpression",
              TestUtils::GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo>
      indexes{IndexInfo("IndexName1", IndexType::VECTOR, "rating", true)};
  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    std::string binData = "some_data_" + std::to_string(i);
    documents.push_back(TestUtils::GetTweetObject(i,
                                        i,
                                        &name,
                                        &text,
                                        (double) i / 100.0,
                                        &binData));
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
  Database db(g_TestRootDirectory,
              "ExecuteSelect_DoubleExpression",
              TestUtils::GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes;
  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    std::string binData = "some_data_" + std::to_string(i);
    documents.push_back(TestUtils::GetTweetObject(i,
                                        i,
                                        &name,
                                        &text,
                                        (double) i / 100.0,
                                        &binData));
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
  Database db(g_TestRootDirectory,
              "ExecuteSelect_EWAHIndexed_String_GTE",
              TestUtils::GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes{IndexInfo("IndexName1",
                                           IndexType::INVERTED_COMPRESSED_BITMAP,
                                           "user.name",
                                           true)};
  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    std::string binData = "some_data_" + std::to_string(i);
    documents.push_back(TestUtils::GetTweetObject(i,
                                        i,
                                        &name,
                                        &text,
                                        (double) i / 100.0,
                                        &binData));
  }
  db.MultiInsert("tweet", documents);

  auto rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                                 "FROM tweet WHERE [user.name] >= 'zarian_3';");
  int counter = 3;
  while (rs.Next()) {
    std::string expectedName = "zarian_" + std::to_string(counter);
    ASSERT_STREQ(expectedName.c_str(),
                 rs.GetString(rs.GetColumnIndex("user.name")).str());
    counter++;
  }
  ASSERT_EQ(counter, 10);
}

void ValidateTweetResultSet(Database& db,
                            int lowerCount,
                            int upperCount,
                            const std::string& lowerColName,
                            const std::string& lowerOp,
                            const std::string& lowerColVal,
                            const std::string& upperColName,
                            const std::string& upperOp,
                            const std::string& upperColVal) {
  std::string
      sql = "SELECT id, text, [user.id], [user.name], rating FROM tweet WHERE ";
  sql.append(lowerColName).append(" ").append(lowerOp).append(" ").
      append(lowerColVal).append(" AND ").append(upperColName).append(" ").
      append(upperOp).append(" ").append(upperColVal).append(";");
  auto rs = db.ExecuteSelect(sql);
  while (rs.Next()) {
    std::string expectedName = "zarian_" + std::to_string(lowerCount);
    ASSERT_STREQ(expectedName.c_str(),
                 rs.GetString(rs.GetColumnIndex("user.name")).str());
    ASSERT_EQ(lowerCount, rs.GetInteger(rs.GetColumnIndex("id")));
    ASSERT_DOUBLE_EQ(double(lowerCount),
                     rs.GetDouble(rs.GetColumnIndex("rating")));
    ASSERT_EQ(lowerCount, rs.GetInteger(rs.GetColumnIndex("user.id")));
    lowerCount++;
  }
  ASSERT_EQ(upperCount, lowerCount);
}

TEST(Database, ExecuteSelect_EWAHIndexed_Range) {
  Database db(g_TestRootDirectory,
              "ExecuteSelect_EWAHIndexed_Range",
              TestUtils::GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes{ IndexInfo("IndexName1", IndexType::INVERTED_COMPRESSED_BITMAP, "id", true),
    IndexInfo("IndexName2", IndexType::INVERTED_COMPRESSED_BITMAP, "rating", true),
    IndexInfo("IndexName3", IndexType::INVERTED_COMPRESSED_BITMAP, "user.id", true),
    IndexInfo("IndexName4", IndexType::INVERTED_COMPRESSED_BITMAP, "user.name", true) };
  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    std::string binData = "some_data_" + std::to_string(i);
    documents.push_back(TestUtils::GetTweetObject(i,
                                        i,
                                        &name,
                                        &text,
                                        static_cast<double>(i),
                                        &binData));
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
  Database db(g_TestRootDirectory,
              "ExecuteSelect_VECTORIndexed_Range",
              TestUtils::GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  // Todo: Enable tests for string Vector indexer once we have implemented that.
  std::vector<IndexInfo>
      indexes{IndexInfo("IndexName1", IndexType::VECTOR, "id", true),
              IndexInfo("IndexName2", IndexType::VECTOR, "rating", true),
              IndexInfo("IndexName3", IndexType::VECTOR, "user.id", true),
              IndexInfo("IndexName4", IndexType::VECTOR, "user.name", true)};
  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    std::string binData = "some_data_" + std::to_string(i);
    documents.push_back(TestUtils::GetTweetObject(i,
                                        i,
                                        &name,
                                        &text,
                                        static_cast<double>(i),
                                        &binData));
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
    Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());
    std::vector<IndexInfo> indexes;
    db.CreateCollection(collectionName,
                        SchemaType::FLAT_BUFFERS,
                        schema,
                        indexes);

    std::vector<Buffer> documents;
    std::string text = "hello";
    std::string name = "zarian";
    std::string binData = "some_data";
    for (size_t i = 0; i < idCnt; i++) {
      documents.push_back(
          TestUtils::GetTweetObject(i, i, &name, &text, (double)i, &binData));
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
  std::vector<int> idCounts = {0, 1, 50, 100, 101, 200, 201};
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);

  for (auto idCnt : idCounts) {
    string dbName = "ExecuteSelect_Aggregation_Indexed_" + to_string(idCnt);
    Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());
    std::vector<IndexInfo>
        indexes{IndexInfo("IndexName1", IndexType::VECTOR, "id", true),
                IndexInfo("IndexName2", IndexType::VECTOR, "rating", true),
                IndexInfo("IndexName3", IndexType::VECTOR, "user.id", true),
                IndexInfo("IndexName4", IndexType::VECTOR, "user.name", true)};
    db.CreateCollection(collectionName,
                        SchemaType::FLAT_BUFFERS,
                        schema,
                        indexes);

    std::vector<Buffer> documents;
    std::string text = "hello";
    std::string name = "zarian";
    std::string binData = "some_data";
    std::int64_t expectedSum = 0;
    for (size_t i = 0; i < idCnt; i++) {
      documents.push_back(
          TestUtils::GetTweetObject(i, i, &name, &text, (double)i, &binData));
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
      ASSERT_DOUBLE_EQ((double) expectedSum,
                       rs.GetDouble(rs.GetColumnIndex("sum_rating")));
      ASSERT_EQ(expectedSum, rs.GetInteger(rs.GetColumnIndex("sum_user_id")));
      if (idCnt > 0) {
        ASSERT_EQ(expectedSum / idCnt,
                  rs.GetInteger(rs.GetColumnIndex("avg_id")));
        ASSERT_DOUBLE_EQ((double) expectedSum / (double) idCnt,
                         rs.GetDouble(rs.GetColumnIndex("avg_rating")));
        ASSERT_EQ(expectedSum / idCnt,
                  rs.GetInteger(rs.GetColumnIndex("avg_user_id")));
      }
      rowCnt++;
    }
    ASSERT_EQ(1, rowCnt);
  }
}

TEST(Database, ExecuteSelect_Aggregation) {
  // This test checks the boundary conditions for IDSeq
  std::vector<int> idCounts = {0, 1, 50, 100, 101, 200, 201};
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);

  for (auto idCnt : idCounts) {
    string dbName = "ExecuteSelect_Aggregation_" + to_string(idCnt);
    Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());
    std::vector<IndexInfo> indexes;
    db.CreateCollection(collectionName,
                        SchemaType::FLAT_BUFFERS,
                        schema,
                        indexes);

    std::vector<Buffer> documents;
    std::string text = "hello";
    std::string name = "zarian";
    std::string binData = "some_data";
    std::int64_t expectedSum = 0;
    for (size_t i = 0; i < idCnt; i++) {
      documents.push_back(
          TestUtils::GetTweetObject(i, i, &name, &text, (double)i, &binData));
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
      ASSERT_DOUBLE_EQ((double) expectedSum,
                       rs.GetDouble(rs.GetColumnIndex("sum_rating")));
      ASSERT_EQ(expectedSum, rs.GetInteger(rs.GetColumnIndex("sum_user_id")));
      if (idCnt > 0) {
        ASSERT_EQ(expectedSum / idCnt,
                  rs.GetInteger(rs.GetColumnIndex("avg_id")));
        ASSERT_DOUBLE_EQ((double) expectedSum / (double) idCnt,
                         rs.GetDouble(rs.GetColumnIndex("avg_rating")));
        ASSERT_EQ(expectedSum / idCnt,
                  rs.GetInteger(rs.GetColumnIndex("avg_user_id")));
      }
      rowCnt++;
    }
    ASSERT_EQ(1, rowCnt);
  }
}

TEST(Database, ExecuteSelect_NullStrFields) {
  Database db(g_TestRootDirectory,
              "ExecuteSelect_NullStrFields",
              TestUtils::GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes;
  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    std::string binData = "some_data_" + std::to_string(i);
    if (i % 2 == 0) {
      documents.push_back(TestUtils::GetTweetObject(i,
                                          i,
                                          &name,
                                          &text,
                                          (double) i / 100.0,
                                          &binData));
    } else {
      documents.push_back(TestUtils::GetTweetObject(i,
                                          i,
                                          nullptr,
                                          nullptr,
                                          (double) i / 100.0,
                                          nullptr));
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
      ASSERT_STREQ(expectedName.c_str(),
                   rs.GetString(rs.GetColumnIndex("user.name")).str());
      ASSERT_STREQ(expectedText.c_str(),
                   rs.GetString(rs.GetColumnIndex("text")).str());
    } else {
      ASSERT_TRUE(rs.IsNull(rs.GetColumnIndex("user.name")));
      ASSERT_TRUE(rs.IsNull(rs.GetColumnIndex("text")));
      ASSERT_STREQ("", rs.GetString(rs.GetColumnIndex("user.name")).str());
      ASSERT_STREQ("", rs.GetString(rs.GetColumnIndex("text")).str());
    }
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 10);

  rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                            "FROM tweet where [user.name] > 'a';");
  rowCnt = 0;
  while (rs.Next()) {
    ASSERT_FALSE(rs.IsNull(rs.GetColumnIndex("user.name")));
    ASSERT_FALSE(rs.IsNull(rs.GetColumnIndex("text")));
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 5);

  rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                            "FROM tweet where [user.name] IS NOT NULL;");
  rowCnt = 0;
  while (rs.Next()) {
    ASSERT_FALSE(rs.IsNull(rs.GetColumnIndex("user.name")));
    ASSERT_FALSE(rs.IsNull(rs.GetColumnIndex("text")));
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 5);

  rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                            "FROM tweet where [user.name] < 'a';");
  rowCnt = 0;
  while (rs.Next()) {
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 0);

  rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                            "FROM tweet where [user.name] IS NULL;");
  rowCnt = 0;
  while (rs.Next()) {
    ASSERT_TRUE(rs.IsNull(rs.GetColumnIndex("user.name")));
    ASSERT_TRUE(rs.IsNull(rs.GetColumnIndex("text")));
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 5);
}

TEST(Database, ExecuteSelect_NullStrFields_VectorIndexed) {
  Database db(g_TestRootDirectory,
              "ExecuteSelect_NullStrFields_Indexed",
              TestUtils::GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo>
      indexes{IndexInfo("IndexName1", IndexType::VECTOR, "text", true),
              IndexInfo("IndexName4", IndexType::VECTOR, "user.name", true)};

  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    std::string binData = "some_data_" + std::to_string(i);
    if (i % 2 == 0) {
      documents.push_back(TestUtils::GetTweetObject(i,
                                          i,
                                          &name,
                                          &text,
                                          (double) i / 100.0,
                                          &binData));
    } else {
      documents.push_back(TestUtils::GetTweetObject(i,
                                          i,
                                          nullptr,
                                          nullptr,
                                          (double) i / 100.0,
                                          &binData));
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
      ASSERT_STREQ(expectedName.c_str(),
                   rs.GetString(rs.GetColumnIndex("user.name")).str());
      ASSERT_STREQ(expectedText.c_str(),
                   rs.GetString(rs.GetColumnIndex("text")).str());
    } else {
      ASSERT_TRUE(rs.IsNull(rs.GetColumnIndex("user.name")));
      ASSERT_TRUE(rs.IsNull(rs.GetColumnIndex("text")));
      ASSERT_STREQ("", rs.GetString(rs.GetColumnIndex("user.name")).str());
      ASSERT_STREQ("", rs.GetString(rs.GetColumnIndex("text")).str());
    }
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 10);

  rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                            "FROM tweet where [user.name] > 'a';");
  rowCnt = 0;
  while (rs.Next()) {
    ASSERT_FALSE(rs.IsNull(rs.GetColumnIndex("user.name")));
    ASSERT_FALSE(rs.IsNull(rs.GetColumnIndex("text")));
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 5);

  rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                            "FROM tweet where [user.name] IS NOT NULL;");
  rowCnt = 0;
  while (rs.Next()) {
    ASSERT_FALSE(rs.IsNull(rs.GetColumnIndex("user.name")));
    ASSERT_FALSE(rs.IsNull(rs.GetColumnIndex("text")));
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 5);

  rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                            "FROM tweet where [user.name] < 'a';");
  rowCnt = 0;
  while (rs.Next()) {
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 0);

  rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                            "FROM tweet where [user.name] IS NULL;");
  rowCnt = 0;
  while (rs.Next()) {
    ASSERT_TRUE(rs.IsNull(rs.GetColumnIndex("user.name")));
    ASSERT_TRUE(rs.IsNull(rs.GetColumnIndex("text")));
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 5);
}

TEST(Database, ExecuteSelect_NullStrFields_EWAHIndexed) {
  Database db(g_TestRootDirectory,
              "ExecuteSelect_NullStrFields_EWAHIndexed",
              TestUtils::GetDefaultDBOptions());
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes
      {IndexInfo("IndexName1", IndexType::INVERTED_COMPRESSED_BITMAP, "text", true),
       IndexInfo("IndexName4",
                 IndexType::INVERTED_COMPRESSED_BITMAP,
                 "user.name",
                 true)};

  db.CreateCollection("tweet", SchemaType::FLAT_BUFFERS, schema, indexes);

  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {
    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    std::string binData = "some_data_" + std::to_string(i);
    if (i % 2 == 0) {
      documents.push_back(TestUtils::GetTweetObject(i,
                                          i,
                                          &name,
                                          &text,
                                          (double) i / 100.0,
                                          &binData));
    } else {
      documents.push_back(TestUtils::GetTweetObject(i,
                                          i,
                                          nullptr,
                                          nullptr,
                                          (double) i / 100.0,
                                          &binData));
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
      ASSERT_STREQ(expectedName.c_str(),
                   rs.GetString(rs.GetColumnIndex("user.name")).str());
      ASSERT_STREQ(expectedText.c_str(),
                   rs.GetString(rs.GetColumnIndex("text")).str());
    } else {
      ASSERT_TRUE(rs.IsNull(rs.GetColumnIndex("user.name")));
      ASSERT_TRUE(rs.IsNull(rs.GetColumnIndex("text")));
      ASSERT_STREQ("", rs.GetString(rs.GetColumnIndex("user.name")).str());
      ASSERT_STREQ("", rs.GetString(rs.GetColumnIndex("text")).str());
    }
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 10);

  rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                            "FROM tweet where [user.name] > 'a';");
  rowCnt = 0;
  while (rs.Next()) {
    ASSERT_FALSE(rs.IsNull(rs.GetColumnIndex("user.name")));
    ASSERT_FALSE(rs.IsNull(rs.GetColumnIndex("text")));
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 5);

  rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                            "FROM tweet where [user.name] IS NOT NULL;");
  rowCnt = 0;
  while (rs.Next()) {
    ASSERT_FALSE(rs.IsNull(rs.GetColumnIndex("user.name")));
    ASSERT_FALSE(rs.IsNull(rs.GetColumnIndex("text")));
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 5);

  rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                            "FROM tweet where [user.name] < 'a';");
  rowCnt = 0;
  while (rs.Next()) {
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 0);

  rs = db.ExecuteSelect("SELECT id, text, [user.id], [user.name], rating "
                            "FROM tweet where [user.name] IS NULL;");
  rowCnt = 0;
  while (rs.Next()) {
    ASSERT_TRUE(rs.IsNull(rs.GetColumnIndex("user.name")));
    ASSERT_TRUE(rs.IsNull(rs.GetColumnIndex("text")));
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 5);
}

TEST(Database, NullNestedField) {
  string dbName = "NullNestedField";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());

  string filePath = GetSchemaFilePath("all_field_type.bfbs");
  string schema = File::Read(filePath);
  // auto indexes = CreateAllTypeIndexes(indexType);
  std::vector<IndexInfo> indexes;

  db.CreateCollection(collectionName,
                      SchemaType::FLAT_BUFFERS,
                      schema,
                      indexes);

  Buffer
    documentData = TestUtils::GetAllFieldTypeObjectBuffer(1, 2, true, 4, 5,
                                                          6, 7, 8.0f, 9,
                                                          10.0, "test",
                                                          "test1", "test2", true);
  db.Insert(collectionName, documentData);

  auto rs = db.ExecuteSelect("select field1, \"nestedField.field1\", "
                             "\"nestedField.field2\" FROM CollectionName;");
  while (rs.Next()) {
    ASSERT_FALSE(rs.IsNull(rs.GetColumnIndex("field1")));
    ASSERT_TRUE(rs.IsNull(rs.GetColumnIndex("nestedField.field1")));
    ASSERT_TRUE(rs.IsNull(rs.GetColumnIndex("nestedField.field2")));
  }
}

TEST(Database, ExecuteSelect_ResultsetDoubleConsumption) {
  string dbName = "ExecuteSelect_ResultsetConsumption";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes;
  db.CreateCollection(collectionName,
                      SchemaType::FLAT_BUFFERS,
                      schema,
                      indexes);

  std::string name = "Zarian";
  std::string text = "Say hello to my little friend!";
  std::string binData = "some_data";
  Buffer documentData = TestUtils::GetTweetObject(1, 1, &name, &text, 2.0, &binData);
  db.Insert(collectionName, documentData);

  int rows = 0;
  ResultSet rs = db.ExecuteSelect(
      "SELECT id, text, [user.id], [user.name], rating FROM tweet;");
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), 1);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("text")).str(),
                 "Say hello to my little friend!");
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("user.id")), 1);
    ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("user.name")).str(), "Zarian");
    ASSERT_DOUBLE_EQ(rs.GetDouble(rs.GetColumnIndex("rating")), 2.0);
    ++rows;
  }
  ASSERT_EQ(rows, 1);

  rows = 0;
  while (rs.Next()) {
    ++rows;
  }
  ASSERT_EQ(rows, 0);

  ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("id")), 0);
  ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("text")).str(), "");
  ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("user.id")), 0);
  ASSERT_STREQ(rs.GetString(rs.GetColumnIndex("user.name")).str(), "");
  ASSERT_DOUBLE_EQ(rs.GetDouble(rs.GetColumnIndex("rating")), 0.0);

  rs.Close();
}

TEST(Database, Insert_Invalid) {
  string dbName = "Insert_Invalid";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());

  string filePath = GetSchemaFilePath("all_field_type.bfbs");
  string schema = File::Read(filePath);  
  vector<IndexInfo> indexes;

  db.CreateCollection(collectionName,
                      SchemaType::FLAT_BUFFERS,
                      schema,
                      indexes);

  string str = "some invalid data buffer";
  Buffer documentData(str.data(), str.length(), str.length());
  
  ASSERT_THROW(db.Insert(collectionName, documentData),
               JonoonDBException);
}
