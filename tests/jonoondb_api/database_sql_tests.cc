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

// Test whether we can get vectorized explain plan.
// This test was added after the bug that caused a crash
// because the vectorized OP names were not added in
// the sqlite3OpcodeName.
TEST(Database, ExecuteSelect_Explain) {
  string dbName = "Database_ExecuteSelect_Explain";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, GetDefaultDBOptions());

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes;
  db.CreateCollection(collectionName,
                      SchemaType::FLAT_BUFFERS,
                      schema,
                      indexes);

  auto rs = db.ExecuteSelect("explain SELECT SUM(rating) from tweet;");
  auto rowCnt = 0;
  while (rs.Next()) {
    for (int i = 0; i < rs.GetColumnCount(); i++) {
      rs.GetString(i);
    }
    rowCnt++;
  }
  ASSERT_GT(rowCnt, 0);
}

TEST(Database, ExecuteSelect_GetDocument) {
  string dbName = "ExecuteSelect_GetDocument";
  string collectionName = "tweet";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, GetDefaultDBOptions());

  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  std::vector<IndexInfo> indexes;
  
  db.CreateCollection(collectionName,
                      SchemaType::FLAT_BUFFERS,
                      schema,
                      indexes); 
  
  std::vector<Buffer> documents;
  for (size_t i = 0; i < 10; i++) {

    std::string name = "zarian_" + std::to_string(i);
    std::string text = "hello_" + std::to_string(i);
    std::string binData = "some_data_" + std::to_string(i);
    documents.push_back(
      GetTweetObject2(i, i, &name, &text, (double)i, &binData));
  }

  db.MultiInsert(collectionName, documents);

  // Now see if they were inserted correctly
  auto rs = db.ExecuteSelect("SELECT _document FROM tweet;");
  auto rowCnt = 0;
  while (rs.Next()) {
    auto blob = rs.GetBlob(rs.GetColumnIndex("_document"));
    ASSERT_EQ(blob.GetLength(), documents[rowCnt].GetLength());
    ASSERT_EQ(memcmp(blob.GetData(), documents[rowCnt].GetData(), blob.GetLength()), 0);
    rowCnt++;
  }
  ASSERT_EQ(rowCnt, 10);
}
