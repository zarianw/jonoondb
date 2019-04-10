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

class DatabaseDeleteTestSuite : public ::testing::Test {
 protected:
  std::unique_ptr<Database> db;

  void SetUp() override {
    db.reset(new Database(g_TestRootDirectory,
                          ::testing::UnitTest::GetInstance()->current_test_info()->name(),
                          TestUtils::GetDefaultDBOptions()));

    string filePath = GetSchemaFilePath("all_field_type.bfbs");
    string schema = File::Read(filePath);
    // Create Collections
    vector<IndexInfo> indexes;
    db->CreateCollection("collection_with_docs", SchemaType::FLAT_BUFFERS, schema, indexes);
    // Insert some documents
    InsertDocuments("collection_with_docs", 10);
  }

 private:
  void InsertDocuments(const std::string& collectionName, size_t docCnt) {
    std::vector<Buffer> documents;
    for (size_t i = 0; i < docCnt; i++) {
      std::string str = std::to_string(i);
      documents.push_back(TestUtils::GetAllFieldTypeObjectBuffer(static_cast<int8_t>(i),
                                                                 static_cast<uint8_t>(i),
                                                                 true,
                                                                 static_cast<int16_t>(i),
                                                                 static_cast<uint16_t>(i),
                                                                 static_cast<int32_t>(i),
                                                                 static_cast<uint32_t>(i),
                                                                 (float) i,
                                                                 static_cast<int64_t>(i),
                                                                 (double) i,
                                                                 str, str, str));
    }

    db->MultiInsert(collectionName, documents);
  }
};

TEST_F(DatabaseDeleteTestSuite, DeleteDocument) {
  auto deletedCnt = db->Delete("DELETE FROM collection_with_docs where field1 = 5;");
  ASSERT_EQ(deletedCnt, 1);
}