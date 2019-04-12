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
#include <boost/algorithm/string.hpp>

using namespace std;
using namespace flatbuffers;
using namespace jonoondb_api;
using namespace jonoondb_test;
using namespace jonoondb_api_vx_test;

const string COLLECTION_WITH_NO_DOCS = "collection_with_no_docs";
const string COLLECTION_WITH_DOCS = "collection_with_docs";
const string COLLECTION_WITH_EWAH_INDEXED_DOCS = "collection_with_ewah_indexed_docs";
const string COLLECTION_WITH_VEC_INDEXED_DOCS = "collection_with_vec_indexed_docs";

class DatabaseDeleteTestSuite : public ::testing::Test, public ::testing::WithParamInterface<const char*> {
 protected:
  std::unique_ptr<Database> m_db;
  int m_docCnt = 10;

  void SetUp() override {
    string name(::testing::UnitTest::GetInstance()->current_test_info()->name());
    SanitizeDBName(name);
    m_db.reset(new Database(g_TestRootDirectory,
                            name,
                            TestUtils::GetDefaultDBOptions()));

    string filePath = GetSchemaFilePath("all_field_type.bfbs");
    string schema = File::Read(filePath);
    // Create Collections
    m_db->CreateCollection(COLLECTION_WITH_NO_DOCS, SchemaType::FLAT_BUFFERS, schema, vector<IndexInfo>());
    m_db->CreateCollection(COLLECTION_WITH_DOCS, SchemaType::FLAT_BUFFERS, schema, vector<IndexInfo>());
    m_db->CreateCollection(COLLECTION_WITH_EWAH_INDEXED_DOCS, SchemaType::FLAT_BUFFERS, schema,
                           TestUtils::CreateAllTypeIndexes(IndexType::INVERTED_COMPRESSED_BITMAP));
    m_db->CreateCollection(COLLECTION_WITH_VEC_INDEXED_DOCS, SchemaType::FLAT_BUFFERS, schema,
                           TestUtils::CreateAllTypeIndexes(IndexType::VECTOR));

    // Insert some documents
    InsertDocuments(COLLECTION_WITH_DOCS);
    InsertDocuments(COLLECTION_WITH_EWAH_INDEXED_DOCS);
    InsertDocuments(COLLECTION_WITH_VEC_INDEXED_DOCS);
  }

  Buffer GetAllFieldObject(size_t i) {
    std::string str = std::to_string(i);
    return TestUtils::GetAllFieldTypeObjectBuffer(static_cast<int8_t>(i),
                                                  static_cast<uint8_t>(i),
                                                  true,
                                                  static_cast<int16_t>(i),
                                                  static_cast<uint16_t>(i),
                                                  static_cast<int32_t>(i),
                                                  static_cast<uint32_t>(i),
                                                  (float) i,
                                                  static_cast<int64_t>(i),
                                                  (double) i,
                                                  str, str, str);
  }

 private:
  void SanitizeDBName(string& testName) {
    boost::replace_all(testName, "/", "_");
    boost::replace_all(testName, "\\", "_");
  }
  void InsertDocuments(const std::string& collectionName) {
    std::vector<Buffer> documents;
    for (size_t i = 0; i < m_docCnt; i++) {
      std::string str = std::to_string(i);
      documents.push_back(GetAllFieldObject(i));
    }

    m_db->MultiInsert(collectionName, documents);
  }
};

INSTANTIATE_TEST_CASE_P(CollectionsWithDocsGroup, DatabaseDeleteTestSuite,
                        ::testing::Values(
                            COLLECTION_WITH_DOCS.c_str(),
                            COLLECTION_WITH_EWAH_INDEXED_DOCS.c_str(),
                            COLLECTION_WITH_VEC_INDEXED_DOCS.c_str()));

TEST_P(DatabaseDeleteTestSuite, DeleteDocument) {
  cout << GetParam() << endl;
  // given: document is deleted
  auto deletedCnt = m_db->Delete(string("DELETE FROM ") + string(GetParam()) + string(" where field1 = 5"));
  ASSERT_EQ(deletedCnt, 1);

  // when: trying to get the deleted document
  ResultSet rs = m_db->ExecuteSelect(string("SELECT * from ") + string(GetParam()) + string(" where field1 = 5"));
  // then: we should not get anything back
  int cnt = 0;
  while (rs.Next()) {
    cnt++;
  }
  ASSERT_EQ(cnt, 0);

  // when: trying to get all the documents
  rs = m_db->ExecuteSelect("SELECT * from " + string(GetParam()));
  // then: we should not get anything that was deleted
  cnt = 0;
  while (rs.Next()) {
    cnt++;
  }
  ASSERT_EQ(cnt, m_docCnt - 1);

  // when: trying to get a non deleted document
  rs = m_db->ExecuteSelect(string("SELECT * from ") + string(GetParam() + string(" where field1 = 3")));
  // then: we should not get that document back
  cnt = 0;
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("field1")), 3);
    cnt++;
  }
  ASSERT_EQ(cnt, 1);
}

TEST_P(DatabaseDeleteTestSuite, DeleteAllDocuments) {
  // given: all documents are deleted
  auto deletedCnt = m_db->Delete(string("DELETE FROM ") + string(GetParam()));
  ASSERT_EQ(deletedCnt, m_docCnt);

  // when: trying to get the deleted document
  ResultSet rs = m_db->ExecuteSelect(string("SELECT * from ") + string(GetParam()) + string(" where field1 = 5"));
  // then: we should not get anything back
  int cnt = 0;
  while (rs.Next()) {
    cnt++;
  }
  ASSERT_EQ(cnt, 0);

  // when: trying to get all the documents
  rs = m_db->ExecuteSelect(string("SELECT * from ") + string(GetParam()));
  // then: we should not get anything
  cnt = 0;
  while (rs.Next()) {
    cnt++;
  }
  ASSERT_EQ(cnt, 0);
}

TEST_P(DatabaseDeleteTestSuite, DeleteAndReinsertDocument) {
  cout << GetParam() << endl;
  // given: document is deleted and reinserted
  auto deletedCnt = m_db->Delete(string("DELETE FROM ") + string(GetParam()) + string(" where field1 = 5"));
  ASSERT_EQ(deletedCnt, 1);
  Buffer obj = GetAllFieldObject(5);
  m_db->Insert(string(GetParam()), obj);

  // when: trying to get the deleted/reinserted document
  ResultSet rs = m_db->ExecuteSelect(string("SELECT * from ") + string(GetParam()) + string(" where field1 = 5"));
  // then: we should get it back
  int cnt = 0;
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("field1")), 5);
    cnt++;
  }
  ASSERT_EQ(cnt, 1);

  // when: trying to get all the documents
  rs = m_db->ExecuteSelect("SELECT * from " + string(GetParam()));
  // then: we should get back the same number of documents as originally inserted
  cnt = 0;
  while (rs.Next()) {
    cnt++;
  }
  ASSERT_EQ(cnt, m_docCnt);
}

TEST_F(DatabaseDeleteTestSuite, DeleteDocumentInEmptyCollection) {
  // given: document is deleted in empty collection
  auto deletedCnt = m_db->Delete("DELETE FROM " + COLLECTION_WITH_NO_DOCS + " where field1 = 5");
  ASSERT_EQ(deletedCnt, 0);

  // when: trying to get the deleted document
  ResultSet rs = m_db->ExecuteSelect(string("SELECT * FROM ") + COLLECTION_WITH_NO_DOCS + string(" where field1 = 5"));
  // then: we should not get anything back
  int cnt = 0;
  while (rs.Next()) {
    cnt++;
  }
  ASSERT_EQ(cnt, 0);

  // when: trying to get all the documents
  rs = m_db->ExecuteSelect("SELECT * FROM " + COLLECTION_WITH_NO_DOCS);
  // then: we should not get anything back
  cnt = 0;
  while (rs.Next()) {
    cnt++;
  }
  ASSERT_EQ(cnt, 0);
}
