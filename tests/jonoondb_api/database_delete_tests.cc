#include <boost/algorithm/string.hpp>
#include <cstdio>
#include <fstream>
#include <string>
#include "buffer_impl.h"
#include "database.h"
#include "enums.h"
#include "file.h"
#include "flatbuffers/flatbuffers.h"
#include "gtest/gtest.h"
#include "jonoondb_api_vx_test_utils.h"
#include "test_utils.h"
#include "tweet_generated.h"

using namespace std;
using namespace flatbuffers;
using namespace jonoondb_api;
using namespace jonoondb_test;
using namespace jonoondb_api_vx_test;

const string COLLECTION_WITH_NO_DOCS = "collection_with_no_docs";
const string COLLECTION_WITH_DOCS = "collection_with_docs";
const string COLLECTION_WITH_EWAH_INDEXED_DOCS =
    "collection_with_ewah_indexed_docs";
const string COLLECTION_WITH_VEC_INDEXED_DOCS =
    "collection_with_vec_indexed_docs";

struct DatabaseDeleteTestSuiteParams {
  DatabaseDeleteTestSuiteParams(const string& collectionName, bool reopenDB)
      : collectionName(collectionName), reopenDB(reopenDB) {}
  string collectionName;
  bool reopenDB;
};

class DatabaseDeleteTestSuite
    : public ::testing::Test,
      public ::testing::WithParamInterface<DatabaseDeleteTestSuiteParams> {
 protected:
  std::unique_ptr<Database> m_db;
  int m_docCnt = 10;
  string m_dbName;

  void SetUp() override {
    m_dbName = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    SanitizeDBName(m_dbName);
    m_db.reset(new Database(g_TestRootDirectory, m_dbName,
                            TestUtils::GetDefaultDBOptions()));

    string filePath = GetSchemaFilePath("all_field_type.bfbs");
    string schema = File::Read(filePath);
    // Create Collections
    m_db->CreateCollection(COLLECTION_WITH_NO_DOCS, SchemaType::FLAT_BUFFERS,
                           schema, vector<IndexInfo>());
    m_db->CreateCollection(COLLECTION_WITH_DOCS, SchemaType::FLAT_BUFFERS,
                           schema, vector<IndexInfo>());
    m_db->CreateCollection(
        COLLECTION_WITH_EWAH_INDEXED_DOCS, SchemaType::FLAT_BUFFERS, schema,
        TestUtils::CreateAllTypeIndexes(IndexType::INVERTED_COMPRESSED_BITMAP));
    m_db->CreateCollection(COLLECTION_WITH_VEC_INDEXED_DOCS,
                           SchemaType::FLAT_BUFFERS, schema,
                           TestUtils::CreateAllTypeIndexes(IndexType::VECTOR));

    // Insert some documents
    InsertDocuments(COLLECTION_WITH_DOCS);
    InsertDocuments(COLLECTION_WITH_EWAH_INDEXED_DOCS);
    InsertDocuments(COLLECTION_WITH_VEC_INDEXED_DOCS);
  }

  Buffer GetAllFieldObject(size_t i) {
    std::string str = std::to_string(i);
    return TestUtils::GetAllFieldTypeObjectBuffer(
        static_cast<int8_t>(i), static_cast<uint8_t>(i), true,
        static_cast<int16_t>(i), static_cast<uint16_t>(i),
        static_cast<int32_t>(i), static_cast<uint32_t>(i), (float) i,
        static_cast<int64_t>(i), (double) i, str, str, str);
  }

  void CloseAndReopenDB() {
    m_db.reset(nullptr);

    m_db.reset(new Database(g_TestRootDirectory, m_dbName,
                            TestUtils::GetDefaultDBOptions()));
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

INSTANTIATE_TEST_CASE_P(
    CollectionsWithDocsGroup, DatabaseDeleteTestSuite,
    ::testing::Values(
        DatabaseDeleteTestSuiteParams(COLLECTION_WITH_DOCS, false),
        DatabaseDeleteTestSuiteParams(COLLECTION_WITH_DOCS, true),
        DatabaseDeleteTestSuiteParams(COLLECTION_WITH_EWAH_INDEXED_DOCS, false),
        DatabaseDeleteTestSuiteParams(COLLECTION_WITH_EWAH_INDEXED_DOCS, true),
        DatabaseDeleteTestSuiteParams(COLLECTION_WITH_VEC_INDEXED_DOCS, false),
        DatabaseDeleteTestSuiteParams(COLLECTION_WITH_VEC_INDEXED_DOCS, true)));

TEST_P(DatabaseDeleteTestSuite, DeleteDocument) {
  // given: document is deleted
  auto deletedCnt =
      m_db->Delete(string("DELETE FROM ") + GetParam().collectionName +
          string(" where field1 = 5"));
  ASSERT_EQ(deletedCnt, 1);

  if (GetParam().reopenDB) {
    // when: the db is closed and reopened the state should be maintained
    CloseAndReopenDB();
  }

  // when: trying to get the deleted document
  ResultSet rs =
      m_db->ExecuteSelect(string("SELECT * from ") + GetParam().collectionName +
          string(" where field1 = 5"));
  // then: we should not get anything back
  int cnt = 0;
  while (rs.Next()) {
    cnt++;
  }
  ASSERT_EQ(cnt, 0);

  // when: trying to get all the documents
  rs = m_db->ExecuteSelect("SELECT * from " + GetParam().collectionName);
  // then: we should not get anything that was deleted
  cnt = 0;
  while (rs.Next()) {
    cnt++;
  }
  ASSERT_EQ(cnt, m_docCnt - 1);

  // when: trying to get a non deleted document
  rs =
      m_db->ExecuteSelect(string("SELECT * from ") + GetParam().collectionName +
          string(" where field1 = 3"));
  // then: we should get that document back
  cnt = 0;
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("field1")), 3);
    cnt++;
  }
  ASSERT_EQ(cnt, 1);
}

TEST_P(DatabaseDeleteTestSuite, DeleteDocumentWithNestedSelect) {
  // given: document is deleted
  auto deletedCnt =
      m_db->Delete(string("DELETE FROM ") + GetParam().collectionName +
          string(" WHERE field1 = (SELECT field1 FROM " +
              GetParam().collectionName + " WHERE field1 = 5)"));
  ASSERT_EQ(deletedCnt, 1);

  if (GetParam().reopenDB) {
    // when: the db is closed and reopened the state should be maintained
    CloseAndReopenDB();
  }

  // when: trying to get the deleted document
  ResultSet rs =
      m_db->ExecuteSelect(string("SELECT * from ") + GetParam().collectionName +
          string(" where field1 = 5"));
  // then: we should not get anything back
  int cnt = 0;
  while (rs.Next()) {
    cnt++;
  }
  ASSERT_EQ(cnt, 0);

  // when: trying to get all the documents
  rs = m_db->ExecuteSelect("SELECT * from " + GetParam().collectionName);
  // then: we should not get anything that was deleted
  cnt = 0;
  while (rs.Next()) {
    cnt++;
  }
  ASSERT_EQ(cnt, m_docCnt - 1);

  // when: trying to get a non deleted document
  rs =
      m_db->ExecuteSelect(string("SELECT * from ") + GetParam().collectionName +
          string(" where field1 = 3"));
  // then: we should get that document back
  cnt = 0;
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("field1")), 3);
    cnt++;
  }
  ASSERT_EQ(cnt, 1);
}

TEST_P(DatabaseDeleteTestSuite, DeleteAllDocuments) {
  // given: all documents are deleted
  auto deletedCnt =
      m_db->Delete(string("DELETE FROM ") + GetParam().collectionName);
  ASSERT_EQ(deletedCnt, m_docCnt);

  if (GetParam().reopenDB) {
    // when: the db is closed and reopened the state should be maintained
    CloseAndReopenDB();
  }

  // when: trying to get the deleted document
  ResultSet rs =
      m_db->ExecuteSelect(string("SELECT * from ") + GetParam().collectionName +
          string(" where field1 = 5"));
  // then: we should not get anything back
  int cnt = 0;
  while (rs.Next()) {
    cnt++;
  }
  ASSERT_EQ(cnt, 0);

  // when: trying to get all the documents
  rs =
      m_db->ExecuteSelect(string("SELECT * from ") + GetParam().collectionName);
  // then: we should not get anything
  cnt = 0;
  while (rs.Next()) {
    cnt++;
  }
  ASSERT_EQ(cnt, 0);
}

TEST_P(DatabaseDeleteTestSuite, DeleteAndReinsertDocument) {
  // given: document is deleted and reinserted
  auto deletedCnt =
      m_db->Delete(string("DELETE FROM ") + GetParam().collectionName +
          string(" where field1 = 5"));
  ASSERT_EQ(deletedCnt, 1);
  Buffer obj = GetAllFieldObject(5);
  m_db->Insert(GetParam().collectionName, obj);

  if (GetParam().reopenDB) {
    // when: the db is closed and reopened the state should be maintained
    CloseAndReopenDB();
  }

  // when: trying to get the deleted/reinserted document
  ResultSet rs =
      m_db->ExecuteSelect(string("SELECT * from ") + GetParam().collectionName +
          string(" where field1 = 5"));
  // then: we should get it back
  int cnt = 0;
  while (rs.Next()) {
    ASSERT_EQ(rs.GetInteger(rs.GetColumnIndex("field1")), 5);
    cnt++;
  }
  ASSERT_EQ(cnt, 1);

  // when: trying to get all the documents
  rs = m_db->ExecuteSelect("SELECT * from " + GetParam().collectionName);
  // then: we should get back the same number of documents as originally
  // inserted
  cnt = 0;
  while (rs.Next()) {
    cnt++;
  }
  ASSERT_EQ(cnt, m_docCnt);

  // when: a new document is inserted and retrieved
  obj = GetAllFieldObject(11);
  m_db->Insert(GetParam().collectionName, obj);
  // then: we should get the document
  rs = m_db->ExecuteSelect("SELECT * from " + GetParam().collectionName +
      string(" where field1 = 11"));
  cnt = 0;
  while (rs.Next()) {
    cnt++;
  }
  ASSERT_EQ(cnt, 1);

  // when: trying to get all the documents
  rs = m_db->ExecuteSelect("SELECT * from " + GetParam().collectionName);
  // then: we should get back the same number of documents as originally
  // inserted + 1
  cnt = 0;
  while (rs.Next()) {
    cnt++;
  }
  ASSERT_EQ(cnt, m_docCnt + 1);
}

TEST_F(DatabaseDeleteTestSuite, DeleteDocumentInEmptyCollection) {
  // given: document is deleted in empty collection
  auto deletedCnt = m_db->Delete("DELETE FROM " + COLLECTION_WITH_NO_DOCS +
      " where field1 = 5");
  ASSERT_EQ(deletedCnt, 0);

  // when: trying to get the deleted document
  ResultSet rs =
      m_db->ExecuteSelect(string("SELECT * FROM ") + COLLECTION_WITH_NO_DOCS +
          string(" where field1 = 5"));
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

TEST_F(DatabaseDeleteTestSuite,
       InternalJonoondbDataShouldNotBeDelatableThroughDeleteAPI) {
  // when: trying to delete internal jonoondb collection
  ASSERT_ANY_THROW(m_db->Delete("DELETE FROM Collection"));
  ASSERT_ANY_THROW(m_db->Delete("DELETE FROM CollectionIndex"));
  ASSERT_ANY_THROW(m_db->Delete("DELETE FROM CollectionDataFile"));
  ASSERT_ANY_THROW(m_db->Delete("DELETE FROM CollectionDeleteVector"));
}

TEST_F(DatabaseDeleteTestSuite, ApiMisuseTestForDeleteApi) {
  // when: trying to perform non delete sql stmts then ApiMisuseException should be thrown
  ASSERT_THROW(m_db->Delete("DROP table " + COLLECTION_WITH_DOCS),
               ApiMisuseException);
  ASSERT_THROW(m_db->Delete(
      "INSERT INTO " + COLLECTION_WITH_DOCS + " VALUES (1, 2, 3)"),
               ApiMisuseException);
  ASSERT_THROW(m_db->Delete("UPDATE " + COLLECTION_WITH_DOCS + " SET col1 = 1"),
               ApiMisuseException);
  ASSERT_THROW(m_db->Delete("CREATE TABLE tab (c1 INT, c2 INT)"),
               ApiMisuseException);
}


