#include "gtest/gtest.h"
#include "test_utils.h"
#include "jonoondb_api/delete_vector.h"
#include "jonoondb_exceptions.h"

using namespace std;
using namespace jonoondb_api;
using namespace jonoondb_test;

TEST(DeleteVector, DatabaseFileIsMissing) {
  string dbName = GetUniqueDBName();
  string dbPath = g_TestRootDirectory;
  string collectionName = "Collection";
  ASSERT_THROW(DeleteVector
                   delVector(dbPath, dbName, collectionName, false, 0),
               MissingDatabaseFileException);
}

TEST(DeleteVector, EmptyCheckOnDeleteVectorOnDocInsert) {
  DeleteVector delVector(g_TestRootDirectory, GetUniqueDBName(),
                         "Collection", true, 0);
  ASSERT_TRUE(delVector.GetDeleteVectorBitmap().Empty());

  // when documents are inserted
  delVector.OnDocumentsInserted(1);
  // then: the delete vector should not be empty
  ASSERT_FALSE(delVector.GetDeleteVectorBitmap().Empty());
}

TEST(DeleteVector, EmptyCheckOnDeleteVectorOnDocDelete) {
  DeleteVector delVector(g_TestRootDirectory, GetUniqueDBName(),
                         "Collection", true, 1);

  // when documents are deleted
  delVector.OnDocumentDeleted(0);
  // then: the delete vector should not be empty
  ASSERT_FALSE(delVector.GetDeleteVectorBitmap().Empty());
}

static void AssertEqual(const vector<int>& expectedValues, const MamaJenniesBitmap& actualValues) {
  size_t index = 0;
  for (uint64_t item : actualValues) {
    ASSERT_LE(index, expectedValues.size());
    ASSERT_EQ(item, expectedValues[index]);
    index++;
  }
  ASSERT_EQ(index, expectedValues.size());
}

TEST(DeleteVector, CheckBitmapOnEmptyDeleteVector) {
  DeleteVector delVector(g_TestRootDirectory, GetUniqueDBName(),
                         "Collection", true, 0);
  vector<int> expectedValues;
  AssertEqual(expectedValues, delVector.GetDeleteVectorBitmap());
}

TEST(DeleteVector, CheckBitmapOnNonEmptyDeleteVector) {
  DeleteVector delVector(g_TestRootDirectory, GetUniqueDBName(),
                         "Collection", true, 0);
  delVector.OnDocumentsInserted(5);
  delVector.OnDocumentDeleted(2);
  vector<int> expectedValues{0, 1, 3, 4};
  AssertEqual(expectedValues, delVector.GetDeleteVectorBitmap());
}

TEST(DeleteVector, CheckBitmapWhenDocumentsAreInserted) {
  DeleteVector delVector(g_TestRootDirectory, GetUniqueDBName(),
                         "Collection", true, 0);

  vector<int> expectedValues;
  for (uint64_t i = 0; i < 5; ++i) {
    delVector.OnDocumentsInserted(i + 1);
    expectedValues.push_back(i);
    AssertEqual(expectedValues, delVector.GetDeleteVectorBitmap());
  }
}

TEST(DeleteVector, FullDelete) {
  string dbName = GetUniqueDBName();
  {
    // given: a delete vector with 5 documents with ids 0 to 4
    DeleteVector delVector(g_TestRootDirectory, dbName, "Collection", true, 0);
    delVector.OnDocumentsInserted(5);

    // when: all documents are deleted
    for (uint64_t i = 0; i < 5; ++i) {
      delVector.OnDocumentDeleted(i);
    }
    // then: the delete vector bitmap should have no docIds marked as present
    vector<int> expectedValues;
    AssertEqual(expectedValues, delVector.GetDeleteVectorBitmap());
  }

  {
    // when: the deleteVector/db is reopened
    DeleteVector delVector(g_TestRootDirectory, dbName, "Collection", false, 5);
    // then: the last state of delete vector should be maintained
    vector<int> expectedValues;
    AssertEqual(expectedValues, delVector.GetDeleteVectorBitmap());
  }
}

TEST(DeleteVector, ZigZagDelete) {
  string dbName = GetUniqueDBName();
  vector<int> expectedValues;

  {
    // given: a delete vector with 5 documents with ids 0 to 4
    DeleteVector delVector(g_TestRootDirectory, dbName, "Collection", true, 0);
    delVector.OnDocumentsInserted(5);
    // when: documentIds are deleted in zigzag order
    for (uint64_t i = 0; i < 5; i++) {
      if (i % 2 == 0) {
        delVector.OnDocumentDeleted(i);
      } else {
        expectedValues.push_back(i);
      }
    }
    // then: the delete vector bitmap should have only the ids that were not deleted
    AssertEqual(expectedValues, delVector.GetDeleteVectorBitmap());
  }

  {
    // when: the deleteVector/db is reopened
    DeleteVector delVector(g_TestRootDirectory, dbName, "Collection", false, 5);
    // then: the last state of delete vector should be maintained
    AssertEqual(expectedValues, delVector.GetDeleteVectorBitmap());
  }
}