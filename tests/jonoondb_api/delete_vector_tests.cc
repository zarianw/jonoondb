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
  ASSERT_THROW(DeleteVector delVector(dbPath, dbName, collectionName, false), MissingDatabaseFileException);
}

TEST(DeleteVector, EmptyCheckOnDeleteVector) {
  DeleteVector delVector(g_TestRootDirectory, GetUniqueDBName(), "Collection", true);
  ASSERT_TRUE(delVector.GetDeleteVectorBitmap().Empty());

  // when documents are inserted the delete vector should remain empty
  delVector.OnDocumentsInserted(1);
  ASSERT_FALSE(delVector.GetDeleteVectorBitmap().Empty());

  delVector.OnDocumentDeleted(0);
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
  DeleteVector delVector(g_TestRootDirectory, GetUniqueDBName(), "Collection", true);
  vector<int> expectedValues;
  AssertEqual(expectedValues, delVector.GetDeleteVectorBitmap());
}

TEST(DeleteVector, CheckBitmapOnNonEmptyDeleteVector) {
  DeleteVector delVector(g_TestRootDirectory, GetUniqueDBName(), "Collection", true);
  delVector.OnDocumentsInserted(5);
  delVector.OnDocumentDeleted(2);
  vector<int> expectedValues{0, 1, 3, 4};
  AssertEqual(expectedValues, delVector.GetDeleteVectorBitmap());
}

TEST(DeleteVector, CheckBitmapWhenDocumentsAreInserted) {
  DeleteVector delVector(g_TestRootDirectory, GetUniqueDBName(), "Collection", true);

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
    DeleteVector delVector(g_TestRootDirectory, dbName, "Collection", true);
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
    DeleteVector delVector(g_TestRootDirectory, dbName, "Collection", false);
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
    DeleteVector delVector(g_TestRootDirectory, dbName, "Collection", true);
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
    DeleteVector delVector(g_TestRootDirectory, dbName, "Collection", false);
    // delVector does not persist nextDocumentId in order to optimize inserts
    // in db, this has to be provided by the caller after construction to
    // complete the delVector initialization
    delVector.OnDocumentsInserted(5);
    // then: the last state of delete vector should be maintained
    AssertEqual(expectedValues, delVector.GetDeleteVectorBitmap());
  }
}