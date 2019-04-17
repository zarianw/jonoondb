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

void AssertEqual(const vector<int>& expectedValues, const MamaJenniesBitmap& actualValues) {
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

TEST(DeleteVector, OnDocumentInsertedIncreasingOrder) {
  DeleteVector delVector(g_TestRootDirectory, GetUniqueDBName(), "Collection", true);

  vector<int> expectedValues;
  for (uint64_t i = 0; i < 5; ++i) {
    delVector.OnDocumentsInserted(i + 1);
    expectedValues.push_back(i);
    AssertEqual(expectedValues, delVector.GetDeleteVectorBitmap());
  }
}

TEST(DeleteVector, FullDelete) {
  DeleteVector delVector(g_TestRootDirectory, GetUniqueDBName(), "Collection", true);
  delVector.OnDocumentsInserted(5);

  for (uint64_t i = 0; i < 5; ++i) {
    delVector.OnDocumentDeleted(i);
  }
  vector<int> expectedValues;
  AssertEqual(expectedValues, delVector.GetDeleteVectorBitmap());
}

TEST(DeleteVector, ZigZagDelete) {
  DeleteVector delVector(g_TestRootDirectory, GetUniqueDBName(), "Collection", true);
  delVector.OnDocumentsInserted(5);

  vector<int> expectedValues;
  for (uint64_t i = 0; i < 5; i++) {
    if (i % 2 == 0) {
      delVector.OnDocumentDeleted(i);
    } else {
      expectedValues.push_back(i);
    }
  }

  AssertEqual(expectedValues, delVector.GetDeleteVectorBitmap());
}