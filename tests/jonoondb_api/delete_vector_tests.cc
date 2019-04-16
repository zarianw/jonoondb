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

TEST(DeleteVector, EmptyCheckOnEmptyDeleteVector) {
  DeleteVector delVector(g_TestRootDirectory, GetUniqueDBName(), "Collection", true);
  ASSERT_TRUE(delVector.Empty());

  // when documents are inserted the delete vector should remain empty
  delVector.OnDocumentsInserted(1);
  ASSERT_TRUE(delVector.Empty());
  delVector.OnDocumentsInserted(2);
  ASSERT_TRUE(delVector.Empty());
}

TEST(DeleteVector, CheckBitmapOnEmptyDeleteVector) {
  DeleteVector delVector(g_TestRootDirectory, GetUniqueDBName(), "Collection", true);
  int cnt = 0;
  for (uint64_t item : delVector.GetDeleteVectorBitmap()) {
    cnt++;
  }
  ASSERT_EQ(cnt, 0);

  delVector.OnDocumentsInserted(1);
  delVector.OnDocumentsInserted(2);

  cnt = 0;
  for (uint64_t item : delVector.GetDeleteVectorBitmap()) {
    cnt++;
  }
  ASSERT_EQ(cnt, 0);

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

TEST(DeleteVector, CheckBitmapOnNonEmptyDeleteVector) {
  DeleteVector delVector(g_TestRootDirectory, GetUniqueDBName(), "Collection", true);
  delVector.OnDocumentsInserted(5);
  delVector.OnDocumentDeleted(2);
  vector<int> expectedValues{0, 1, 3, 4};
  AssertEqual(expectedValues, delVector.GetDeleteVectorBitmap());
}