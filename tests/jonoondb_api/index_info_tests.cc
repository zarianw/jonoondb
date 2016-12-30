#include "gtest/gtest.h"
#include "database.h"

using namespace jonoondb_api;

TEST(IndexInfo, Ctor) {
  IndexInfo indexInfo("index1", IndexType::INVERTED_COMPRESSED_BITMAP,
                      "column1", true);
  ASSERT_STREQ(indexInfo.GetIndexName(), "index1");
  ASSERT_STREQ(indexInfo.GetColumnName(), "column1");
  ASSERT_EQ(indexInfo.GetType(), IndexType::INVERTED_COMPRESSED_BITMAP);
  ASSERT_TRUE(indexInfo.GetIsAscending());
}

