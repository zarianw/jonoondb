#include <string>
#include "gtest/gtest.h"
#include "test_utils.h"
#include "all_field_type_generated.h"
#include "jonoondb_api_vx_test_utils.h"
#include "jonoondb_api/database.h"
#include "jonoondb_api/enums.h"
#include "jonoondb_api/file.h"

using namespace std;
using namespace flatbuffers;
using namespace jonoondb_api;
using namespace jonoondb_test;
using namespace jonoondb_api_vx_test;


TEST(Resultset, GetColumnType) {
  string dbName = "Resultset_GetColumnType";
  string collectionName = "CollectionName";
  string dbPath = g_TestRootDirectory;
  Database db(dbPath, dbName, TestUtils::GetDefaultDBOptions());

  string filePath = GetSchemaFilePath("all_field_type.bfbs");
  string schema = File::Read(filePath);
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

  auto rs = db.ExecuteSelect("select * FROM CollectionName;");
  while (rs.Next()) {
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("field1")), SqlType::INTEGER);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("field2")), SqlType::INTEGER);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("field3")), SqlType::INTEGER);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("field4")), SqlType::INTEGER);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("field5")), SqlType::INTEGER);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("field6")), SqlType::INTEGER);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("field7")), SqlType::INTEGER);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("field8")), SqlType::DOUBLE);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("field9")), SqlType::INTEGER);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("field10")), SqlType::DOUBLE);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("field11")), SqlType::TEXT);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("field12")), SqlType::BLOB);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("field13")), SqlType::BLOB);   
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("field15.field1")), SqlType::INTEGER);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("field15.field2")), SqlType::INTEGER);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("nestedField.field1")), SqlType::DB_NULL);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("nestedField.field2")), SqlType::DB_NULL);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("nestedField.field3")), SqlType::DB_NULL);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("nestedField.field4")), SqlType::DB_NULL);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("nestedField.field5")), SqlType::DB_NULL);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("nestedField.field6")), SqlType::DB_NULL);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("nestedField.field7")), SqlType::DB_NULL);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("nestedField.field8")), SqlType::DB_NULL);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("nestedField.field9")), SqlType::DB_NULL);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("nestedField.field10")), SqlType::DB_NULL);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("nestedField.field11")), SqlType::DB_NULL);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("nestedField.field12")), SqlType::DB_NULL);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("nestedField.field13")), SqlType::DB_NULL);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("nestedField.field15.field1")), SqlType::DB_NULL);
    ASSERT_EQ(rs.GetColumnType(rs.GetColumnIndex("nestedField.field15.field2")), SqlType::DB_NULL);
  }
}
