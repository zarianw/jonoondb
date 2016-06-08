#include "gtest/gtest.h"
#include <string>
#include "buffer_impl.h"
#include "flatbuffers_document.h"
#include "flatbuffers_document_schema.h"
#include "enums.h"
#include "test_utils.h"
#include "all_field_type_generated.h"
#include "flatbuffers/flatbuffers.h"
#include "file.h"

using namespace std;
using namespace flatbuffers;
using namespace jonoondb_api;
using namespace jonoondb_test;

BufferImpl GetAllFieldTypeObject() {
  FlatBufferBuilder fbb;
  auto str = fbb.CreateString("joker");
  auto nestedobject =
      CreateNestedAllFieldType(fbb, 1, 2, true, 4, 5, 6, 7, 8.0f, 9, 10.0, str);
  auto str2 = fbb.CreateString("ali");
  auto allfieldtype = CreateAllFieldType(fbb, 1, 2, false, 4, 5, 6, 7, 8.0f, 9, 10.0, str2, nestedobject);
  fbb.Finish(allfieldtype);
  auto size = fbb.GetSize();
  return BufferImpl(reinterpret_cast<char*>(fbb.GetBufferPointer()),
                    size,
                    size);
}

void CompareObjects(const FlatbuffersDocument& fbDoc,
                    const AllFieldType& allFieldObj) {
  ASSERT_EQ(fbDoc.GetIntegerValueAsInt64("field1"), allFieldObj.field1());
  ASSERT_EQ(fbDoc.GetIntegerValueAsInt64("field2"), allFieldObj.field2());
  ASSERT_EQ(fbDoc.GetIntegerValueAsInt64("field3"),
            allFieldObj.field3() ? 1 : 0);
  ASSERT_EQ(fbDoc.GetIntegerValueAsInt64("field4"), allFieldObj.field4());
  ASSERT_EQ(fbDoc.GetIntegerValueAsInt64("field5"), allFieldObj.field5());
  ASSERT_EQ(fbDoc.GetIntegerValueAsInt64("field6"), allFieldObj.field6());
  ASSERT_EQ(fbDoc.GetIntegerValueAsInt64("field7"), allFieldObj.field7());
  ASSERT_EQ(fbDoc.GetFloatingValueAsDouble("field8"), allFieldObj.field8());
  ASSERT_EQ(fbDoc.GetIntegerValueAsInt64("field9"), allFieldObj.field9());
  ASSERT_EQ(fbDoc.GetFloatingValueAsDouble("field10"), allFieldObj.field10());
  ASSERT_STREQ(fbDoc.GetStringValue("field11").c_str(),
               allFieldObj.field11()->c_str());
  // Check rest of the fields
  auto subDoc = fbDoc.AllocateSubDocument();
  fbDoc.GetDocumentValue("nestedField", *subDoc);
  ASSERT_EQ(subDoc->GetIntegerValueAsInt64("field1"),
            allFieldObj.nestedField()->field1());
  ASSERT_EQ(subDoc->GetIntegerValueAsInt64("field2"),
            allFieldObj.nestedField()->field2());
  ASSERT_EQ(fbDoc.GetIntegerValueAsInt64("field3"),
            allFieldObj.field3() ? 1 : 0);
  ASSERT_EQ(subDoc->GetIntegerValueAsInt64("field4"),
            allFieldObj.nestedField()->field4());
  ASSERT_EQ(subDoc->GetIntegerValueAsInt64("field4"),
            allFieldObj.nestedField()->field4());
  ASSERT_EQ(subDoc->GetIntegerValueAsInt64("field5"),
            allFieldObj.nestedField()->field5());
  ASSERT_EQ(subDoc->GetIntegerValueAsInt64("field6"),
            allFieldObj.nestedField()->field6());
  ASSERT_EQ(subDoc->GetIntegerValueAsInt64("field7"),
            allFieldObj.nestedField()->field7());
  ASSERT_EQ(subDoc->GetFloatingValueAsDouble("field8"),
            allFieldObj.nestedField()->field8());
  ASSERT_EQ(subDoc->GetIntegerValueAsInt64("field9"),
            allFieldObj.nestedField()->field9());
  ASSERT_EQ(subDoc->GetFloatingValueAsDouble("field10"),
            allFieldObj.nestedField()->field10());
  ASSERT_STREQ(subDoc->GetStringValue("field11").c_str(),
               allFieldObj.nestedField()->field11()->c_str());
}

TEST(FlatbuffersDocument, GetterTest) {
  // 1: Schema and generated file already exist
  auto schema = File::Read(GetSchemaFilePath("all_field_type.bfbs"));
  // 2: Implement the function GetAllFieldTypeObject
  auto documentData = GetAllFieldTypeObject();
  auto docSchema = make_shared<FlatbuffersDocumentSchema>(schema);
  FlatbuffersDocument fbDoc(docSchema.get(), &documentData);
  auto buf2 = documentData;
  auto allFieldTypeObj = flatbuffers::GetRoot<AllFieldType>(buf2.GetData());
  // 3: Compare the values of FlatBufferDocument object and FlatBufferObject
  CompareObjects(fbDoc, *allFieldTypeObj);
}