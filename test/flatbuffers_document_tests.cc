#include "gtest/gtest.h"
#include <string>
#include "buffer_impl.h"
#include "flatbuffers_document.h"
#include "flatbuffers_document_schema.h"
#include "enums.h"
#include "test_utils.h"
#include "schemas/flatbuffers/all_field_type_generated.h"
#include "flatbuffers/flatbuffers.h"

using namespace jonoondb_api;
using namespace jonoondb_test;
using namespace flatbuffers;
using namespace std;

BufferImpl GetAllFieldTypeObject() {
  //CreateNestedAllFieldType
  //CreateAllFieldType
  FlatBufferBuilder fbb;
  auto field12 = fbb.CreateString("joker");
  auto nestedobject = CreateNestedAllFieldType(fbb, 2, 3, 4, 5, 6, 7, 8, 9, 0, 11, 13, field12);
  auto outerfield12 = fbb.CreateString("ali");
  auto allfieldtype = CreateAllFieldType(fbb, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 11, outerfield12, nestedobject);
  fbb.Finish(allfieldtype);
  auto size = fbb.GetSize();
  BufferImpl buffer;
  if (size > buffer.GetCapacity()) {
    buffer.Resize(size);
  }
  buffer.Copy((char*)fbb.GetBufferPointer(), size);

  return buffer;
  
}
void CompareObjects(const FlatbuffersDocument &fbDoc, const AllFieldType& allFieldObj) {
  ASSERT_EQ(fbDoc.GetScalarValueAsInt8("field1"), allFieldObj.field1());
  ASSERT_EQ(fbDoc.GetScalarValueAsUInt8("field2"), allFieldObj.field2());
 // ASSERT_EQ(fbDoc.GetScalarValueAsbool("field3"), allFieldObj.field3()); GETSCALAR VALUE AS BOOL SHOULD BE IN THIS FIELD.
  ASSERT_EQ(fbDoc.GetScalarValueAsInt16("field4"), allFieldObj.field4());
  ASSERT_EQ(fbDoc.GetScalarValueAsUInt16("field5"), allFieldObj.field5());
  ASSERT_EQ(fbDoc.GetScalarValueAsInt32("field6"), allFieldObj.field6());
  ASSERT_EQ(fbDoc.GetScalarValueAsUInt32("field7"), allFieldObj.field7());
  ASSERT_EQ(fbDoc.GetScalarValueAsFloat("field8"), allFieldObj.field8());
 
  ASSERT_EQ(fbDoc.GetScalarValueAsInt64("field9"), allFieldObj.field9());
 
  ASSERT_EQ(fbDoc.GetScalarValueAsUInt64("field10"), allFieldObj.field10());
  
  ASSERT_EQ(fbDoc.GetScalarValueAsDouble("field11"), allFieldObj.field11());
  // Check rest of the fields
  
  auto subDoc = fbDoc.AllocateSubDocument();
  fbDoc.GetDocumentValue("nestedField", *subDoc);
  ASSERT_EQ(subDoc->GetScalarValueAsInt8("field1"), allFieldObj.nestedField()->field1());
  ASSERT_EQ(subDoc->GetScalarValueAsUInt8("field2"), allFieldObj.nestedField()->field2());
  ASSERT_EQ(subDoc->GetScalarValueAsInt16("field4"), allFieldObj.nestedField()->field4());
  ASSERT_EQ(subDoc->GetScalarValueAsUInt16("field4"), allFieldObj.nestedField()->field4());
  ASSERT_EQ(subDoc->GetScalarValueAsInt16("field5"), allFieldObj.nestedField()->field5());
  ASSERT_EQ(subDoc->GetScalarValueAsInt32("field6"), allFieldObj.nestedField()->field6());
  ASSERT_EQ(subDoc->GetScalarValueAsUInt32("field7"), allFieldObj.nestedField()->field7());
  ASSERT_EQ(subDoc->GetScalarValueAsFloat("field8"), allFieldObj.nestedField()->field8());
  ASSERT_EQ(subDoc->GetScalarValueAsInt64("field9"), allFieldObj.nestedField()->field9());
  ASSERT_EQ(subDoc->GetScalarValueAsUInt64("field10"), allFieldObj.nestedField()->field10());
  ASSERT_EQ(subDoc->GetScalarValueAsDouble("field11"), allFieldObj.nestedField()->field11());
  ASSERT_STREQ(subDoc->GetStringValue("field12").c_str(), allFieldObj.nestedField()->field12()->c_str());
}

TEST(FlatbuffersDocument, GetterTest) {
  // 1: Schema and generated file already exist
  auto filePath = g_SchemaFolderPath + "all_field_type.fbs";
  auto schemaText = ReadTextFile(filePath.c_str());
  // 2: Implement the function GetAllFieldTypeObject
  auto documentData = GetAllFieldTypeObject();
  auto docSchema = make_shared<FlatbuffersDocumentSchema>(schemaText, SchemaType::FLAT_BUFFERS);
  FlatbuffersDocument fbDoc(docSchema, documentData);
  auto buf2 = documentData;
  auto allFieldTypeObj = flatbuffers::GetRoot<AllFieldType>(buf2.GetData());

  // 3: Compare the values of FlatBufferDocument object and FlatBufferObject
  CompareObjects(fbDoc, *allFieldTypeObj);
}