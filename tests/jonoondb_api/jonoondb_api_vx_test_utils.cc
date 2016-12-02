#include "gtest/gtest.h"
#include "jonoondb_api_vx_test_utils.h"
#include "tweet_generated.h"
#include "all_field_type_generated.h"

using namespace jonoondb_test;
using namespace jonoondb_api_vx_test;
using namespace jonoondb_api;
using namespace flatbuffers;

Options TestUtils::GetDefaultDBOptions() {
  Options opt;
  opt.SetMaxDataFileSize(1024 * 1024);
  return opt;
}

Buffer TestUtils::GetTweetObject(std::size_t tweetId, std::size_t userId,
                                  const std::string* nameStr, const std::string* textStr,
                                  double rating, const std::string* binData) {
  // create user object
  FlatBufferBuilder fbb;
  Offset<String> name = 0;
  if (nameStr) {
    name = fbb.CreateString(*nameStr);
  }
  auto user = CreateUser(fbb, name, userId);

  // create tweet
  Offset<String> text = 0;
  if (textStr) {
    text = fbb.CreateString(*textStr);
  }

  Offset<Vector<int8_t>> binDataVec = 0;
  if (binData) {
    binDataVec = fbb.CreateVector<int8_t>(
      reinterpret_cast<const int8_t*>(binData->data()),
      binData->size());
  }

  auto tweet = CreateTweet(fbb, tweetId, text, user, rating, binDataVec);

  fbb.Finish(tweet);
  auto size = fbb.GetSize();

  Buffer buffer((char*)fbb.GetBufferPointer(), size, size);
  return buffer;
}

flatbuffers::Offset<NestedAllFieldType> CreateNestedAllFieldTypeObject(
    FlatBufferBuilder& fbb, char field1,
    unsigned char field2,
    bool field3, int16_t field4,
    uint16_t field5, int32_t field6,
    uint32_t field7, float field8,
    int64_t field9, double field10,
    const std::string& field11,
    const std::string& field12,
    const std::string& field13) {
  // create nested object
  StructType s1(1, 2), s2(3, 4);
  auto str11 = fbb.CreateString(field11);
  auto vec12 = fbb.CreateVector<int8_t>(
    reinterpret_cast<const int8_t*>(field12.c_str()), field12.size());
  auto vec13 = fbb.CreateVector<uint8_t>(
    reinterpret_cast<const uint8_t*>(field13.c_str()), field13.size());
  flatbuffers::Offset<flatbuffers::String> strings[2];
  strings[0] = fbb.CreateString("1");
  strings[1] = fbb.CreateString("2");
  auto strVec = fbb.CreateVector(strings, 2);
  StructType structs[] = { s1, s2 };
  auto structVec = fbb.CreateVectorOfStructs(structs, 2);
  auto uA = CreateUnionTypeA(fbb, 1, 2);
  auto uB = CreateUnionTypeB(fbb, true, 3);

  return CreateNestedAllFieldType(fbb, field1, field2, field3,
                                  field4, field5, field6, field7,
                                  field8, field9, field10, str11,
                                  vec12, vec13, strVec, &s1, structVec,
                                  UnionType::UnionType_UnionTypeA,
                                  uA.Union(),
                                  UnionType::UnionType_UnionTypeB,
                                  uB.Union());
}

Buffer TestUtils::GetAllFieldTypeObjectBuffer(char field1,
                                              unsigned char field2,
                                              bool field3, int16_t field4,
                                              uint16_t field5, int32_t field6,
                                              uint32_t field7, float field8,
                                              int64_t field9, double field10,
                                              const std::string& field11,
                                              const std::string& field12,
                                              const std::string& field13,
                                              bool nullifyNestedField) {
  FlatBufferBuilder fbb;
  flatbuffers::Offset<NestedAllFieldType> nestedObj;
  StructType s1(1, 2), s2(3, 4);    
  // create nested object
  if (!nullifyNestedField) {           
    nestedObj = CreateNestedAllFieldTypeObject(fbb, field1, field2, field3,
                                               field4, field5, field6, field7,
                                               field8, field9, field10, field11,
                                               field12, field13);
  }

  // create parent object
  auto str2_11 = fbb.CreateString(field11);
  auto vec2_12 = fbb.CreateVector<int8_t>(
    reinterpret_cast<const int8_t*>(field12.c_str()), field12.size());
  auto vec2_13 = fbb.CreateVector<uint8_t>(
    reinterpret_cast<const uint8_t*>(field13.c_str()), field13.size());
  
  flatbuffers::Offset<flatbuffers::String> strings[2];
  strings[0] = fbb.CreateString("1");
  strings[1] = fbb.CreateString("2");
  auto strVec = fbb.CreateVector(strings, 2);
  StructType structs[] = { s1, s2 };
  auto structVec = fbb.CreateVectorOfStructs(structs, 2);
  auto uA = CreateUnionTypeA(fbb, 1, 2);
  auto uB = CreateUnionTypeB(fbb, true, 3);
  flatbuffers::Offset<NestedAllFieldType> nestedObjs[2];
  nestedObjs[0] = CreateNestedAllFieldTypeObject(fbb, field1, field2, field3,
                                                 field4, field5, field6, field7,
                                                 field8, field9, field10, field11,
                                                 field12, field13);
  nestedObjs[1] = CreateNestedAllFieldTypeObject(fbb, field1, field2, field3,
                                                 field4, field5, field6, field7,
                                                 field8, field9, field10, field11,
                                                 field12, field13);
  auto nestedObjVec = fbb.CreateVector(nestedObjs, 2);

  auto parentObj = CreateAllFieldType(fbb, field1, field2, field3, field4,
                                      field5, field6, field7, field8,
                                      field9, field10, str2_11, nestedObj,
                                      vec2_12, vec2_13, strVec, &s1, structVec,
                                      UnionType::UnionType_UnionTypeA,
                                      uA.Union(),
                                      UnionType::UnionType_UnionTypeB,
                                      uB.Union(), nestedObjVec);
  fbb.Finish(parentObj);

  return Buffer((char*)fbb.GetBufferPointer(), fbb.GetSize(), fbb.GetSize());
}
