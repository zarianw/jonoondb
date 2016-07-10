#include "gtest/gtest.h"
#include "test_utils.h"
#include "tweet_generated.h"
#include "all_field_type_generated.h"

using namespace jonoondb_test;
using namespace jonoondb_api;
using namespace flatbuffers;

void TestUtils::CompareTweetObject(const Document& doc,
                                   const BufferImpl& tweetObject) {
  auto tweet = GetTweet(tweetObject.GetData());
  ASSERT_EQ(doc.GetIntegerValueAsInt64("id"), tweet->id());
  ASSERT_STREQ(doc.GetStringValue("text").c_str(), tweet->text()->c_str());

  auto subDoc = doc.AllocateSubDocument();
  doc.GetDocumentValue("user", *subDoc.get());
  ASSERT_EQ(subDoc->GetIntegerValueAsInt64("id"), tweet->user()->id());
  ASSERT_STREQ(subDoc->GetStringValue("name").c_str(),
               tweet->user()->name()->c_str());
  ASSERT_DOUBLE_EQ(doc.GetFloatingValueAsDouble("rating"), tweet->rating());
  std::size_t size;
  auto data = doc.GetBlobValue("binData", size);
  ASSERT_EQ(memcmp(data, reinterpret_cast<const char*>(tweet->binData()),
                   size), 0);
}

Buffer TestUtils::GetAllFieldTypeObjectBuffer(char field1,
                                              unsigned char field2,
                                              bool field3, int16_t field4,
                                              uint16_t field5, int32_t field6,
                                              uint32_t field7, float field8,
                                              int64_t field9, double field10,
                                              const std::string& field11,
                                              const std::string& field12,
                                              const std::string& field13) {
  FlatBufferBuilder fbb;
  // create nested object
  auto str11 = fbb.CreateString(field11);
  auto vec12 = fbb.CreateVector<int8_t>(
    reinterpret_cast<const int8_t*>(field11.c_str()), field11.size());
  auto vec13 = fbb.CreateVector<uint8_t>(
    reinterpret_cast<const uint8_t*>(field11.c_str()), field11.size());
  auto nestedObj = CreateNestedAllFieldType(fbb, field1, field2, field3,
                                            field4, field5, field6, field7,
                                            field8, field9, field10, str11,
                                            vec12, vec13);
  // create parent object
  auto str2_11 = fbb.CreateString(field11);
  auto vec2_12 = fbb.CreateVector<int8_t>(
    reinterpret_cast<const int8_t*>(field11.c_str()), field11.size());
  auto vec2_13 = fbb.CreateVector<uint8_t>(
    reinterpret_cast<const uint8_t*>(field11.c_str()), field11.size());
  auto parentObj = CreateAllFieldType(fbb, field1, field2, field3, field4,
                                      field5, field6, field7, field8,
                                      field9, field10, str2_11, nestedObj,
                                      vec2_12, vec2_13);
  fbb.Finish(parentObj);

  return Buffer((char*)fbb.GetBufferPointer(), fbb.GetSize(), fbb.GetSize());
}
