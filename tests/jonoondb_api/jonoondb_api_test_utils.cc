#include "gtest/gtest.h"
#include "jonoondb_api_test_utils.h"
#include "tweet_generated.h"
#include "all_field_type_generated.h"

using namespace jonoondb_test;
using namespace jonoondb_api;
using namespace jonoondb_api_test;
using namespace flatbuffers;

BufferImpl TestUtils::GetTweetObject() {
  // create user object
  FlatBufferBuilder fbb;
  auto name = fbb.CreateString("Zarian");
  auto user = CreateUser(fbb, name, 1);

  // create tweet
  auto text = fbb.CreateString("Say hello to my little friend!");
  auto tweet = CreateTweet(fbb, 1, text, user);

  fbb.Finish(tweet);
  auto size = fbb.GetSize();
  return BufferImpl((char*)fbb.GetBufferPointer(), size, size);
}

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