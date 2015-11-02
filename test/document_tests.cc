#include <string>
#include <memory>
#include "gtest/gtest.h"
#include "flatbuffers/flatbuffers.h"
#include "test_utils.h"
#include "document.h"
#include "document_factory.h"
#include "status.h"
#include "index_info.h"
#include "enums.h"
#include "options.h"
#include "buffer.h"
#include "schemas/flatbuffers/tweet_generated.h"
#include "document_schema_factory.h"
#include "document_schema.h"

using namespace std;
using namespace flatbuffers;
using namespace jonoondb_api;
using namespace jonoondb_test;

void CompareTweetObject(const Document* doc, const Buffer& tweetObject) {
  auto tweet = GetTweet(tweetObject.GetData());
  ASSERT_TRUE(tweet != nullptr);
  uint64_t id;
  char* str;
  ASSERT_TRUE(doc->GetScalarValueAsUInt64("id", id).OK());
  ASSERT_TRUE(id == tweet->id());
  ASSERT_TRUE(doc->GetStringValue("text", str).OK());
  ASSERT_TRUE(strcmp(str, tweet->text()->c_str()) == 0);
  auto user = tweet->user();
  ASSERT_TRUE(user != nullptr);

  Document* subDoc;
  ASSERT_TRUE(doc->AllocateSubDocument(subDoc).OK());
  ASSERT_TRUE(doc->GetDocumentValue("user", subDoc).OK());
  ASSERT_TRUE(subDoc->GetScalarValueAsUInt64("id", id).OK());
  ASSERT_TRUE(id == tweet->user()->id());
  ASSERT_TRUE(subDoc->GetStringValue("name", str).OK());
  ASSERT_TRUE(strcmp(str, tweet->user()->name()->c_str()) == 0);
  subDoc->Dispose();
}

TEST(Document, Flatbuffers_GetValues_ValidBuffer) {
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  Buffer documentData;
  ASSERT_TRUE(GetTweetObject(documentData).OK());
  shared_ptr<DocumentSchema> docSchemaPtr(DocumentSchemaFactory::CreateDocumentSchema(
    schema.c_str(), SchemaType::FLAT_BUFFERS));
  
  auto doc = DocumentFactory::CreateDocument(docSchemaPtr, documentData);
  CompareTweetObject(doc, documentData);
  doc->Dispose();
}
