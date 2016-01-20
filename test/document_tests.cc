#include <string>
#include <memory>
#include "gtest/gtest.h"
#include "flatbuffers/flatbuffers.h"
#include "test_utils.h"
#include "document.h"
#include "document_factory.h"
#include "index_info_impl.h"
#include "enums.h"
#include "options_impl.h"
#include "buffer_impl.h"
#include "schemas/flatbuffers/tweet_generated.h"
#include "document_schema_factory.h"
#include "document_schema.h"

using namespace std;
using namespace flatbuffers;
using namespace jonoondb_api;
using namespace jonoondb_test;

void CompareTweetObject(const Document& doc, const BufferImpl& tweetObject) {
  auto tweet = GetTweet(tweetObject.GetData());
  ASSERT_EQ(doc.GetScalarValueAsUInt64("id"), tweet->id());
  ASSERT_STREQ(doc.GetStringValue("text").c_str(), tweet->text()->c_str());

  auto subDoc = doc.AllocateSubDocument();
  doc.GetDocumentValue("user", *subDoc.get());  
  ASSERT_EQ(subDoc->GetScalarValueAsUInt64("id"), tweet->user()->id());  
  ASSERT_STREQ(subDoc->GetStringValue("name").c_str(), tweet->user()->name()->c_str());
}

TEST(Document, Flatbuffers_GetValues_ValidBuffer) {
  string filePath = g_SchemaFolderPath + "tweet.fbs";
  string schema = ReadTextFile(filePath.c_str());
  BufferImpl documentData = GetTweetObject();
  shared_ptr<DocumentSchema> docSchemaPtr(DocumentSchemaFactory::CreateDocumentSchema(
    schema.c_str(), SchemaType::FLAT_BUFFERS));
  
  auto doc = DocumentFactory::CreateDocument(docSchemaPtr, documentData);
  CompareTweetObject(*doc.get(), documentData);  
}
