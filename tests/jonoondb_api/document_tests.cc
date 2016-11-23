#include <string>
#include <memory>
#include "gtest/gtest.h"
#include "flatbuffers/flatbuffers.h"
#include "test_utils.h"
#include "jonoondb_api_test_utils.h"
#include "document.h"
#include "document_factory.h"
#include "index_info_impl.h"
#include "enums.h"
#include "options_impl.h"
#include "buffer_impl.h"
#include "tweet_generated.h"
#include "document_schema_factory.h"
#include "document_schema.h"
#include "file.h"

using namespace std;
using namespace flatbuffers;
using namespace jonoondb_api;
using namespace jonoondb_test;
using namespace jonoondb_api_test;

TEST(Document, Flatbuffers_GetValues_ValidBuffer) {
  string filePath = GetSchemaFilePath("tweet.bfbs");
  string schema = File::Read(filePath);
  BufferImpl documentData = TestUtils::GetTweetObject();
  shared_ptr<DocumentSchema>
      docSchemaPtr(DocumentSchemaFactory::CreateDocumentSchema(
      schema, SchemaType::FLAT_BUFFERS));

  auto doc = DocumentFactory::CreateDocument(*docSchemaPtr, documentData);
  TestUtils::CompareTweetObject(*doc.get(), documentData);
}
