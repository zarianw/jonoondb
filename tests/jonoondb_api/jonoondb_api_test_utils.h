#pragma once

#include <string>
#include "buffer_impl.h"
#include "jonoondb_api/document.h"

namespace jonoondb_api_test {
class TestUtils {
public:
  static jonoondb_api::BufferImpl GetTweetObject();
  static void CompareTweetObject(const jonoondb_api::Document& doc,
                                 const jonoondb_api::BufferImpl& tweetObject);  
};
}  // namespace jonoondb_test
