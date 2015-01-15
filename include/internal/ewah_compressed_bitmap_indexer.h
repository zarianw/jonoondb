#pragma once

#include <map>
#include <memory>
#include "indexer.h"
#include "index_info.h"
#include "status.h"

namespace jonoondb_api {
// Forward declarations
class Document;
class MamaJenniesBitmap;

template<typename T>
class EWAHCompressedBitmapIndexer final : public Indexer {
 public:
  EWAHCompressedBitmapIndexer(const IndexInfo& indexInfo)
      : m_indexInfo(indexInfo) {
  }
  ~EWAHCompressedBitmapIndexer() override {
  }
  Status ValidateForInsert(const Document& document) override {
    return Status();
  }
  void Insert(const Document& document) override {
  }
 private:
  IndexInfo m_indexInfo;
  std::map<T, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmaps;
};
}  // namespace jonoondb_api
