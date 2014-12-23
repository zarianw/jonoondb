#pragma once

#include "indexer.h"
#include "index_info.h"

namespace jonoondb_api {
// Forward declarations
class Status;
class Document;

class EWAHCompressedBitmapIndexer final : public Indexer {
public:
  EWAHCompressedBitmapIndexer(const IndexInfo& indexInfo);
  ~EWAHCompressedBitmapIndexer() override;
  Status ValidateForInsert(const Document& document) override;
  void Insert(const Document& document) override;
private:
  IndexInfo m_indexInfo;
};
} // namespace jonoondb_api