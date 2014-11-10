#pragma once

#include "indexer.h"

namespace jonoondb_api {
// Forward declarations
class Status;
class Document;

class EWAHCompressedBitmapIndexer final : public Indexer {
public:
  ~EWAHCompressedBitmapIndexer() override;
  Status ValidateForInsert(const Document& document) override;
  void Insert(const Document& document) override;
};
} // namespace jonoondb_api