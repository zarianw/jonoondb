#pragma once

#include <memory>

namespace jonoondb_api {

// Forward declarations
class Status;
class IndexInfoImpl;
class Document;
class IndexStat;
class Constraint;
class MamaJenniesBitmap;

class Indexer {
 public:
  virtual ~Indexer() {
  }
  // This function validates that the document is valid for insert.
  // If this function returns without exception then the insert function call on the same document must succeed.
  virtual void ValidateForInsert(const Document& document) = 0;
  virtual void Insert(std::uint64_t documentID, const Document& document) = 0;
  virtual const IndexStat& GetIndexStats() = 0;
  virtual std::shared_ptr<MamaJenniesBitmap> Filter(const Constraint& constraints) = 0;
};
}
