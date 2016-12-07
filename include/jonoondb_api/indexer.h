#pragma once

#include <memory>
#include <gsl/span.h>

namespace jonoondb_api {
// Forward declarations
class IndexInfoImpl;
class Document;
class IndexStat;
struct Constraint;
class MamaJenniesBitmap;
class BufferImpl;

class Indexer {
 public:
  virtual ~Indexer() {
  }
  virtual void Insert(std::uint64_t documentID, const Document& document) = 0;
  virtual const IndexStat& GetIndexStats() = 0;
  virtual std::shared_ptr<MamaJenniesBitmap>
      Filter(const Constraint& constraint) = 0;
  virtual std::shared_ptr<MamaJenniesBitmap> FilterRange(
      const Constraint& lowerConstraint,
      const Constraint& upperConstraint) = 0;

  virtual bool TryGetIntegerValue(std::uint64_t documentID, std::int64_t& val) {
    return false;
  }

  virtual bool TryGetDoubleValue(std::uint64_t documentID, double& val) {
    return false;
  }

  virtual bool TryGetStringValue(std::uint64_t documentID, std::string& val) {
    return false;
  }

  virtual bool TryGetBlobValue(std::uint64_t documentID, BufferImpl& val) {
    return false;
  }

  virtual bool TryGetIntegerVector(
      const gsl::span<std::uint64_t>& documentIDs,
      std::vector<std::int64_t>& values) {
    return false;
  }

  virtual bool TryGetDoubleVector(
      const gsl::span<std::uint64_t>& documentIDs,
      std::vector<double>& values) {
    return false;
  }
};
} // namespace jonoondb_api
