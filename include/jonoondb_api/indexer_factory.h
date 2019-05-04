#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

namespace jonoondb_api {
// Forward declarations
class IndexInfoImpl;
class Indexer;
enum class FieldType : std::int8_t;

class IndexerFactory {
 public:
  static Indexer* CreateIndexer(const IndexInfoImpl& indexInfo,
                                const FieldType& fieldType);

 private:
  IndexerFactory() = delete;
  IndexerFactory(const IndexerFactory&) = delete;
  IndexerFactory(IndexerFactory&&) = delete;
};
}  // namespace jonoondb_api
