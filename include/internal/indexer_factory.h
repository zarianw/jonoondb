#pragma once

#include <cstdint>
#include <unordered_map>
#include <string>

namespace jonoondb_api {
// Forward declarations
class Status;
class IndexInfoImpl;
class Indexer;
enum class FieldType
: std::int32_t;

class IndexerFactory {
 public:
   static Indexer* CreateIndexer(
      const IndexInfoImpl& indexInfo,
      const std::unordered_map<std::string, FieldType>& fieldName);
 private:
  IndexerFactory() = delete;
  IndexerFactory(const IndexerFactory&) = delete;
  IndexerFactory(IndexerFactory&&) = delete;
};
}

