#pragma once

#include <cstdint>
#include <cstddef>
#include <string>
#include "jonoondb_api_export.h"

namespace jonoondb_api {
// Forward declaration
class BufferImpl;
enum class IndexType
    : std::int32_t;

class IndexInfoImpl {
 public:
  IndexInfoImpl(std::string name, IndexType type, std::string columnName,
                bool isAscending);
  IndexInfoImpl();
  IndexInfoImpl(const IndexInfoImpl& other);
  ~IndexInfoImpl();
  IndexInfoImpl& operator=(const IndexInfoImpl& other);
  void SetIndexName(const std::string& value);
  const std::string& GetIndexName() const;
  void SetIsAscending(bool value);
  bool GetIsAscending() const;
  void SetType(IndexType value);
  IndexType GetType() const;
  const std::string& GetColumnName() const;
  void SetColumnName(const std::string& value);

 private:
  // Forward declaration
  struct IndexInfoData;
  IndexInfoData* m_indexInfoData;
};
}

