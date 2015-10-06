#pragma once

#include <cstdint>
#include <cstddef>

namespace jonoondb_api {
// Forward declaration
class Status;
class Buffer;
enum class IndexType
: std::int32_t;

class IndexInfo {
 public:
  IndexInfo(const char* name, IndexType type, const char* columns[],
            std::size_t columnsLength, bool isAscending);
  IndexInfo();
  IndexInfo(const IndexInfo& other);
  ~IndexInfo();
  IndexInfo& operator=(const IndexInfo& other);
  Status Validate();
  void SetName(const char* value);
  const char* GetName() const;
  void SetIsAscending(bool value);
  bool GetIsAscending() const;
  void SetType(IndexType value);
  IndexType GetType() const;
  std::size_t GetColumnsLength() const;
  void SetColumnsLength(std::size_t value);
  const char* GetColumn(std::size_t index) const;
  Status SetColumn(std::size_t index, const char* column);

 private:
  // Forward declaration
  struct IndexInfoData;
  IndexInfoData* m_indexInfoData;
};
}

