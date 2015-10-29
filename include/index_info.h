#pragma once

#include <cstdint>
#include <cstddef>
#include <string>

namespace jonoondb_api {
// Forward declaration
class Status;
class Buffer;
enum class IndexType
: std::int32_t;

class IndexInfo {
 public:
  IndexInfo(const std::string& name, IndexType type, const std::string& columnName,
            bool isAscending);
  IndexInfo();
  IndexInfo(const IndexInfo& other);
  ~IndexInfo();
  IndexInfo& operator=(const IndexInfo& other);  
  void SetName(const char* value);
  const char* GetName() const;
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

