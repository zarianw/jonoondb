#pragma once

#include <cstdint>

namespace jonoondb_api
{
  // Forward declaration
  class Status;
  class Buffer;
  enum class IndexType : std::int16_t;

  class IndexInfo
  {
  public:
    IndexInfo(const char* name, IndexType type, const char* columns[], size_t columnsLength, bool isAscending);
    IndexInfo();
    IndexInfo(const IndexInfo& other);
    ~IndexInfo();
    Status Validate();
    void SetName(const char* value);
    const char* GetName() const;
    void SetIsAscending(bool value);
    bool GetIsAscending() const;
    void SetType(IndexType value);
    IndexType GetType() const;
    size_t GetColumnsLength() const;
    void SetColumnsLength(size_t value);
    const char* GetColumn(size_t index) const;
    Status SetColumn(size_t index, const char* column);

  private:
    // Forward declaration
    struct IndexInfoData;
    IndexInfoData* m_indexInfoData;
  };
}

