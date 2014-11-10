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
    ~IndexInfo();
    Status Validate();
    void SetName(const char* value);
    const char* GetName() const;
    void SetIsAscending(bool value);
    bool GetIsAscending() const;
    void SetType(IndexType value);
    IndexType GetType() const;

  private:
    // Forward declaration
    struct IndexInfoData;
    IndexInfoData* m_indexInfoData;
  };
}

