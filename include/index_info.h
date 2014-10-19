#pragma once

#include <cstdint>

namespace jonoondb_api
{
  // Forward declaration
  class Status;
  class Buffer;

  class IndexInfo
  {
  public:
    IndexInfo(const char* name, int16_t type, const char* columns[], int columnsLength, bool isAscending);
    IndexInfo();
    ~IndexInfo();
    Status Validate();
    void SetName(const char* value);
    const char* GetName() const;
    void SetIsAscending(bool value);
    bool GetIsAscending() const;
    void SetType(int value);
    int16_t GetType() const;

  private:
    // Forward declaration
    struct IndexInfoData;
    IndexInfoData* m_indexInfoData;
  };
}

