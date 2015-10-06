#pragma once

#include "index_info.h"
#include "enums.h"

namespace jonoondb_api {
class IndexStat {
public:
  IndexStat();
  IndexStat(const IndexInfo& indexInfo, FieldType fieldType);
  const IndexInfo& GetIndexInfo() const;
  FieldType GetFieldType() const;
private:
  IndexInfo m_indexInfo;
  FieldType m_fieldType;
  // In future add selectivity, avgEntrySizeInBytes etc.
};
} // jonoondb_api