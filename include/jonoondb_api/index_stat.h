#pragma once

#include "jonoondb_api/index_info_impl.h"
#include "jonoondb_api/field.h"

namespace jonoondb_api {
class IndexStat {
 public:
  IndexStat();
  IndexStat(const IndexInfoImpl& indexInfo, FieldType fieldType);
  const IndexInfoImpl& GetIndexInfo() const;
  FieldType GetFieldType() const;
 private:
  IndexInfoImpl m_indexInfo;
  FieldType m_fieldType;
  // In future add selectivity, avgEntrySizeInBytes etc.
};
} // jonoondb_api