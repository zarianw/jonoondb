#include <string>
#include <vector>
#include <cstdint>
#include <sstream>
#include "index_info.h"
#include "status.h"
#include "string_utils.h"
#include "enums.h"

using namespace jonoondb_api;

struct IndexInfo::IndexInfoData {
  std::string Name;
  std::string ColumnName;
  bool IsAscending;
  IndexType Type;
};

IndexInfo::IndexInfo(const std::string& name, IndexType type, const std::string& columnName,
                     bool isAscending) {
  m_indexInfoData = new IndexInfoData();
  m_indexInfoData->Name = name;  
  m_indexInfoData->IsAscending = isAscending;
  m_indexInfoData->Type = type;
  m_indexInfoData->ColumnName = columnName;
}

IndexInfo::IndexInfo()
    : m_indexInfoData(new IndexInfoData()) {
}

IndexInfo::IndexInfo(const IndexInfo& other) {
  if (this != &other) {
    m_indexInfoData = new IndexInfoData();
    m_indexInfoData->Name = other.GetIndexName();
    m_indexInfoData->IsAscending = other.GetIsAscending();
    m_indexInfoData->Type = other.GetType();
    m_indexInfoData->ColumnName = other.GetColumnName();
  }
}

IndexInfo::~IndexInfo() {
  delete m_indexInfoData;
}

IndexInfo& IndexInfo::operator=(const IndexInfo& other) {
  if (this != &other) {
    m_indexInfoData->Name = other.GetIndexName();
    m_indexInfoData->IsAscending = other.GetIsAscending();
    m_indexInfoData->Type = other.GetType();
    m_indexInfoData->ColumnName = other.GetColumnName();
  }  

  return *this;
}

void IndexInfo::SetIsAscending(bool value) {
  m_indexInfoData->IsAscending = value;
}

bool IndexInfo::GetIsAscending() const {
  return m_indexInfoData->IsAscending;
}

void IndexInfo::SetType(IndexType value) {
  m_indexInfoData->Type = value;
}

IndexType IndexInfo::GetType() const {
  return m_indexInfoData->Type;
}

void IndexInfo::SetIndexName(const std::string& value) {
  m_indexInfoData->Name = value;
}

const std::string& IndexInfo::GetIndexName() const {
  return m_indexInfoData->Name;
}

const std::string& IndexInfo::GetColumnName() const {
  return m_indexInfoData->ColumnName;
}

void IndexInfo::SetColumnName(const std::string& value) {
  m_indexInfoData->ColumnName = value;
}
