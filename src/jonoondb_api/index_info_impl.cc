#include <string>
#include <vector>
#include <cstdint>
#include <sstream>
#include "index_info_impl.h"
#include "string_utils.h"
#include "enums.h"

using namespace jonoondb_api;

struct IndexInfoImpl::IndexInfoData {
  std::string Name;
  std::string ColumnName;
  bool IsAscending;
  IndexType Type;
};

IndexInfoImpl::IndexInfoImpl(const std::string& name, IndexType type, const std::string& columnName,
                     bool isAscending) {
  m_indexInfoData = new IndexInfoData();
  m_indexInfoData->Name = name;  
  m_indexInfoData->IsAscending = isAscending;
  m_indexInfoData->Type = type;
  m_indexInfoData->ColumnName = columnName;
}

IndexInfoImpl::IndexInfoImpl()
    : m_indexInfoData(new IndexInfoData()) {
}

IndexInfoImpl::IndexInfoImpl(const IndexInfoImpl& other) {
  if (this != &other) {
    m_indexInfoData = new IndexInfoData();
    m_indexInfoData->Name = other.GetIndexName();
    m_indexInfoData->IsAscending = other.GetIsAscending();
    m_indexInfoData->Type = other.GetType();
    m_indexInfoData->ColumnName = other.GetColumnName();
  }
}

IndexInfoImpl::~IndexInfoImpl() {
  delete m_indexInfoData;
}

IndexInfoImpl& IndexInfoImpl::operator=(const IndexInfoImpl& other) {
  if (this != &other) {
    m_indexInfoData->Name = other.GetIndexName();
    m_indexInfoData->IsAscending = other.GetIsAscending();
    m_indexInfoData->Type = other.GetType();
    m_indexInfoData->ColumnName = other.GetColumnName();
  }  

  return *this;
}

void IndexInfoImpl::SetIsAscending(bool value) {
  m_indexInfoData->IsAscending = value;
}

bool IndexInfoImpl::GetIsAscending() const {
  return m_indexInfoData->IsAscending;
}

void IndexInfoImpl::SetType(IndexType value) {
  m_indexInfoData->Type = value;
}

IndexType IndexInfoImpl::GetType() const {
  return m_indexInfoData->Type;
}

void IndexInfoImpl::SetIndexName(const std::string& value) {
  m_indexInfoData->Name = value;
}

const std::string& IndexInfoImpl::GetIndexName() const {
  return m_indexInfoData->Name;
}

const std::string& IndexInfoImpl::GetColumnName() const {
  return m_indexInfoData->ColumnName;
}

void IndexInfoImpl::SetColumnName(const std::string& value) {
  m_indexInfoData->ColumnName = value;
}
