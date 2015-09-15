#include <string>
#include <vector>
#include <cstdint>
#include <sstream>
#include "index_info.h"
#include "status.h"
#include "string_utils.h"
#include "enums.h"

using namespace jonoondb_api;
using namespace std;

struct IndexInfo::IndexInfoData {
  string Name;
  vector<string> Columns;
  bool IsAscending;
  IndexType Type;
};

IndexInfo::IndexInfo(const char* name, IndexType type, const char* columns[],
                     size_t columnsLength, bool isAscending) {
  m_indexInfoData = new IndexInfoData();
  if (name != nullptr) {
    m_indexInfoData->Name = name;
  }
  m_indexInfoData->IsAscending = isAscending;
  m_indexInfoData->Type = type;
  for (size_t i = 0; i < columnsLength; i++) {
    if (columns[i] != nullptr) {
      m_indexInfoData->Columns.push_back(string(columns[i]));
    }
  }
}

IndexInfo::IndexInfo()
    : m_indexInfoData(new IndexInfoData()) {
}

IndexInfo::IndexInfo(const IndexInfo& other) {
  if (this != &other) {
    m_indexInfoData = new IndexInfoData();
    m_indexInfoData->Name = other.GetName();
    m_indexInfoData->IsAscending = other.GetIsAscending();
    m_indexInfoData->Type = other.GetType();
    size_t length = other.GetColumnsLength();
    SetColumnsLength(length);
    for (size_t i = 0; i < length; i++) {
      m_indexInfoData->Columns[i] = other.GetColumn(i);
    }
  }
}

IndexInfo::~IndexInfo() {
  delete m_indexInfoData;
}

Status IndexInfo::Validate() {
  string errorMessage;

  if (StringUtils::IsNullOrEmpty(m_indexInfoData->Name)) {
    errorMessage = "Index name is null or empty.";
    return Status(kStatusGenericErrorCode, errorMessage.c_str(),
                  __FILE__, "", __LINE__);
  }

  if (m_indexInfoData->Columns.size() < 1) {
    errorMessage = "Index columns should be greater than 0.";
    return Status(kStatusGenericErrorCode, errorMessage.c_str(),
                  __FILE__, "", __LINE__);
  }

  return Status();
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

void IndexInfo::SetName(const char* value) {
  m_indexInfoData->Name = value;
}

const char* IndexInfo::GetName() const {
  return m_indexInfoData->Name.c_str();
}

const char* IndexInfo::GetColumn(size_t index) const {
  if (index < m_indexInfoData->Columns.size()) {
    return m_indexInfoData->Columns[index].c_str();
  } else {
    return nullptr;
  }
}

Status IndexInfo::SetColumn(size_t index, const char* column) {
  if (index >= m_indexInfoData->Columns.size()) {
    ostringstream ss;
    ss << "Cannot set column. Index value " << index
       << " is more than the size of the column vector which is "
       << m_indexInfoData->Columns.size();
    ".";
    string errorMsg = ss.str();
    return Status(kStatusInvalidArgumentCode, errorMsg.c_str(), __FILE__, "",
                  __LINE__);
  }

  m_indexInfoData->Columns[index] = column;

  return Status();
}

size_t IndexInfo::GetColumnsLength() const {
  return m_indexInfoData->Columns.size();
}

void IndexInfo::SetColumnsLength(size_t length) {
  m_indexInfoData->Columns.resize(length);
}

