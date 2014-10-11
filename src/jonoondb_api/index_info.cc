#include <string>
#include <vector>
#include <cstdint>
#include "index_info.h"
#include "status.h"
#include "string_utils.h"



using namespace jonoondb_api;
using namespace jonoon_utils;
using namespace std;

struct IndexInfo::IndexInfoData
{
  string Name;
  vector<string> Columns;
  bool IsAscending;
  int16_t Type;
};

IndexInfo::IndexInfo(const char* name, int16_t type, char* columns[], int columnsLength, bool isAscending)
{
  m_indexInfoData = new IndexInfoData();
  if (name != nullptr)
  {
    m_indexInfoData->Name = name;
  }
  m_indexInfoData->IsAscending = isAscending;
  m_indexInfoData->Type = type;
  for (size_t i = 0; i < columnsLength; i++)
  {
    if (columns[i] != nullptr)
    {
      m_indexInfoData->Columns.push_back(string(columns[i]));
    }
  }
}

IndexInfo::IndexInfo() : m_indexInfoData(new IndexInfoData())
{
}

IndexInfo::~IndexInfo()
{
  delete m_indexInfoData;
}

Status IndexInfo::Validate()
{
  string errorMessage;

  if (StringUtils::IsNullOrEmpty(m_indexInfoData->Name))
  {
    errorMessage = "Index name is null or empty.";
    return Status(Status::GenericErrorCode, errorMessage.c_str(), (int32_t)errorMessage.length());
  }

  if (m_indexInfoData->Type < 1)
  {
    errorMessage = "Index type should be greater than 0.";
    return Status(Status::GenericErrorCode, errorMessage.c_str(), (int32_t)errorMessage.length());
  }

  if (m_indexInfoData->Columns.size() < 1)
  {
    errorMessage = "Index columns should be greater than 0.";
    return Status(Status::GenericErrorCode, errorMessage.c_str(), (int32_t)errorMessage.length());
  }

  return Status();
}

void IndexInfo::SetIsAscending(bool value)
{
  m_indexInfoData->IsAscending = value;
}

bool IndexInfo::GetIsAscending() const
{
  return m_indexInfoData->IsAscending;
}

void IndexInfo::SetType(int value)
{
  m_indexInfoData->Type = value;
}

int16_t IndexInfo::GetType() const
{
  return m_indexInfoData->Type;
}

void IndexInfo::SetName(const char* value)
{
  m_indexInfoData->Name = value;
}

const char* IndexInfo::GetName() const
{
  return m_indexInfoData->Name.c_str();
}


