#pragma once

#include <map>
#include <memory>
#include <cstdint>
#include <sstream>
#include "indexer.h"
#include "index_info.h"
#include "status.h"
#include "string_utils.h"
#include "document.h"
#include "mama_jennies_bitmap.h"

namespace jonoondb_api {

template<typename T>
class EWAHCompressedBitmapIndexer final : public Indexer {
 public:
  static Status Construct(const IndexInfo& indexInfo, FieldType fieldType,
    EWAHCompressedBitmapIndexer*& obj) {
    // TODO: Add index name in the error message as well
    std::string errorMsg;
    if (indexInfo.GetColumnsLength() != 1) {
      errorMsg = "Argument indexInfo can only have 1 column for IndexType EWAHCompressedBitmap.";
    } else if (StringUtils::IsNullOrEmpty(indexInfo.GetName())) {
      errorMsg = "Argument indexInfo has null or empty name.";
    } else if (StringUtils::IsNullOrEmpty(indexInfo.GetColumn(0))) {
      errorMsg = "Argument indexInfo has null or empty column name.";
    } else if (indexInfo.GetType() != IndexType::EWAHCompressedBitmap) {
      errorMsg = "Argument indexInfo can only have IndexType EWAHCompressedBitmap for EWAHCompressedBitmapIndexer.";
    } else if (!IsValidFieldType(fieldType)) {
      std::ostringstream ss;
      ss << "Argument FieldType " << GetFieldString(fieldType) << " is not valid for EWAHCompressedBitmapIndexer.";
      errorMsg = ss.str();
    }

    if (errorMsg.length() > 0) {
      return Status(kStatusInvalidArgumentCode, errorMsg.c_str(), errorMsg.length());
    }

    obj = new EWAHCompressedBitmapIndexer(indexInfo, fieldType);
    return Status();     
  }
  
  ~EWAHCompressedBitmapIndexer() override {
  }

  Status ValidateForInsert(const Document& document) override {    
    switch (m_fieldType)
    {
      case FieldType::BASE_TYPE_UINT8: {
        std::uint8_t val;
        return document.GetScalarValueAsUInt8(m_indexInfo.GetName(), val);          
      }
      case FieldType::BASE_TYPE_UINT16: {
        std::uint16_t val;
        return document.GetScalarValueAsUInt16(m_indexInfo.GetName(), val);
      }        
      case FieldType::BASE_TYPE_UINT32: {
        std::uint32_t val;
        return document.GetScalarValueAsUInt32(m_indexInfo.GetName(), val);
      }       
      case FieldType::BASE_TYPE_UINT64: {
        std::uint64_t val;
        return document.GetScalarValueAsUInt64(m_indexInfo.GetName(), val);
      }       
      case FieldType::BASE_TYPE_INT8: {
        std::int8_t val;
        return document.GetScalarValueAsInt8(m_indexInfo.GetName(), val);
      }        
      case FieldType::BASE_TYPE_INT16: {
        std::int16_t val;
        return document.GetScalarValueAsInt16(m_indexInfo.GetName(), val);
      }        
      case FieldType::BASE_TYPE_INT32: {
        std::int32_t val;
        return document.GetScalarValueAsInt32(m_indexInfo.GetName(), val);
      }
      case FieldType::BASE_TYPE_INT64: {
        std::int64_t val;
        return document.GetScalarValueAsInt64(m_indexInfo.GetName(), val);
      }        
      case FieldType::BASE_TYPE_FLOAT32: {
        float val;
        return document.GetScalarValueAsFloat(m_indexInfo.GetName(), val);
      }      
      case FieldType::BASE_TYPE_DOUBLE: {
        double val;
        return document.GetScalarValueAsDouble(m_indexInfo.GetName(), val);
      }      
      case FieldType::BASE_TYPE_STRING: {
        char* val;
        return document.GetStringValue(m_indexInfo.GetName(), val);
      }      
      default: {
        std::ostringstream ss;
        ss << "FieldType " << GetFieldString(m_fieldType) << " is not valid for EWAHCompressedBitmapIndexer.";
        std::string errorMsg = ss.str();
        return Status(kStatusGenericErrorCode, errorMsg.c_str(), errorMsg.length());
      }
    }    
  }

  void Insert(const Document& document) override {
  }

 private:
  EWAHCompressedBitmapIndexer(const IndexInfo& indexInfo, FieldType fieldType)
    : m_indexInfo(indexInfo), m_fieldType(fieldType) {
  }

  static bool IsValidFieldType(FieldType fieldType) {
    return (fieldType == FieldType::BASE_TYPE_INT8 || fieldType == FieldType::BASE_TYPE_INT16 ||
      fieldType == FieldType::BASE_TYPE_INT32 || fieldType == FieldType::BASE_TYPE_INT64 ||
      fieldType == FieldType::BASE_TYPE_UINT8 || fieldType == FieldType::BASE_TYPE_UINT16 ||
      fieldType == FieldType::BASE_TYPE_UINT32 || fieldType == FieldType::BASE_TYPE_UINT64 ||
      fieldType == FieldType::BASE_TYPE_FLOAT32 || fieldType == FieldType::BASE_TYPE_DOUBLE ||
      fieldType == FieldType::BASE_TYPE_STRING);
  }

  IndexInfo m_indexInfo;
  FieldType m_fieldType;
  std::map<T, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmaps;  
};
}  // namespace jonoondb_api
