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
#include "exception_utils.h"

namespace jonoondb_api {

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

    obj = new EWAHCompressedBitmapIndexer(indexInfo, fieldType,
      StringUtils::Split(indexInfo.GetColumn(0), "."));
    return Status();     
  }
  
  ~EWAHCompressedBitmapIndexer() override {
  }

  Status ValidateForInsert(const Document& document) override {    
    Document* subDoc;
    switch (m_fieldType)
    {
      case FieldType::BASE_TYPE_UINT8: {
        std::uint8_t val;  
        auto sts = GetSubDocumentRecursively(document, subDoc);
        if (!sts.OK()) {
          return sts;
        }
        return subDoc->GetScalarValueAsUInt8(m_fieldNameTokens.back().c_str(), val);
      }
      case FieldType::BASE_TYPE_UINT16: {
        std::uint16_t val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        if (!sts.OK()) {
          return sts;
        }
        return subDoc->GetScalarValueAsUInt16(m_fieldNameTokens.back().c_str(), val);
      }        
      case FieldType::BASE_TYPE_UINT32: {
        std::uint32_t val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        if (!sts.OK()) {
          return sts;
        }
        return subDoc->GetScalarValueAsUInt32(m_fieldNameTokens.back().c_str(), val);
      }       
      case FieldType::BASE_TYPE_UINT64: {
        std::uint64_t val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        if (!sts.OK()) {
          return sts;
        }
        return subDoc->GetScalarValueAsUInt64(m_fieldNameTokens.back().c_str(), val);
      }       
      case FieldType::BASE_TYPE_INT8: {
        std::int8_t val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        if (!sts.OK()) {
          return sts;
        }
        return subDoc->GetScalarValueAsInt8(m_fieldNameTokens.back().c_str(), val);
      }        
      case FieldType::BASE_TYPE_INT16: {
        std::int16_t val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        if (!sts.OK()) {
          return sts;
        }
        return subDoc->GetScalarValueAsInt16(m_fieldNameTokens.back().c_str(), val);
      }        
      case FieldType::BASE_TYPE_INT32: {
        std::int32_t val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        if (!sts.OK()) {
          return sts;
        }
        return subDoc->GetScalarValueAsInt32(m_fieldNameTokens.back().c_str(), val);
      }
      case FieldType::BASE_TYPE_INT64: {
        std::int64_t val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        if (!sts.OK()) {
          return sts;
        }
        return subDoc->GetScalarValueAsInt64(m_fieldNameTokens.back().c_str(), val);
      }        
      case FieldType::BASE_TYPE_FLOAT32: {
        float val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        if (!sts.OK()) {
          return sts;
        }
        return subDoc->GetScalarValueAsFloat(m_fieldNameTokens.back().c_str(), val);
      }      
      case FieldType::BASE_TYPE_DOUBLE: {
        double val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        if (!sts.OK()) {
          return sts;
        }
        return subDoc->GetScalarValueAsDouble(m_fieldNameTokens.back().c_str(), val);
      }      
      case FieldType::BASE_TYPE_STRING: {
        char* val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        if (!sts.OK()) {
          return sts;
        }
        return subDoc->GetStringValue(m_fieldNameTokens.back().c_str(), val);
      }      
      default: {
        std::ostringstream ss;
        ss << "FieldType " << GetFieldString(m_fieldType) << " is not valid for EWAHCompressedBitmapIndexer.";
        std::string errorMsg = ss.str();
        return Status(kStatusGenericErrorCode, errorMsg.c_str(), errorMsg.length());
      }
    }    
  }

  void Insert(std::uint64_t documentID, const Document& document) override {
    Document* subDoc;
    switch (m_fieldType) {
      case FieldType::BASE_TYPE_UINT8: {
        std::uint8_t val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        assert(sts.OK());
        subDoc->GetScalarValueAsUInt8(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsUInt8.find(val);
        if (compressedBitmap == m_compressedBitmapsUInt8.end()) {
          auto bm = shared_ptr<MamaJenniesBitmap>(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsUInt8[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }
      case FieldType::BASE_TYPE_UINT16: {
        std::uint16_t val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        subDoc->GetScalarValueAsUInt16(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsUInt16.find(val);
        if (compressedBitmap == m_compressedBitmapsUInt16.end()) {
          auto bm = shared_ptr<MamaJenniesBitmap>(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsUInt16[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }
      case FieldType::BASE_TYPE_UINT32: {
        std::uint32_t val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        subDoc->GetScalarValueAsUInt32(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsUInt32.find(val);
        if (compressedBitmap == m_compressedBitmapsUInt32.end()) {
          auto bm = shared_ptr<MamaJenniesBitmap>(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsUInt32[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;        
      }
      case FieldType::BASE_TYPE_UINT64: {
        std::uint64_t val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        subDoc->GetScalarValueAsUInt64(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsUInt64.find(val);
        if (compressedBitmap == m_compressedBitmapsUInt64.end()) {
          auto bm = shared_ptr<MamaJenniesBitmap>(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsUInt64[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }
      case FieldType::BASE_TYPE_INT8: {
        std::int8_t val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        subDoc->GetScalarValueAsInt8(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsInt8.find(val);
        if (compressedBitmap == m_compressedBitmapsInt8.end()) {
          auto bm = shared_ptr<MamaJenniesBitmap>(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsInt8[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }
      case FieldType::BASE_TYPE_INT16: {
        std::int16_t val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        subDoc->GetScalarValueAsInt16(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsInt16.find(val);
        if (compressedBitmap == m_compressedBitmapsInt16.end()) {
          auto bm = shared_ptr<MamaJenniesBitmap>(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsInt16[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;        
      }
      case FieldType::BASE_TYPE_INT32: {
        std::int32_t val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        subDoc->GetScalarValueAsInt32(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsInt32.find(val);
        if (compressedBitmap == m_compressedBitmapsInt32.end()) {
          auto bm = shared_ptr<MamaJenniesBitmap>(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsInt32[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;        
      }
      case FieldType::BASE_TYPE_INT64: {
        std::int64_t val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        subDoc->GetScalarValueAsInt64(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsInt64.find(val);
        if (compressedBitmap == m_compressedBitmapsInt64.end()) {
          auto bm = shared_ptr<MamaJenniesBitmap>(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsInt64[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;        
      }
      case FieldType::BASE_TYPE_FLOAT32: {
        float val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        subDoc->GetScalarValueAsFloat(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsFloat32.find(val);
        if (compressedBitmap == m_compressedBitmapsFloat32.end()) {
          auto bm = shared_ptr<MamaJenniesBitmap>(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsFloat32[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;        
      }
      case FieldType::BASE_TYPE_DOUBLE: {
        double val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        subDoc->GetScalarValueAsDouble(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsDouble.find(val);
        if (compressedBitmap == m_compressedBitmapsDouble.end()) {
          auto bm = shared_ptr<MamaJenniesBitmap>(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsDouble[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;        
      }
      case FieldType::BASE_TYPE_STRING: {
        char* val;
        auto sts = GetSubDocumentRecursively(document, subDoc);
        assert(sts.OK());
        subDoc->GetStringValue(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsString.find(val);
        if (compressedBitmap == m_compressedBitmapsString.end()) {
          auto bm = shared_ptr<MamaJenniesBitmap>(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsString[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }
      default: {
        assert(false);
      }
    }
  }

 private:
  EWAHCompressedBitmapIndexer(const IndexInfo& indexInfo, FieldType fieldType, std::vector<std::string>& fieldNameTokens)
    : m_indexInfo(indexInfo), m_fieldType(fieldType), m_fieldNameTokens(fieldNameTokens) {
  }

  static bool IsValidFieldType(FieldType fieldType) {
    return (fieldType == FieldType::BASE_TYPE_INT8 || fieldType == FieldType::BASE_TYPE_INT16 ||
      fieldType == FieldType::BASE_TYPE_INT32 || fieldType == FieldType::BASE_TYPE_INT64 ||
      fieldType == FieldType::BASE_TYPE_UINT8 || fieldType == FieldType::BASE_TYPE_UINT16 ||
      fieldType == FieldType::BASE_TYPE_UINT32 || fieldType == FieldType::BASE_TYPE_UINT64 ||
      fieldType == FieldType::BASE_TYPE_FLOAT32 || fieldType == FieldType::BASE_TYPE_DOUBLE ||
      fieldType == FieldType::BASE_TYPE_STRING);
  }

  Status GetSubDocumentRecursively(const Document& parentDoc, Document*& subDoc) {
    Document* doc = nullptr;
    Status sts = parentDoc.AllocateSubDocument(doc);
    if (!sts.OK()) {
      return sts;
    }
    
    for (size_t i = 0; i < m_fieldNameTokens.size() - 1; i++) {
      if (i == 0) {
        sts = parentDoc.GetDocumentValue(m_fieldNameTokens[i].c_str(), doc);
      } else {
        sts = doc->GetDocumentValue(m_fieldNameTokens[i].c_str(), doc);
      }
      if (!sts.OK()) {
        return sts;
      }
    }

    subDoc = doc;
    return sts;
  }

  IndexInfo m_indexInfo;
  FieldType m_fieldType;
  std::vector<std::string> m_fieldNameTokens;
  std::map<std::uint8_t, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmapsUInt8;
  std::map<std::uint16_t, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmapsUInt16;
  std::map<std::uint32_t, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmapsUInt32;
  std::map<std::uint64_t, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmapsUInt64;
  std::map<std::int8_t, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmapsInt8;
  std::map<std::int16_t, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmapsInt16;
  std::map<std::int32_t, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmapsInt32;
  std::map<std::int64_t, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmapsInt64;

  std::map<float, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmapsFloat32;
  std::map<double, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmapsDouble;
  std::map<std::string, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmapsString;
};
}  // namespace jonoondb_api
