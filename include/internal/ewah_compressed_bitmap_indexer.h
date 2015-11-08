#pragma once

#include <map>
#include <memory>
#include <cstdint>
#include <sstream>
#include <vector>
#include <string>
#include "indexer.h"
#include "index_info_impl.h"
#include "status.h"
#include "string_utils.h"
#include "document.h"
#include "mama_jennies_bitmap.h"
#include "exception_utils.h"
#include "index_stat.h"

namespace jonoondb_api {

class EWAHCompressedBitmapIndexer final : public Indexer {
 public:
  static Status Construct(const IndexInfoImpl& indexInfo,
                          const FieldType& fieldType,
                          EWAHCompressedBitmapIndexer*& obj) {
    // TODO: Add index name in the error message as well
    std::string errorMsg;
    if (StringUtils::IsNullOrEmpty(indexInfo.GetIndexName())) {
      errorMsg = "Argument indexInfo has null or empty name.";
    } else if (StringUtils::IsNullOrEmpty(indexInfo.GetColumnName())) {
      errorMsg = "Argument indexInfo has null or empty column name.";
    } else if (indexInfo.GetType() != IndexType::EWAHCompressedBitmap) {
      errorMsg =
          "Argument indexInfo can only have IndexType EWAHCompressedBitmap for EWAHCompressedBitmapIndexer.";
    } else if (!IsValidFieldType(fieldType)) {
      std::ostringstream ss;
      ss << "Argument FieldType " << GetFieldString(fieldType)
         << " is not valid for EWAHCompressedBitmapIndexer.";
      errorMsg = ss.str();
    }

    if (errorMsg.length() > 0) {
      throw InvalidArgumentException(errorMsg, __FILE__, "", __LINE__);
    }

    std::vector<std::string> tokens = StringUtils::Split(indexInfo.GetColumnName(),
                                                         ".");    
    IndexStat indexStat(indexInfo, fieldType);
    obj = new EWAHCompressedBitmapIndexer(indexStat, tokens);
    return Status();
  }

  ~EWAHCompressedBitmapIndexer() override {
  }  

  Status ValidateForInsert(const Document& document) override {
    Document* subDoc;    
    Status sts;
    if (m_fieldNameTokens.size() > 1) {
      sts = GetSubDocumentRecursively(document, subDoc);
      if (!sts) return sts;
      sts = CanAccessValue(subDoc, m_indexStat.GetFieldType(), 
                           m_fieldNameTokens.back().c_str());
      subDoc->Dispose();
    } else {
      sts = CanAccessValue(&document, m_indexStat.GetFieldType(),
                           m_fieldNameTokens.back().c_str());    }   

    return sts;
  } 

  void Insert(std::uint64_t documentID, const Document& document) override {
    if (m_fieldNameTokens.size() > 1) {
      Document* subDoc;
      auto sts = GetSubDocumentRecursively(document, subDoc);
      assert(sts.OK());
      InsertInternal(documentID, subDoc);
      subDoc->Dispose();
    } else {
      InsertInternal(documentID, &document);
    }    
  }

  const IndexStat& GetIndexStats() override {
    return m_indexStat;
  }

 private:
  EWAHCompressedBitmapIndexer(const IndexStat& indexStat,                              
                              std::vector<std::string>& fieldNameTokens)
      : m_indexStat(indexStat),
        m_fieldNameTokens(fieldNameTokens) {
  }

  static bool IsValidFieldType(FieldType fieldType) {
    return (fieldType == FieldType::BASE_TYPE_INT8
        || fieldType == FieldType::BASE_TYPE_INT16
        || fieldType == FieldType::BASE_TYPE_INT32
        || fieldType == FieldType::BASE_TYPE_INT64
        || fieldType == FieldType::BASE_TYPE_UINT8
        || fieldType == FieldType::BASE_TYPE_UINT16
        || fieldType == FieldType::BASE_TYPE_UINT32
        || fieldType == FieldType::BASE_TYPE_UINT64
        || fieldType == FieldType::BASE_TYPE_FLOAT32
        || fieldType == FieldType::BASE_TYPE_DOUBLE
        || fieldType == FieldType::BASE_TYPE_STRING);
  }

  Status GetSubDocumentRecursively(const Document& parentDoc,
                                   Document*& subDoc) {
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

  Status CanAccessValue(const Document* subDoc, FieldType fieldType,
    std::string fieldName) {
    switch (fieldType) {
      case FieldType::BASE_TYPE_UINT8: {
        std::uint8_t val;
        return subDoc->GetScalarValueAsUInt8(fieldName.c_str(), val);
      }
      case FieldType::BASE_TYPE_UINT16: {
        std::uint16_t val;
        return subDoc->GetScalarValueAsUInt16(m_fieldNameTokens.back().c_str(),
          val);
      }
      case FieldType::BASE_TYPE_UINT32: {
        std::uint32_t val;
        return subDoc->GetScalarValueAsUInt32(m_fieldNameTokens.back().c_str(),
          val);
      }
      case FieldType::BASE_TYPE_UINT64: {
        std::uint64_t val;
        return subDoc->GetScalarValueAsUInt64(m_fieldNameTokens.back().c_str(),
          val);
      }
      case FieldType::BASE_TYPE_INT8: {
        std::int8_t val;
        return subDoc->GetScalarValueAsInt8(m_fieldNameTokens.back().c_str(),
          val);
      }
      case FieldType::BASE_TYPE_INT16: {
        std::int16_t val;
        return subDoc->GetScalarValueAsInt16(m_fieldNameTokens.back().c_str(),
          val);
      }
      case FieldType::BASE_TYPE_INT32: {
        std::int32_t val;
        return subDoc->GetScalarValueAsInt32(m_fieldNameTokens.back().c_str(),
          val);
      }
      case FieldType::BASE_TYPE_INT64: {
        std::int64_t val;
        return subDoc->GetScalarValueAsInt64(m_fieldNameTokens.back().c_str(),
          val);
      }
      case FieldType::BASE_TYPE_FLOAT32: {
        float val;
        return subDoc->GetScalarValueAsFloat(m_fieldNameTokens.back().c_str(),
          val);
      }
      case FieldType::BASE_TYPE_DOUBLE: {
        double val;
        return subDoc->GetScalarValueAsDouble(m_fieldNameTokens.back().c_str(),
          val);
      }
      case FieldType::BASE_TYPE_STRING: {
        char* val;
        return subDoc->GetStringValue(m_fieldNameTokens.back().c_str(), val);
      }
      default: {
        std::ostringstream ss;
        ss << "FieldType " << GetFieldString(m_indexStat.GetFieldType())
          << " is not valid for EWAHCompressedBitmapIndexer.";        
        throw JonoonDBException(ss.str(), __FILE__, "", __LINE__);        
      }
    }
  }

  void InsertInternal(std::uint64_t documentID, const Document* document) {
    switch (m_indexStat.GetFieldType()) {
      case FieldType::BASE_TYPE_UINT8: {
        std::uint8_t val;
        document->GetScalarValueAsUInt8(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsUInt8.find(val);
        if (compressedBitmap == m_compressedBitmapsUInt8.end()) {
          auto bm = shared_ptr < MamaJenniesBitmap >(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsUInt8[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }
      case FieldType::BASE_TYPE_UINT16: {
        std::uint16_t val;
        document->GetScalarValueAsUInt16(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsUInt16.find(val);
        if (compressedBitmap == m_compressedBitmapsUInt16.end()) {
          auto bm = shared_ptr < MamaJenniesBitmap >(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsUInt16[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }
      case FieldType::BASE_TYPE_UINT32: {
        std::uint32_t val;
        document->GetScalarValueAsUInt32(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsUInt32.find(val);
        if (compressedBitmap == m_compressedBitmapsUInt32.end()) {
          auto bm = shared_ptr < MamaJenniesBitmap >(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsUInt32[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }
      case FieldType::BASE_TYPE_UINT64: {
        std::uint64_t val;
        document->GetScalarValueAsUInt64(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsUInt64.find(val);
        if (compressedBitmap == m_compressedBitmapsUInt64.end()) {
          auto bm = shared_ptr < MamaJenniesBitmap >(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsUInt64[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }
      case FieldType::BASE_TYPE_INT8: {
        std::int8_t val;
        document->GetScalarValueAsInt8(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsInt8.find(val);
        if (compressedBitmap == m_compressedBitmapsInt8.end()) {
          auto bm = shared_ptr < MamaJenniesBitmap >(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsInt8[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }
      case FieldType::BASE_TYPE_INT16: {
        std::int16_t val;
        document->GetScalarValueAsInt16(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsInt16.find(val);
        if (compressedBitmap == m_compressedBitmapsInt16.end()) {
          auto bm = shared_ptr < MamaJenniesBitmap >(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsInt16[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }
      case FieldType::BASE_TYPE_INT32: {
        std::int32_t val;
        document->GetScalarValueAsInt32(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsInt32.find(val);
        if (compressedBitmap == m_compressedBitmapsInt32.end()) {
          auto bm = shared_ptr < MamaJenniesBitmap >(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsInt32[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }
      case FieldType::BASE_TYPE_INT64: {
        std::int64_t val;
        document->GetScalarValueAsInt64(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsInt64.find(val);
        if (compressedBitmap == m_compressedBitmapsInt64.end()) {
          auto bm = shared_ptr < MamaJenniesBitmap >(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsInt64[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }
      case FieldType::BASE_TYPE_FLOAT32: {
        float val;
        document->GetScalarValueAsFloat(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsFloat32.find(val);
        if (compressedBitmap == m_compressedBitmapsFloat32.end()) {
          auto bm = shared_ptr < MamaJenniesBitmap >(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsFloat32[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }
      case FieldType::BASE_TYPE_DOUBLE: {
        double val;
        document->GetScalarValueAsDouble(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsDouble.find(val);
        if (compressedBitmap == m_compressedBitmapsDouble.end()) {
          auto bm = shared_ptr < MamaJenniesBitmap >(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmapsDouble[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }
      case FieldType::BASE_TYPE_STRING: {
        char* val;
        document->GetStringValue(m_fieldNameTokens.back().c_str(), val);
        auto compressedBitmap = m_compressedBitmapsString.find(val);
        if (compressedBitmap == m_compressedBitmapsString.end()) {
          auto bm = shared_ptr < MamaJenniesBitmap >(new MamaJenniesBitmap());
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

  IndexStat m_indexStat;
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
