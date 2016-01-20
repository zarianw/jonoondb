#pragma once

#include <map>
#include <memory>
#include <cstdint>
#include <sstream>
#include <vector>
#include <string>
#include <cmath>
#include "indexer.h"
#include "index_info_impl.h"
#include "status_impl.h"
#include "string_utils.h"
#include "document.h"
#include "mama_jennies_bitmap.h"
#include "exception_utils.h"
#include "index_stat.h"
#include "constraint.h"
#include "enums.h"

namespace jonoondb_api {

class EWAHCompressedBitmapIndexerString final : public Indexer {
public:
  static void Construct(const IndexInfoImpl& indexInfo,
    const FieldType& fieldType,
    EWAHCompressedBitmapIndexerString*& obj) {
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
      ss << "Argument fieldType " << GetFieldString(fieldType)
        << " is not valid for EWAHCompressedBitmapIndexerString.";
      errorMsg = ss.str();
    }

    if (errorMsg.length() > 0) {
      throw InvalidArgumentException(errorMsg, __FILE__, __func__, __LINE__);
    }

    std::vector<std::string> tokens = StringUtils::Split(indexInfo.GetColumnName(),
      ".");
    IndexStat indexStat(indexInfo, fieldType);
    obj = new EWAHCompressedBitmapIndexerString(indexStat, tokens);
  }

  ~EWAHCompressedBitmapIndexerString() override {
  }

  static bool IsValidFieldType(FieldType fieldType) {
    return (fieldType == FieldType::BASE_TYPE_STRING);
  }

  void ValidateForInsert(const Document& document) override {
    if (m_fieldNameTokens.size() > 1) {
      auto subDoc = DocumentUtils::GetSubDocumentRecursively(document, m_fieldNameTokens);
      subDoc->VerifyFieldForRead(m_fieldNameTokens.back(),
        m_indexStat.GetFieldType());
    } else {
      document.VerifyFieldForRead(m_fieldNameTokens.back(),
        m_indexStat.GetFieldType());
    }
  }

  void Insert(std::uint64_t documentID, const Document& document) override {
    if (m_fieldNameTokens.size() > 1) {
      auto subDoc = DocumentUtils::GetSubDocumentRecursively(document, m_fieldNameTokens);
      InsertInternal(documentID, *subDoc.get());
    } else {
      InsertInternal(documentID, document);
    }
  }

  const IndexStat& GetIndexStats() override {
    return m_indexStat;
  }

  std::vector<std::shared_ptr<MamaJenniesBitmap>> Filter(const Constraint& constraint) override {    
    assert(constraint.operandType == OperandType::STRING);
    switch (constraint.op) {
      case jonoondb_api::IndexConstraintOperator::EQUAL:
        return GetBitmapEQ(constraint);
      case jonoondb_api::IndexConstraintOperator::LESS_THAN:
        return GetBitmapLT(constraint, false);        
      case jonoondb_api::IndexConstraintOperator::LESS_THAN_EQUAL:
        return GetBitmapLT(constraint, true);        
      case jonoondb_api::IndexConstraintOperator::GREATER_THAN:
        return GetBitmapGT(constraint, false);        
      case jonoondb_api::IndexConstraintOperator::GREATER_THAN_EQUAL:
        return GetBitmapGT(constraint, true);
      case jonoondb_api::IndexConstraintOperator::MATCH:
        break;
      default:
        std::ostringstream ss;
        ss << "IndexConstraintOperator type " << static_cast<std::int32_t>(constraint.op) << " is not valid.";
        throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
    }
  }

private:
  EWAHCompressedBitmapIndexerString(const IndexStat& indexStat,
    std::vector<std::string>& fieldNameTokens)
    : m_indexStat(indexStat),
    m_fieldNameTokens(fieldNameTokens) {
  }

  void InsertInternal(std::uint64_t documentID, const Document& document) {
     switch (m_indexStat.GetFieldType()) {
      case FieldType::BASE_TYPE_STRING: {
        auto val = document.GetStringValue(m_fieldNameTokens.back());        
        auto compressedBitmap = m_compressedBitmaps.find(val);
        if (compressedBitmap == m_compressedBitmaps.end()) {
          auto bm = shared_ptr < MamaJenniesBitmap >(new MamaJenniesBitmap());
          bm->Add(documentID);
          m_compressedBitmaps[val] = bm;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }      
      default: {
        // This can never happen
        std::ostringstream ss;
        ss << "FieldType " << GetFieldString(m_indexStat.GetFieldType())
          << " is not valid for EWAHCompressedBitmapIndexerString.";
        throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
      }
    }    
  }

  std::vector<std::shared_ptr<MamaJenniesBitmap>> GetBitmapEQ(const Constraint& constraint) {    
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    if (constraint.operandType == OperandType::STRING) {
      auto iter = m_compressedBitmaps.find(constraint.strVal);
      if (iter != m_compressedBitmaps.end()) {
        bitmaps.push_back(iter->second);
      }
    }
    
    return bitmaps;
  }

  std::vector<std::shared_ptr<MamaJenniesBitmap>> GetBitmapLT(const Constraint& constraint, bool orEqual) {    
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;    
    if (constraint.operandType == OperandType::STRING) {
      for (auto& item : m_compressedBitmaps) {
        if (item.first.compare(constraint.strVal) < 0) {        
          bitmaps.push_back(item.second);
        } else {
          if (orEqual && item.first.compare(constraint.strVal) == 0) {
            bitmaps.push_back(item.second);
          }
          break;
        }
      }      
    }
    
    return bitmaps;
  }  

  std::vector<std::shared_ptr<MamaJenniesBitmap>> GetBitmapGT(const Constraint& constraint, bool isEqual) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    if (constraint.operandType == OperandType::STRING) {
      std::map<std::string, std::shared_ptr<MamaJenniesBitmap>>::const_iterator iter;
      if (isEqual) {
        iter = m_compressedBitmaps.lower_bound(constraint.strVal);
      } else {
        iter = m_compressedBitmaps.upper_bound(constraint.strVal);
      }

      if (iter != m_compressedBitmaps.end()) {
        bitmaps.push_back(iter->second);
        iter++;
      }
    }

    return bitmaps;
  }

  IndexStat m_indexStat;
  std::vector<std::string> m_fieldNameTokens;
  std::map<std::string, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmaps;
};
}  // namespace jonoondb_api
