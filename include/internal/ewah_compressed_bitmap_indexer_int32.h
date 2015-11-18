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
#include "constraint.h"
#include "enums.h"
#include <climits>

namespace jonoondb_api {

class EWAHCompressedBitmapIndexerInt32 final : public Indexer {
 public:
  static void Construct(const IndexInfoImpl& indexInfo,
                          const FieldType& fieldType,
                          EWAHCompressedBitmapIndexerInt32*& obj) {
    // TODO: Add index name in the error message as well
    std::string errorMsg;
    if (StringUtils::IsNullOrEmpty(indexInfo.GetIndexName())) {
      errorMsg = "Argument indexInfo has null or empty name.";
    } else if (StringUtils::IsNullOrEmpty(indexInfo.GetColumnName())) {
      errorMsg = "Argument indexInfo has null or empty column name.";
    } else if (indexInfo.GetType() != IndexType::EWAHCompressedBitmap) {
      errorMsg =
          "Argument indexInfo can only have IndexType EWAHCompressedBitmap for EWAHCompressedBitmapIndexer.";
    } else if (fieldType != FieldType::BASE_TYPE_INT32) {
      std::ostringstream ss;
      ss << "Argument fieldType " << GetFieldString(fieldType)
         << " is not valid for EWAHCompressedBitmapIndexerInt32.";
      errorMsg = ss.str();
    }

    if (errorMsg.length() > 0) {
      throw InvalidArgumentException(errorMsg, __FILE__, "", __LINE__);
    }

    std::vector<std::string> tokens = StringUtils::Split(indexInfo.GetColumnName(),
                                                         ".");    
    IndexStat indexStat(indexInfo, fieldType);
    obj = new EWAHCompressedBitmapIndexerInt32(indexStat, tokens);
  }

  ~EWAHCompressedBitmapIndexerInt32() override {
  }  

  void ValidateForInsert(const Document& document) override {
    if (m_fieldNameTokens.size() > 1) {
      auto subDoc = GetSubDocumentRecursively(document);
      // Todo: Make sure this function is not getting optimized out,
      // compiler may just do a no-op for this function
      subDoc->GetScalarValueAsInt32(m_fieldNameTokens.back().c_str());          
    } else {
      document.GetScalarValueAsInt32(m_fieldNameTokens.back().c_str());      
    }    
  } 

  void Insert(std::uint64_t documentID, const Document& document) override {
    if (m_fieldNameTokens.size() > 1) {
      auto subDoc = GetSubDocumentRecursively(document);      
      InsertInternal(documentID, *subDoc.get());      
    } else {
      InsertInternal(documentID, document);
    }    
  }

  const IndexStat& GetIndexStats() override {
    return m_indexStat;
  }

 private:
   EWAHCompressedBitmapIndexerInt32(const IndexStat& indexStat,
                              std::vector<std::string>& fieldNameTokens)
      : m_indexStat(indexStat),
        m_fieldNameTokens(fieldNameTokens) {
  }

  std::unique_ptr<Document> GetSubDocumentRecursively(const Document& parentDoc) {
    auto doc = parentDoc.AllocateSubDocument();       
    for (size_t i = 0; i < m_fieldNameTokens.size() - 1; i++) {
      if (i == 0) {
        parentDoc.GetDocumentValue(m_fieldNameTokens[i].c_str(), *doc.get());
      } else {
        doc->GetDocumentValue(m_fieldNameTokens[i].c_str(), *doc.get());
      }      
    }

    return doc;
  }

  void InsertInternal(std::uint64_t documentID, const Document& document) {
    std::int32_t val = document.GetScalarValueAsInt32(m_fieldNameTokens.back());
    auto compressedBitmap = m_compressedBitmapsInt32.find(val);
    if (compressedBitmap == m_compressedBitmapsInt32.end()) {
      auto bm = shared_ptr < MamaJenniesBitmap >(new MamaJenniesBitmap());
      bm->Add(documentID);
      m_compressedBitmapsInt32[val] = bm;
    } else {
      compressedBitmap->second->Add(documentID);
    }
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapEQ(const Constraint& constraint) {    
    if (constraint.operandType == FieldType::BASE_TYPE_INT64) {
      // First see if the provided operand is in our range
      if (constraint.operand.int64Val >= INT32_MIN && constraint.operand.int64Val <= INT32_MAX) {
        auto iter = m_compressedBitmapsInt32.find(constraint.operand.int64Val); //safe cast
        if (iter != m_compressedBitmapsInt32.end()) {
          return iter->second;
        }
      }      
    } else if (constraint.operandType == FieldType::BASE_TYPE_DOUBLE) {
      // Check if double has no fractional part
      std::int64_t intVal = static_cast<std::int64_t>(constraint.operand.doubleVal);
      if (constraint.operand.doubleVal == intVal) {
        // See if the provided operand is in our range
        if (constraint.operand.doubleVal >= INT32_MIN_AS_DOUBLE && constraint.operand.doubleVal <= INT32_MAX_AS_DOUBLE) {          
          auto iter = m_compressedBitmapsInt32.find(constraint.operand.doubleVal); //safe cast
          if (iter != m_compressedBitmapsInt32.end()) {
            return iter->second;
          }
        }
      }
    }

    // In all other cases the operand cannot be equal. The cases are:
    // 1. The operand is outside the range of int32_t
    // 2. Operand is a string value, this should not happen because the query should fail before reaching this point   
    return std::make_shared<MamaJenniesBitmap>();
  }

  std::shared_ptr<MamaJenniesBitmap> Filter(const Constraint& constraint) override {
    switch (constraint.op) {
      case jonoondb_api::IndexConstraintOperator::EQUAL:
        return GetBitmapEQ(constraint);
      case jonoondb_api::IndexConstraintOperator::LESS_THAN:
        break;
      case jonoondb_api::IndexConstraintOperator::LESS_THAN_EQUAL:
        break;
      case jonoondb_api::IndexConstraintOperator::GREATER_THAN:
        break;
      case jonoondb_api::IndexConstraintOperator::GREATER_THAN_EQUAL:
        break;
      case jonoondb_api::IndexConstraintOperator::MATCH:
        break;
      default:
        std::ostringstream ss;
        ss << "IndexConstraintOperator type " << static_cast<std::int32_t>(constraint.op) << " is not valid.";
        throw JonoonDBException(ss.str(), __FILE__, "", __LINE__);
    }
  }

private:
  IndexStat m_indexStat;
  std::vector<std::string> m_fieldNameTokens;
  std::map<std::int32_t, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmapsInt32;
};
}  // namespace jonoondb_api
