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
#include "status.h"
#include "string_utils.h"
#include "document.h"
#include "mama_jennies_bitmap.h"
#include "exception_utils.h"
#include "index_stat.h"
#include "constraint.h"
#include "enums.h"

namespace jonoondb_api {

class EWAHCompressedBitmapIndexerDouble final : public Indexer {
public:
  static void Construct(const IndexInfoImpl& indexInfo,
    const FieldType& fieldType,
    EWAHCompressedBitmapIndexerDouble*& obj) {
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
        << " is not valid for EWAHCompressedBitmapIndexerDouble.";
      errorMsg = ss.str();
    }

    if (errorMsg.length() > 0) {
      throw InvalidArgumentException(errorMsg, __FILE__, "", __LINE__);
    }

    std::vector<std::string> tokens = StringUtils::Split(indexInfo.GetColumnName(),
      ".");
    IndexStat indexStat(indexInfo, fieldType);
    obj = new EWAHCompressedBitmapIndexerDouble(indexStat, tokens);
  }

  ~EWAHCompressedBitmapIndexerDouble() override {
  }

  static bool IsValidFieldType(FieldType fieldType) {
    return (fieldType == FieldType::BASE_TYPE_FLOAT32
      || fieldType == FieldType::BASE_TYPE_DOUBLE);
  }

  void ValidateForInsert(const Document& document) override {
    if (m_fieldNameTokens.size() > 1) {
      auto subDoc = GetSubDocumentRecursively(document);
      subDoc->VerifyFieldForRead(m_fieldNameTokens.back().c_str(),
        m_indexStat.GetFieldType());
    } else {
      document.VerifyFieldForRead(m_fieldNameTokens.back().c_str(),
        m_indexStat.GetFieldType());
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
  EWAHCompressedBitmapIndexerDouble(const IndexStat& indexStat,
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
    double val;
    switch (m_indexStat.GetFieldType()) {
      case FieldType::BASE_TYPE_FLOAT32: {
        val = document.GetScalarValueAsFloat(m_fieldNameTokens.back());
        break;
      }
      case FieldType::BASE_TYPE_DOUBLE: {
        val = document.GetScalarValueAsDouble(m_fieldNameTokens.back());
        break;
      }      
      default: {
        // This can never happen
        std::ostringstream ss;
        ss << "FieldType " << GetFieldString(m_indexStat.GetFieldType())
          << " is not valid for EWAHCompressedBitmapIndexerDouble.";
        throw JonoonDBException(ss.str(), __FILE__, "", __LINE__);
      }
    }

    auto compressedBitmap = m_compressedBitmaps.find(val);
    if (compressedBitmap == m_compressedBitmaps.end()) {
      auto bm = shared_ptr < MamaJenniesBitmap >(new MamaJenniesBitmap());
      bm->Add(documentID);
      m_compressedBitmaps[val] = bm;
    } else {
      compressedBitmap->second->Add(documentID);
    }
  }

  std::vector<std::shared_ptr<MamaJenniesBitmap>> GetBitmapEQ(const Constraint& constraint) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    if (constraint.operandType == OperandType::INTEGER) {
      auto iter = m_compressedBitmaps.find(constraint.operand.int64Val);
      if (iter != m_compressedBitmaps.end()) {
        bitmaps.push_back(iter->second);
      }
    } else if (constraint.operandType == OperandType::DOUBLE) {
      // Check if double has no fractional part      
      auto iter = m_compressedBitmaps.find(constraint.operand.doubleVal);
      if (iter != m_compressedBitmaps.end()) {
        bitmaps.push_back(iter->second);
      }
    }

    // In all other cases the operand cannot be equal. The cases are:
    // Operand is a string value, this should not happen because the query should fail before reaching this point   
    return bitmaps;
  }

  std::vector<std::shared_ptr<MamaJenniesBitmap>> GetBitmapLT(const Constraint& constraint) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;    
    
    if (constraint.operandType == OperandType::INTEGER) {
      double dVal = constraint.operand.int64Val;
      for (auto& item : m_compressedBitmaps) {
        if (item.first < dVal) {
          bitmaps.push_back(item.second);
        } else {
          break;
        }
      }      
    } else if (constraint.operandType == OperandType::DOUBLE) {
      for (auto& item : m_compressedBitmaps) {        
        if (item.first < constraint.operand.doubleVal) {
          bitmaps.push_back(item.second);
        } else {
          break;
        }
      }
    }

    // In all other cases the operand cannot be equal. The cases are:
    // Operand is a string value, this should not happen because the query should fail before reaching this point   
    return bitmaps;
  }

  std::vector<std::shared_ptr<MamaJenniesBitmap>> Filter(const Constraint& constraint) override {
    switch (constraint.op) {
      case jonoondb_api::IndexConstraintOperator::EQUAL:
        return GetBitmapEQ(constraint);
      case jonoondb_api::IndexConstraintOperator::LESS_THAN:
        return GetBitmapLT(constraint);
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
  // Todo: We are assuming that double will be 8 bytes (which should be the case mostly),
  // but that is not gauranteed. Change the code to handle this properly
  std::map<double, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmaps;
};
}  // namespace jonoondb_api
