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

class EWAHCompressedBitmapIndexerInteger final : public Indexer {
public:
  static void Construct(const IndexInfoImpl& indexInfo,
    const FieldType& fieldType,
    EWAHCompressedBitmapIndexerInteger*& obj) {
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
        << " is not valid for EWAHCompressedBitmapIndexerInteger.";
      errorMsg = ss.str();
    }

    if (errorMsg.length() > 0) {
      throw InvalidArgumentException(errorMsg, __FILE__, "", __LINE__);
    }

    std::vector<std::string> tokens = StringUtils::Split(indexInfo.GetColumnName(),
      ".");
    IndexStat indexStat(indexInfo, fieldType);
    obj = new EWAHCompressedBitmapIndexerInteger(indexStat, tokens);
  }

  ~EWAHCompressedBitmapIndexerInteger() override {
  }

  static bool IsValidFieldType(FieldType fieldType) {
    return (fieldType == FieldType::BASE_TYPE_INT8
      || fieldType == FieldType::BASE_TYPE_INT16
      || fieldType == FieldType::BASE_TYPE_INT32
      || fieldType == FieldType::BASE_TYPE_INT64
      || fieldType == FieldType::BASE_TYPE_UINT8
      || fieldType == FieldType::BASE_TYPE_UINT16
      || fieldType == FieldType::BASE_TYPE_UINT32
      || fieldType == FieldType::BASE_TYPE_UINT64);
  }

  void ValidateForInsert(const Document& document) override {
    if (m_fieldNameTokens.size() > 1) {
      auto subDoc = DocumentUtils::GetSubDocumentRecursively(document, m_fieldNameTokens);
      subDoc->VerifyFieldForRead(m_fieldNameTokens.back().c_str(),
        m_indexStat.GetFieldType());
    } else {
      document.VerifyFieldForRead(m_fieldNameTokens.back().c_str(),
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
    switch (constraint.op) {
      case jonoondb_api::IndexConstraintOperator::EQUAL:
        return GetBitmapEQ(constraint);
      case jonoondb_api::IndexConstraintOperator::LESS_THAN:
        return GetBitmapLT(constraint, false);        
      case jonoondb_api::IndexConstraintOperator::LESS_THAN_EQUAL:
        return GetBitmapLT(constraint, true);        
      case jonoondb_api::IndexConstraintOperator::GREATER_THAN:
        return GetBitmapGT(constraint);        
      case jonoondb_api::IndexConstraintOperator::GREATER_THAN_EQUAL:
        return GetBitmapGTE(constraint);
      case jonoondb_api::IndexConstraintOperator::MATCH:
        break;
      default:
        std::ostringstream ss;
        ss << "IndexConstraintOperator type " << static_cast<std::int32_t>(constraint.op) << " is not valid.";
        throw JonoonDBException(ss.str(), __FILE__, "", __LINE__);
    }
  }

private:
  EWAHCompressedBitmapIndexerInteger(const IndexStat& indexStat,
    std::vector<std::string>& fieldNameTokens)
    : m_indexStat(indexStat),
    m_fieldNameTokens(fieldNameTokens) {
  }

  void InsertInternal(std::uint64_t documentID, const Document& document) {
    int64_t val;
    switch (m_indexStat.GetFieldType()) {
      case FieldType::BASE_TYPE_UINT8: {
        val = document.GetScalarValueAsUInt8(m_fieldNameTokens.back());
        break;
      }
      case FieldType::BASE_TYPE_UINT16: {
        val = document.GetScalarValueAsUInt16(m_fieldNameTokens.back());
        break;
      }
      case FieldType::BASE_TYPE_UINT32: {
        val = document.GetScalarValueAsUInt32(m_fieldNameTokens.back());
        break;
      }
      case FieldType::BASE_TYPE_UINT64: {
        val = document.GetScalarValueAsUInt64(m_fieldNameTokens.back());
        break;
      }
      case FieldType::BASE_TYPE_INT8: {
        val = document.GetScalarValueAsInt8(m_fieldNameTokens.back());
        break;
      }
      case FieldType::BASE_TYPE_INT16: {
        val = document.GetScalarValueAsInt16(m_fieldNameTokens.back());
        break;
      }
      case FieldType::BASE_TYPE_INT32: {
        val = document.GetScalarValueAsInt32(m_fieldNameTokens.back());
        break;
      }
      case FieldType::BASE_TYPE_INT64: {
        val = document.GetScalarValueAsInt64(m_fieldNameTokens.back());
        break;
      }
      default: {
        // This can never happen
        std::ostringstream ss;
        ss << "FieldType " << GetFieldString(m_indexStat.GetFieldType())
          << " is not valid for EWAHCompressedBitmapIndexerInteger.";
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
      std::int64_t intVal = static_cast<std::int64_t>(constraint.operand.doubleVal);
      if (constraint.operand.doubleVal == intVal) {
        auto iter = m_compressedBitmaps.find(intVal);
        if (iter != m_compressedBitmaps.end()) {
          bitmaps.push_back(iter->second);
        }
      }
    }

    // In all other cases the operand cannot be equal. The cases are:
    // Operand is a string value, this should not happen because the query should fail before reaching this point   
    return bitmaps;
  }

  std::vector<std::shared_ptr<MamaJenniesBitmap>> GetBitmapLT(const Constraint& constraint, bool orEqual) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    int64_t ceiling;
    if (constraint.operandType == OperandType::DOUBLE) {
      ceiling = std::ceil(constraint.operand.doubleVal);
    }

    if (constraint.operandType == OperandType::INTEGER) {
      for (auto& item : m_compressedBitmaps) {
        if (item.first < constraint.operand.int64Val) {
          bitmaps.push_back(item.second);
        } else {
          if (orEqual && item.first == constraint.operand.int64Val) {
            bitmaps.push_back(item.second);
          }
          break;
        }
      }
    } else if (constraint.operandType == OperandType::DOUBLE) {
      for (auto& item : m_compressedBitmaps) {
        if (item.first < ceiling) {
          bitmaps.push_back(item.second);
        } else {
          if (orEqual && constraint.operand.doubleVal == (double)item.first) {
            bitmaps.push_back(item.second);
          }
          break;
        }
      }
    }

    return bitmaps;
  }

  std::vector<std::shared_ptr<MamaJenniesBitmap>> GetBitmapGT(const Constraint& constraint) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    int64_t operandVal;
    if (constraint.operandType == OperandType::DOUBLE) {
      operandVal = std::floor(constraint.operand.doubleVal);
    } else {
      operandVal = constraint.operand.int64Val;
    }

    auto iter = m_compressedBitmaps.upper_bound(operandVal);    
    while (iter != m_compressedBitmaps.end()) {
      bitmaps.push_back(iter->second);
      iter++;
    }
    
    return bitmaps;
  }

  std::vector<std::shared_ptr<MamaJenniesBitmap>> GetBitmapGTE(const Constraint& constraint) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    int64_t operandVal;
    if (constraint.operandType == OperandType::DOUBLE) {
      operandVal = std::ceil(constraint.operand.doubleVal);
    } else {
      operandVal = constraint.operand.int64Val;
    }

    auto iter = m_compressedBitmaps.lower_bound(operandVal);
    while (iter != m_compressedBitmaps.end()) {
      bitmaps.push_back(iter->second);
      iter++;
    }

    return bitmaps;
  }

  IndexStat m_indexStat;
  std::vector<std::string> m_fieldNameTokens;
  std::map<std::int64_t, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmaps;
};
}  // namespace jonoondb_api
