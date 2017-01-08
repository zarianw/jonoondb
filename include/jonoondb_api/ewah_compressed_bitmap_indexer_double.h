#pragma once

#include <map>
#include <memory>
#include <cstdint>
#include <sstream>
#include <vector>
#include <string>
#include <cmath>
#include "jonoondb_api/indexer.h"
#include "jonoondb_api/index_info_impl.h"
#include "jonoondb_api/string_utils.h"
#include "jonoondb_api/document.h"
#include "jonoondb_api/mama_jennies_bitmap.h"
#include "jonoondb_api/exception_utils.h"
#include "jonoondb_api/index_stat.h"
#include "jonoondb_api/constraint.h"
#include "jonoondb_api/enums.h"
#include "jonoondb_api/jonoondb_exceptions.h"

namespace jonoondb_api {

class EWAHCompressedBitmapIndexerDouble final: public Indexer {
 public:
  static void Construct(const IndexInfoImpl& indexInfo,
                        const FieldType& fieldType,
                        EWAHCompressedBitmapIndexerDouble*& obj) {    
    std::string errorMsg;
    if (indexInfo.GetIndexName().size() == 0) {
      errorMsg = "Argument indexInfo has empty name.";
    } else if (indexInfo.GetColumnName().size() == 0) {
      errorMsg = "Argument indexInfo has empty column name.";
    } else if (indexInfo.GetType() != IndexType::INVERTED_COMPRESSED_BITMAP) {
      errorMsg =
          "Argument indexInfo can only have IndexType INVERTED_COMPRESSED_BITMAP for EWAHCompressedBitmapIndexer.";
    } else if (!IsValidFieldType(fieldType)) {
      std::ostringstream ss;
      ss << "Argument fieldType " << GetFieldString(fieldType)
          << " is not valid for EWAHCompressedBitmapIndexerDouble.";
      errorMsg = ss.str();
    }

    if (errorMsg.length() > 0) {
      throw InvalidArgumentException(errorMsg, __FILE__, __func__, __LINE__);
    }

    std::vector<std::string>
        tokens = StringUtils::Split(indexInfo.GetColumnName(), ".");
    IndexStat indexStat(indexInfo, fieldType);
    obj = new EWAHCompressedBitmapIndexerDouble(indexStat, tokens);
  }

  ~EWAHCompressedBitmapIndexerDouble() override {
  }

  static bool IsValidFieldType(FieldType fieldType) {
    return (fieldType == FieldType::FLOAT
        || fieldType == FieldType::DOUBLE);
  }

  void Insert(std::uint64_t documentID, const Document& document) override {
    auto val = DocumentUtils::GetFloatValue(document,
                                            m_subDoc,
                                            m_fieldNameTokens);
    auto compressedBitmap = m_compressedBitmaps.find(val);
    if (compressedBitmap == m_compressedBitmaps.end()) {
      auto bm = shared_ptr<MamaJenniesBitmap>(new MamaJenniesBitmap());
      bm->Add(documentID);
      m_compressedBitmaps[val] = bm;
    } else {
      compressedBitmap->second->Add(documentID);
    }
  }

  const IndexStat& GetIndexStats() override {
    return m_indexStat;
  }

  std::shared_ptr<MamaJenniesBitmap> Filter(const Constraint& constraint) override {
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
        // TODO: Handle this
      default:
        std::ostringstream ss;
        ss << "IndexConstraintOperator type "
            << static_cast<std::int32_t>(constraint.op) << " is not valid.";
        throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
    }
  }

  std::shared_ptr<MamaJenniesBitmap> FilterRange(
      const Constraint& lowerConstraint,
      const Constraint& upperConstraint) override {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    double lowerVal = GetOperandVal(lowerConstraint);
    double upperVal = GetOperandVal(upperConstraint);
    std::map<double, std::shared_ptr<MamaJenniesBitmap>>::const_iterator
        startIter, endIter;

    if (lowerConstraint.op == IndexConstraintOperator::GREATER_THAN_EQUAL) {
      startIter = m_compressedBitmaps.lower_bound(lowerVal);
    } else {
      startIter = m_compressedBitmaps.upper_bound(lowerVal);
    }

    while (startIter != m_compressedBitmaps.end()) {
      if (startIter->first < upperVal) {
        bitmaps.push_back(startIter->second);
      } else if (upperConstraint.op == IndexConstraintOperator::LESS_THAN_EQUAL
          && startIter->first == upperVal) {
        bitmaps.push_back(startIter->second);
      } else {
        break;
      }

      startIter++;
    }

    return MamaJenniesBitmap::LogicalOR(bitmaps);
  }

 private:
  EWAHCompressedBitmapIndexerDouble(const IndexStat& indexStat,
                                    std::vector<std::string>& fieldNameTokens)
      : m_indexStat(indexStat),
        m_fieldNameTokens(fieldNameTokens) {
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapEQ(const Constraint& constraint) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    if (constraint.operandType == OperandType::INTEGER) {
      auto iter =
          m_compressedBitmaps.find(static_cast<double>(constraint.operand.int64Val));
      if (iter != m_compressedBitmaps.end()) {
        bitmaps.push_back(iter->second);
      }
    } else if (constraint.operandType == OperandType::DOUBLE) {
      auto iter = m_compressedBitmaps.find(constraint.operand.doubleVal);
      if (iter != m_compressedBitmaps.end()) {
        bitmaps.push_back(iter->second);
      }
    }

    // In all other cases the operand cannot be equal. The cases are:
    // Operand is a string value, this should not happen because the query should fail before reaching this point   
    return MamaJenniesBitmap::LogicalOR(bitmaps);
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapLT(const Constraint& constraint,
                                                 bool orEqual) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;

    if (constraint.operandType == OperandType::INTEGER) {
      double dVal = static_cast<double>(constraint.operand.int64Val);
      for (auto& item : m_compressedBitmaps) {
        if (item.first < dVal) {
          bitmaps.push_back(item.second);
        } else {
          if (orEqual && item.first == dVal) {
            bitmaps.push_back(item.second);
          }
          break;
        }
      }
    } else if (constraint.operandType == OperandType::DOUBLE) {
      for (auto& item : m_compressedBitmaps) {
        if (item.first < constraint.operand.doubleVal) {
          bitmaps.push_back(item.second);
        } else {
          if (orEqual && item.first == constraint.operand.doubleVal) {
            bitmaps.push_back(item.second);
          }
          break;
        }
      }
    }

    // In all other cases the operand cannot be equal. The cases are:
    // Operand is a string value, this should not happen because the query should fail before reaching this point   
    return MamaJenniesBitmap::LogicalOR(bitmaps);
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapGT(const Constraint& constraint) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    double operandVal;
    if (constraint.operandType == OperandType::DOUBLE) {
      operandVal = constraint.operand.doubleVal;
    } else {
      operandVal = static_cast<double>(constraint.operand.int64Val);
    }

    auto iter = m_compressedBitmaps.upper_bound(operandVal);
    while (iter != m_compressedBitmaps.end()) {
      bitmaps.push_back(iter->second);
      iter++;
    }

    return MamaJenniesBitmap::LogicalOR(bitmaps);
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapGTE(const Constraint& constraint) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    double operandVal;
    if (constraint.operandType == OperandType::DOUBLE) {
      operandVal = constraint.operand.doubleVal;
    } else {
      operandVal = static_cast<double>(constraint.operand.int64Val);
    }

    auto iter = m_compressedBitmaps.lower_bound(operandVal);
    while (iter != m_compressedBitmaps.end()) {
      bitmaps.push_back(iter->second);
      iter++;
    }

    return MamaJenniesBitmap::LogicalOR(bitmaps);
  }

  inline double GetOperandVal(const Constraint& constraint) {
    double val = 0;
    if (constraint.operandType == OperandType::INTEGER) {
      val = static_cast<double>(constraint.operand.int64Val);
    } else if (constraint.operandType == OperandType::DOUBLE) {
      val = constraint.operand.doubleVal;
    } 

    // Todo: See if we should throw exception in case of string operand
    return val;
  }

  IndexStat m_indexStat;
  std::vector<std::string> m_fieldNameTokens;
  // Todo: We are assuming that double will be 8 bytes (which should be the case mostly),
  // but that is not gauranteed. Change the code to handle this properly
  std::map<double, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmaps;
  std::unique_ptr<Document> m_subDoc;
};
}  // namespace jonoondb_api
