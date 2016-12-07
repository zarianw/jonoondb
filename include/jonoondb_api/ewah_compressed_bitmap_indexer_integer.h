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
#include "string_utils.h"
#include "document.h"
#include "mama_jennies_bitmap.h"
#include "exception_utils.h"
#include "index_stat.h"
#include "constraint.h"
#include "enums.h"

namespace jonoondb_api {

class EWAHCompressedBitmapIndexerInteger final: public Indexer {
 public:
  static void Construct(const IndexInfoImpl& indexInfo,
                        const FieldType& fieldType,
                        EWAHCompressedBitmapIndexerInteger*& obj) {
    // TODO: Add index name in the error message as well
    std::string errorMsg;
    if (indexInfo.GetIndexName().size() == 0) {
      errorMsg = "Argument indexInfo has empty name.";
    } else if (indexInfo.GetColumnName().size() == 0) {
      errorMsg = "Argument indexInfo has empty column name.";
    } else if (indexInfo.GetType() != IndexType::EWAH_COMPRESSED_BITMAP) {
      errorMsg =
          "Argument indexInfo can only have IndexType EWAH_COMPRESSED_BITMAP for EWAHCompressedBitmapIndexer.";
    } else if (!IsValidFieldType(fieldType)) {
      std::ostringstream ss;
      ss << "Argument fieldType " << GetFieldString(fieldType)
          << " is not valid for EWAHCompressedBitmapIndexerInteger.";
      errorMsg = ss.str();
    }

    if (errorMsg.length() > 0) {
      throw InvalidArgumentException(errorMsg, __FILE__, __func__, __LINE__);
    }

    std::vector<std::string>
        tokens = StringUtils::Split(indexInfo.GetColumnName(),
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
        || fieldType == FieldType::BASE_TYPE_INT64);
  }

  void Insert(std::uint64_t documentID, const Document& document) override {
    if (m_fieldNameTokens.size() > 1) {
      auto subDoc =
          DocumentUtils::GetSubDocumentRecursively(document, m_fieldNameTokens);
      InsertInternal(documentID, *subDoc.get());
    } else {
      InsertInternal(documentID, document);
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
    std::int64_t lowerVal, upperVal;
    if (lowerConstraint.operandType == OperandType::DOUBLE) {
      if (lowerConstraint.op == IndexConstraintOperator::GREATER_THAN) {
        lowerVal =
            static_cast<std::int64_t>(std::floor(lowerConstraint.operand.doubleVal));
      } else {
        lowerVal =
            static_cast<std::int64_t>(std::ceil(lowerConstraint.operand.doubleVal));
      }
    } else {
      lowerVal = lowerConstraint.operand.int64Val;
    }

    if (upperConstraint.operandType == OperandType::DOUBLE) {
      upperVal =
          static_cast<std::int64_t>(std::ceil(upperConstraint.operand.doubleVal));
    } else {
      upperVal = upperConstraint.operand.int64Val;
    }

    std::map<std::int64_t, std::shared_ptr<MamaJenniesBitmap>>::const_iterator
        startIter, endIter;

    if (lowerConstraint.op == IndexConstraintOperator::GREATER_THAN) {
      startIter = m_compressedBitmaps.upper_bound(lowerVal);
    } else {
      startIter = m_compressedBitmaps.lower_bound(lowerVal);
    }

    while (startIter != m_compressedBitmaps.end()) {
      if (startIter->first < upperVal) {
        bitmaps.push_back(startIter->second);
      } else if (upperConstraint.op
          == IndexConstraintOperator::LESS_THAN_EQUAL) {
        if (upperConstraint.operandType == OperandType::DOUBLE) {
          if (static_cast<double>(startIter->first)
              == upperConstraint.operand.doubleVal)
            bitmaps.push_back(startIter->second);
        } else {
          if (startIter->first == upperConstraint.operand.int64Val)
            bitmaps.push_back(startIter->second);
        }
      } else {
        break;
      }

      startIter++;
    }

    return MamaJenniesBitmap::LogicalOR(bitmaps);
  }

 private:
  EWAHCompressedBitmapIndexerInteger(const IndexStat& indexStat,
                                     std::vector<std::string>& fieldNameTokens)
      : m_indexStat(indexStat),
        m_fieldNameTokens(fieldNameTokens) {
  }

  void InsertInternal(std::uint64_t documentID, const Document& document) {
    int64_t val = document.GetIntegerValueAsInt64(m_fieldNameTokens.back());

    auto compressedBitmap = m_compressedBitmaps.find(val);
    if (compressedBitmap == m_compressedBitmaps.end()) {
      auto bm = shared_ptr<MamaJenniesBitmap>(new MamaJenniesBitmap());
      bm->Add(documentID);
      m_compressedBitmaps[val] = bm;
    } else {
      compressedBitmap->second->Add(documentID);
    }
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapEQ(const Constraint& constraint) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    if (constraint.operandType == OperandType::INTEGER) {
      auto iter = m_compressedBitmaps.find(constraint.operand.int64Val);
      if (iter != m_compressedBitmaps.end()) {
        bitmaps.push_back(iter->second);
      }
    } else if (constraint.operandType == OperandType::DOUBLE) {
      // Check if double has no fractional part
      std::int64_t
          intVal = static_cast<std::int64_t>(constraint.operand.doubleVal);
      if (constraint.operand.doubleVal == intVal) {
        auto iter = m_compressedBitmaps.find(intVal);
        if (iter != m_compressedBitmaps.end()) {
          bitmaps.push_back(iter->second);
        }
      }
    }

    // In all other cases the operand cannot be equal. The cases are:
    // Operand is a string value, this should not happen because the query should fail before reaching this point   
    return MamaJenniesBitmap::LogicalOR(bitmaps);
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapLT(const Constraint& constraint,
                                                 bool orEqual) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    int64_t ceiling;
    if (constraint.operandType == OperandType::DOUBLE) {
      ceiling = static_cast<int64_t>(std::ceil(constraint.operand.doubleVal));
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
          if (orEqual && constraint.operand.doubleVal == (double) item.first) {
            bitmaps.push_back(item.second);
          }
          break;
        }
      }
    }

    return MamaJenniesBitmap::LogicalOR(bitmaps);
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapGT(const Constraint& constraint) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    int64_t operandVal;
    if (constraint.operandType == OperandType::DOUBLE) {
      operandVal =
          static_cast<int64_t>(std::floor(constraint.operand.doubleVal));
    } else {
      operandVal = constraint.operand.int64Val;
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
    int64_t operandVal;
    if (constraint.operandType == OperandType::DOUBLE) {
      operandVal =
          static_cast<int64_t>(std::ceil(constraint.operand.doubleVal));
    } else {
      operandVal = constraint.operand.int64Val;
    }

    auto iter = m_compressedBitmaps.lower_bound(operandVal);
    while (iter != m_compressedBitmaps.end()) {
      bitmaps.push_back(iter->second);
      iter++;
    }

    return MamaJenniesBitmap::LogicalOR(bitmaps);
  }

  IndexStat m_indexStat;
  std::vector<std::string> m_fieldNameTokens;
  std::map<std::int64_t, std::shared_ptr<MamaJenniesBitmap>>
      m_compressedBitmaps;
};
}  // namespace jonoondb_api
