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
#include "jonoondb_api/null_helpers.h"

namespace jonoondb_api {

class EWAHCompressedBitmapIndexerString final: public Indexer {
 public:
  static void Construct(const IndexInfoImpl& indexInfo,
                        const FieldType& fieldType,
                        EWAHCompressedBitmapIndexerString*& obj) {
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
          << " is not valid for EWAHCompressedBitmapIndexerString.";
      errorMsg = ss.str();
    }

    if (errorMsg.length() > 0) {
      throw InvalidArgumentException(errorMsg, __FILE__, __func__, __LINE__);
    }

    std::vector<std::string>
        tokens = StringUtils::Split(indexInfo.GetColumnName(),
                                    ".");
    IndexStat indexStat(indexInfo, fieldType);
    obj = new EWAHCompressedBitmapIndexerString(indexStat, tokens);
  }

  ~EWAHCompressedBitmapIndexerString() override {
  }

  static bool IsValidFieldType(FieldType fieldType) {
    return (fieldType == FieldType::BASE_TYPE_STRING);
  }

  void Insert(std::uint64_t documentID, const Document& document) override {
    auto val = DocumentUtils::GetStringValue(document,
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
    if (lowerConstraint.operandType == OperandType::STRING &&
        upperConstraint.operandType == OperandType::STRING) {
      std::map<std::string, std::shared_ptr<MamaJenniesBitmap>>::const_iterator
          startIter, endIter;

      if (lowerConstraint.op == IndexConstraintOperator::GREATER_THAN_EQUAL) {
        startIter = m_compressedBitmaps.lower_bound(lowerConstraint.strVal);
      } else {
        startIter = m_compressedBitmaps.upper_bound(lowerConstraint.strVal);
      }

      bool nullChkRequired = true;
      while (startIter != m_compressedBitmaps.end()) {
        if (nullChkRequired) {
          if (NullHelpers::IsNull(startIter->first)) {
            continue;
          }

          if (!NullHelpers::ContainsJustNullChars(startIter->first) &&
              !NullHelpers::IsNull(startIter->first)) {
            nullChkRequired = false;
          }
        }

        if (startIter->first < upperConstraint.strVal) {
          bitmaps.push_back(startIter->second);
        } else if (
            upperConstraint.op == IndexConstraintOperator::LESS_THAN_EQUAL
                && startIter->first == upperConstraint.strVal) {
          bitmaps.push_back(startIter->second);
        } else {
          break;
        }

        startIter++;
      }
    }

    return MamaJenniesBitmap::LogicalOR(bitmaps);
  }

 private:
  EWAHCompressedBitmapIndexerString(const IndexStat& indexStat,
                                    std::vector<std::string>& fieldNameTokens)
      : m_indexStat(indexStat),
        m_fieldNameTokens(fieldNameTokens) {
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapEQ(const Constraint& constraint) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    if (constraint.operandType == OperandType::STRING) {
      auto iter = m_compressedBitmaps.find(constraint.strVal);
      if (iter != m_compressedBitmaps.end() &&
          !NullHelpers::IsNull(iter->first)) {
        bitmaps.push_back(iter->second);
      }
    }

    return MamaJenniesBitmap::LogicalOR(bitmaps);
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapLT(const Constraint& constraint,
                                                 bool orEqual) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    bool nullChkRequired = true;
    if (constraint.operandType == OperandType::STRING) {
      for (auto& item : m_compressedBitmaps) {
        if (nullChkRequired) {
          if (NullHelpers::IsNull(item.first)) {
            continue;
          }

          if (!NullHelpers::ContainsJustNullChars(item.first) &&
              !NullHelpers::IsNull(item.first)) {
            nullChkRequired = false;
          }
        }

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

    return MamaJenniesBitmap::LogicalOR(bitmaps);
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapGT(const Constraint& constraint,
                                                 bool orEqual) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    if (constraint.operandType == OperandType::STRING) {
      std::map<std::string, std::shared_ptr<MamaJenniesBitmap>>::const_iterator
          iter;
      if (orEqual) {
        iter = m_compressedBitmaps.lower_bound(constraint.strVal);
      } else {
        iter = m_compressedBitmaps.upper_bound(constraint.strVal);
      }

      bool nullChkRequired = true;
      while (iter != m_compressedBitmaps.end()) {
        if (nullChkRequired) {
          if (NullHelpers::IsNull(iter->first)) {
            continue;
          }

          if (!NullHelpers::ContainsJustNullChars(iter->first) &&
              !NullHelpers::IsNull(iter->first)) {
            nullChkRequired = false;
          }
        }

        bitmaps.push_back(iter->second);
        iter++;
      }
    }

    return MamaJenniesBitmap::LogicalOR(bitmaps);
  }

  IndexStat m_indexStat;
  std::vector<std::string> m_fieldNameTokens;
  std::map<std::string, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmaps;
  std::unique_ptr<Document> m_subDoc;
};
}  // namespace jonoondb_api
