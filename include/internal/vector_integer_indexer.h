#pragma once

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
class VectorIntegerIndexer final : public Indexer {
public:
  VectorIntegerIndexer(const IndexInfoImpl& indexInfo,
                       const FieldType& fieldType) {
    // TODO: Add index name in the error message as well
    std::string errorMsg;
    if (indexInfo.GetIndexName().size() == 0) {
      errorMsg = "Argument indexInfo has empty name.";
    } else if (indexInfo.GetColumnName().size() == 0) {
      errorMsg = "Argument indexInfo has empty column name.";
    } else if (indexInfo.GetType() != IndexType::VECTOR) {
      errorMsg =
        "Argument indexInfo can only have IndexType VECTOR for VectorIntegerIndexer.";
    } else if (!IsValidFieldType(fieldType)) {
      std::ostringstream ss;
      ss << "Argument fieldType " << GetFieldString(fieldType)
        << " is not valid for VectorIntegerIndexer.";
      errorMsg = ss.str();
    }

    if (errorMsg.length() > 0) {
      throw InvalidArgumentException(errorMsg, __FILE__, __func__, __LINE__);
    }

    m_fieldNameTokens = StringUtils::Split(indexInfo.GetColumnName(), ".");
    m_indexStat = IndexStat(indexInfo, fieldType);
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

  std::shared_ptr<MamaJenniesBitmap> Filter(const Constraint& constraint) override {
    switch (constraint.op) {
      case jonoondb_api::IndexConstraintOperator::EQUAL:
        return GetBitmapEQ(constraint);
      case jonoondb_api::IndexConstraintOperator::LESS_THAN:
        return GetBitmapLT(constraint);
      case jonoondb_api::IndexConstraintOperator::LESS_THAN_EQUAL:
        return GetBitmapLTE(constraint);
      case jonoondb_api::IndexConstraintOperator::GREATER_THAN:
        return GetBitmapGT(constraint);
      case jonoondb_api::IndexConstraintOperator::GREATER_THAN_EQUAL:
        return GetBitmapGTE(constraint);
      case jonoondb_api::IndexConstraintOperator::MATCH:
        // TODO: Handle this
      default:
        std::ostringstream ss;
        ss << "IndexConstraintOperator type " << static_cast<std::int32_t>(constraint.op) << " is not valid.";
        throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
    }
  }

  bool TryGetIntegerValue(std::uint64_t documentID, std::int64_t& val) override {
    if (documentID < m_dataVector.size()) {
      val = m_dataVector[documentID];
      return true;
    }

    return false;
  }

private:
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
          << " is not valid for VectorIntegerIndexer.";
        throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
      }
    }

    assert(m_dataVector.size() == documentID);
    m_dataVector.push_back(val);
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapEQ(const Constraint& constraint) {
    auto bitmap = std::make_shared<MamaJenniesBitmap>();
    if (constraint.operandType == OperandType::INTEGER) {
      for (size_t i = 0; i < m_dataVector.size(); i++) {
        if (m_dataVector[i] == constraint.operand.int64Val) {
          bitmap->Add(i);
        }
      }
    } else if (constraint.operandType == OperandType::DOUBLE) {
      // Check if double has no fractional part. If it has fractional part
      // then it can't be equal to any integer
      std::int64_t intVal = static_cast<std::int64_t>(constraint.operand.doubleVal);
      if (constraint.operand.doubleVal == intVal) {
        for (size_t i = 0; i < m_dataVector.size(); i++) {
          if (m_dataVector[i] == intVal) {
            bitmap->Add(i);
          }
        }
      }
    }

    // In all other cases the operand cannot be equal. The cases are:
    // Operand is a string value, this should not happen because the query should fail before reaching this point   
    return bitmap;
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapLT(const Constraint& constraint) {
    auto bitmap = std::make_shared<MamaJenniesBitmap>();
    int64_t ceiling;
    if (constraint.operandType == OperandType::DOUBLE) {
      ceiling = static_cast<int64_t>(std::ceil(constraint.operand.doubleVal));
    } else {
      ceiling = constraint.operand.int64Val;
    }

    for (size_t i = 0; i < m_dataVector.size(); i++) {
      if (m_dataVector[i] < ceiling) {
        bitmap->Add(i);
      }
    }

    return bitmap;
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapLTE(const Constraint& constraint) {
    auto bitmap = std::make_shared<MamaJenniesBitmap>();
    int64_t ceiling;
    if (constraint.operandType == OperandType::DOUBLE) {
      ceiling = static_cast<int64_t>(std::ceil(constraint.operand.doubleVal));
    } else {
      ceiling = constraint.operand.int64Val;
    }

    for (size_t i = 0; i < m_dataVector.size(); i++) {
      if (m_dataVector[i] <= ceiling) {
        bitmap->Add(i);
      }
    }

    return bitmap;
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapGT(const Constraint& constraint) {
    auto bitmap = std::make_shared<MamaJenniesBitmap>();
    int64_t operandVal;
    if (constraint.operandType == OperandType::DOUBLE) {
      operandVal = static_cast<int64_t>(std::ceil(constraint.operand.doubleVal));
    } else {
      operandVal = constraint.operand.int64Val;
    }

    for (size_t i = 0; i < m_dataVector.size(); i++) {
      if (m_dataVector[i] > operandVal) {
        bitmap->Add(i);
      }
    }

    return bitmap;
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapGTE(const Constraint& constraint) {
    auto bitmap = std::make_shared<MamaJenniesBitmap>();
    int64_t operandVal;
    if (constraint.operandType == OperandType::DOUBLE) {
      operandVal = static_cast<int64_t>(std::ceil(constraint.operand.doubleVal));
    } else {
      operandVal = constraint.operand.int64Val;
    }

    for (size_t i = 0; i < m_dataVector.size(); i++) {
      if (m_dataVector[i] >= operandVal) {
        bitmap->Add(i);
      }
    }

    return bitmap;
  }

  IndexStat m_indexStat;
  std::vector<std::string> m_fieldNameTokens;
  std::vector<std::int64_t> m_dataVector;
};
} // namespace jonoondb_api
