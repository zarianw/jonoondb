#pragma once

#include <memory>
#include <cstdint>
#include <sstream>
#include <vector>
#include <string>
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
class VectorDoubleIndexer final : public Indexer {
public:
  VectorDoubleIndexer(const IndexInfoImpl& indexInfo,
                       const FieldType& fieldType) {
    // TODO: Add index name in the error message as well
    std::string errorMsg;
    if (indexInfo.GetIndexName().size() == 0) {
      errorMsg = "Argument indexInfo has empty name.";
    } else if (indexInfo.GetColumnName().size() == 0) {
      errorMsg = "Argument indexInfo has empty column name.";
    } else if (indexInfo.GetType() != IndexType::VECTOR) {
      errorMsg =
        "Argument indexInfo can only have IndexType VECTOR for VectorDoubleIndexer.";
    } else if (!IsValidFieldType(fieldType)) {
      std::ostringstream ss;
      ss << "Argument fieldType " << GetFieldString(fieldType)
        << " is not valid for VectorDoubleIndexer.";
      errorMsg = ss.str();
    }

    if (errorMsg.length() > 0) {
      throw InvalidArgumentException(errorMsg, __FILE__, __func__, __LINE__);
    }

    m_fieldNameTokens = StringUtils::Split(indexInfo.GetColumnName(), ".");
    m_indexStat = IndexStat(indexInfo, fieldType);
  }

  static bool IsValidFieldType(FieldType fieldType) {
    return (fieldType == FieldType::BASE_TYPE_FLOAT32
            || fieldType == FieldType::BASE_TYPE_DOUBLE);
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

  bool TryGetDoubleValue(std::uint64_t documentID, double& val) override {
    if (documentID < m_dataVector.size()) {
      val = m_dataVector[documentID];
      return true;
    }

    return false;
  }

private:
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
          << " is not valid for VectorDoubleIndexer.";
        throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
      }
    }

    assert(m_dataVector.size() == documentID);
    m_dataVector.push_back(val);
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

  std::shared_ptr<MamaJenniesBitmap> GetBitmapEQ(const Constraint& constraint) {
    auto bitmap = std::make_shared<MamaJenniesBitmap>();
    double val = GetOperandVal(constraint);

    for (size_t i = 0; i < m_dataVector.size(); i++) {
      if (m_dataVector[i] == val) {
        bitmap->Add(i);
      }
    }

    // In all other cases the operand cannot be equal. The cases are:
    // Operand is a string value, this should not happen because the query should fail before reaching this point   
    return bitmap;
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapLT(const Constraint& constraint) {
    auto bitmap = std::make_shared<MamaJenniesBitmap>();
    double val = GetOperandVal(constraint);

    for (size_t i = 0; i < m_dataVector.size(); i++) {
      if (m_dataVector[i] < val) {
        bitmap->Add(i);
      }
    }

    return bitmap;
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapLTE(const Constraint& constraint) {
    auto bitmap = std::make_shared<MamaJenniesBitmap>();
    double val = GetOperandVal(constraint);

    for (size_t i = 0; i < m_dataVector.size(); i++) {
      if (m_dataVector[i] <= val) {
        bitmap->Add(i);
      }
    }

    return bitmap;
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapGT(const Constraint& constraint) {
    auto bitmap = std::make_shared<MamaJenniesBitmap>();
    double val = GetOperandVal(constraint);

    for (size_t i = 0; i < m_dataVector.size(); i++) {
      if (m_dataVector[i] > val) {
        bitmap->Add(i);
      }
    }

    return bitmap;
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapGTE(const Constraint& constraint) {
    auto bitmap = std::make_shared<MamaJenniesBitmap>();
    double val = GetOperandVal(constraint);

    for (size_t i = 0; i < m_dataVector.size(); i++) {
      if (m_dataVector[i] >= val) {
        bitmap->Add(i);
      }
    }

    return bitmap;
  }

  IndexStat m_indexStat;
  std::vector<std::string> m_fieldNameTokens;
  std::vector<double> m_dataVector;
};
} // namespace jonoondb_api
