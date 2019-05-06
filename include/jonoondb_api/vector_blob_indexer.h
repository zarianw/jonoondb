#pragma once

#include <cstdint>
#include <sstream>
#include <string>
#include <vector>
#include "buffer_impl.h"
#include "constraint.h"
#include "document.h"
#include "enums.h"
#include "exception_utils.h"
#include "index_info_impl.h"
#include "index_stat.h"
#include "indexer.h"
#include "mama_jennies_bitmap.h"
#include "null_helpers.h"
#include "string_utils.h"

namespace jonoondb_api {
class VectorBlobIndexer final : public Indexer {
 public:
  VectorBlobIndexer(const IndexInfoImpl& indexInfo,
                    const FieldType& fieldType) {
    std::string errorMsg;
    if (indexInfo.GetIndexName().size() == 0) {
      errorMsg = "Argument indexInfo has empty name.";
    } else if (indexInfo.GetColumnName().size() == 0) {
      errorMsg = "Argument indexInfo has empty column name.";
    } else if (indexInfo.GetType() != IndexType::VECTOR) {
      errorMsg =
          "Argument indexInfo can only have IndexType VECTOR for "
          "VectorBlobIndexer.";
    } else if (!IsValidFieldType(fieldType)) {
      std::ostringstream ss;
      ss << "Argument fieldType " << GetFieldString(fieldType)
         << " is not valid for VectorBlobIndexer.";
      errorMsg = ss.str();
    }

    if (errorMsg.length() > 0) {
      throw InvalidArgumentException(errorMsg, __FILE__, __func__, __LINE__);
    }

    m_fieldNameTokens = StringUtils::Split(indexInfo.GetColumnName(), ".");
    m_indexStat = IndexStat(indexInfo, fieldType);
  }

  static bool IsValidFieldType(FieldType fieldType) {
    return (fieldType == FieldType::BLOB);
  }

  void Insert(std::uint64_t documentID, const Document& document) override {
    std::size_t size = 0;
    auto data = DocumentUtils::GetBlobValue(document, m_subDoc,
                                            m_fieldNameTokens, size);
    assert(m_dataVector.size() == documentID);
    m_dataVector.push_back(BufferImpl(data, size, size));
  }

  const IndexStat& GetIndexStats() override {
    return m_indexStat;
  }

  std::shared_ptr<MamaJenniesBitmap> Filter(
      const Constraint& constraint) override {
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
        ss << "IndexConstraintOperator type "
           << static_cast<std::int32_t>(constraint.op) << " is not valid.";
        throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
    }
  }

  std::shared_ptr<MamaJenniesBitmap> FilterRange(
      const Constraint& lowerConstraint,
      const Constraint& upperConstraint) override {
    auto bitmap = std::make_shared<MamaJenniesBitmap>();

    if (lowerConstraint.operandType == OperandType::BLOB &&
        upperConstraint.operandType == OperandType::BLOB) {
      if (lowerConstraint.op == IndexConstraintOperator::GREATER_THAN &&
          upperConstraint.op == IndexConstraintOperator::LESS_THAN) {
        for (size_t i = 0; i < m_dataVector.size(); i++) {
          if (m_dataVector[i] > lowerConstraint.blobVal &&
              m_dataVector[i] < upperConstraint.blobVal &&
              m_dataVector[i].GetLength() > 0) {
            bitmap->Add(i);
          }
        }
      } else if (lowerConstraint.op == IndexConstraintOperator::GREATER_THAN &&
                 upperConstraint.op ==
                     IndexConstraintOperator::LESS_THAN_EQUAL) {
        for (size_t i = 0; i < m_dataVector.size(); i++) {
          if (m_dataVector[i] > lowerConstraint.blobVal &&
              m_dataVector[i] <= upperConstraint.blobVal &&
              m_dataVector[i].GetLength() > 0) {
            bitmap->Add(i);
          }
        }
      } else if (lowerConstraint.op ==
                     IndexConstraintOperator::GREATER_THAN_EQUAL &&
                 upperConstraint.op == IndexConstraintOperator::LESS_THAN) {
        for (size_t i = 0; i < m_dataVector.size(); i++) {
          if (m_dataVector[i] >= lowerConstraint.blobVal &&
              m_dataVector[i] < upperConstraint.blobVal &&
              m_dataVector[i].GetLength() > 0) {
            bitmap->Add(i);
          }
        }
      } else if (lowerConstraint.op ==
                     IndexConstraintOperator::GREATER_THAN_EQUAL &&
                 upperConstraint.op ==
                     IndexConstraintOperator::LESS_THAN_EQUAL) {
        for (size_t i = 0; i < m_dataVector.size(); i++) {
          if (m_dataVector[i] >= lowerConstraint.blobVal &&
              m_dataVector[i] <= upperConstraint.blobVal &&
              m_dataVector[i].GetLength() > 0) {
            bitmap->Add(i);
          }
        }
      }
    } else if (lowerConstraint.operandType != OperandType::BLOB &&
               upperConstraint.operandType == OperandType::BLOB) {
      if (upperConstraint.op == IndexConstraintOperator::LESS_THAN) {
        return GetBitmapLT(upperConstraint);
      } else if (upperConstraint.op ==
                 IndexConstraintOperator::LESS_THAN_EQUAL) {
        return GetBitmapLTE(upperConstraint);
      }
    }

    return bitmap;
  }

  bool TryGetBlobValue(std::uint64_t documentID, BufferImpl& val) override {
    if (documentID < m_dataVector.size()) {
      if (val.GetLength() < m_dataVector[documentID].GetLength()) {
        val.Resize(m_dataVector[documentID].GetLength());
      }
      val.Copy(m_dataVector[documentID].GetData(),
               m_dataVector[documentID].GetLength());
      return true;
    }

    return false;
  }

 private:
  // We follow the comparison rules between different type from sqlite given at
  // https://www.sqlite.org/datatype3.html#section_4_3
  // This ensures that the behaviour between index scans/lookups and table
  // scans is consistent. Also it gives us a convention to follow.

  std::shared_ptr<MamaJenniesBitmap> GetBitmapEQ(const Constraint& constraint) {
    auto bitmap = std::make_shared<MamaJenniesBitmap>();
    if (constraint.operandType == OperandType::BLOB) {
      for (size_t i = 0; i < m_dataVector.size(); i++) {
        if (m_dataVector[i] == constraint.blobVal &&
            m_dataVector[i].GetLength() > 0) {
          bitmap->Add(i);
        }
      }
    }

    return bitmap;
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapLT(const Constraint& constraint) {
    auto bitmap = std::make_shared<MamaJenniesBitmap>();
    if (constraint.operandType == OperandType::BLOB) {
      for (size_t i = 0; i < m_dataVector.size(); i++) {
        if (m_dataVector[i] < constraint.blobVal &&
            m_dataVector[i].GetLength() > 0) {
          bitmap->Add(i);
        }
      }
    }

    // Blob is greater than other types according to our comparison rules
    return bitmap;
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapLTE(
      const Constraint& constraint) {
    auto bitmap = std::make_shared<MamaJenniesBitmap>();
    if (constraint.operandType == OperandType::BLOB) {
      for (size_t i = 0; i < m_dataVector.size(); i++) {
        if (m_dataVector[i] <= constraint.blobVal &&
            m_dataVector[i].GetLength() > 0) {
          bitmap->Add(i);
        }
      }
    }

    // Blob is greater than other types according to our comparison rules
    return bitmap;
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapGT(const Constraint& constraint) {
    auto bitmap = std::make_shared<MamaJenniesBitmap>();
    if (constraint.operandType == OperandType::BLOB) {
      for (size_t i = 0; i < m_dataVector.size(); i++) {
        if (m_dataVector[i] > constraint.blobVal &&
            m_dataVector[i].GetLength() > 0) {
          bitmap->Add(i);
        }
      }
    } else {
      // Blob is greater than other types according to our comparison rules
      for (size_t i = 0; i < m_dataVector.size(); i++) {
        bitmap->Add(i);
      }
    }

    return bitmap;
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapGTE(
      const Constraint& constraint) {
    auto bitmap = std::make_shared<MamaJenniesBitmap>();
    if (constraint.operandType == OperandType::BLOB) {
      for (size_t i = 0; i < m_dataVector.size(); i++) {
        if (m_dataVector[i] >= constraint.blobVal &&
            m_dataVector[i].GetLength() > 0) {
          bitmap->Add(i);
        }
      }
    } else {
      // Blob is greater than other types according to our comparison rules
      for (size_t i = 0; i < m_dataVector.size(); i++) {
        bitmap->Add(i);
      }
    }

    return bitmap;
  }

  IndexStat m_indexStat;
  std::vector<std::string> m_fieldNameTokens;
  std::vector<BufferImpl> m_dataVector;
  std::unique_ptr<Document> m_subDoc;
};
}  // namespace jonoondb_api
