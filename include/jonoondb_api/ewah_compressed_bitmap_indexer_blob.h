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
#include "jonoondb_api/buffer_impl.h"
#include "jonoondb_api/standard_deleters.h"
#include "document_id_generator.h"

namespace jonoondb_api {

class EWAHCompressedBitmapIndexerBlob final: public Indexer {
 public:
  static void Construct(const IndexInfoImpl& indexInfo,
                        const FieldType& fieldType,
                        EWAHCompressedBitmapIndexerBlob*& obj) {
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
          << " is not valid for EWAHCompressedBitmapIndexerBlob.";
      errorMsg = ss.str();
    }

    if (errorMsg.length() > 0) {
      throw InvalidArgumentException(errorMsg, __FILE__, __func__, __LINE__);
    }

    std::vector<std::string>
        tokens = StringUtils::Split(indexInfo.GetColumnName(),
                                    ".");
    IndexStat indexStat(indexInfo, fieldType);
    obj = new EWAHCompressedBitmapIndexerBlob(indexStat, tokens);
  }

  ~EWAHCompressedBitmapIndexerBlob() override {
  }

  static bool IsValidFieldType(FieldType fieldType) {
    return (fieldType == FieldType::BASE_TYPE_BLOB);
  }

  void ValidateForInsert(const Document& document) override {
    if (m_fieldNameTokens.size() > 1) {
      auto subDoc =
          DocumentUtils::GetSubDocumentRecursively(document, m_fieldNameTokens);
      subDoc->VerifyFieldForRead(m_fieldNameTokens.back(),
                                 m_indexStat.GetFieldType());
    } else {
      document.VerifyFieldForRead(m_fieldNameTokens.back(),
                                  m_indexStat.GetFieldType());
    }
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
    if (lowerConstraint.operandType == OperandType::BLOB &&
        upperConstraint.operandType == OperandType::BLOB) {
      std::map<BufferImpl, std::shared_ptr<MamaJenniesBitmap>>::const_iterator
          startIter, endIter;

      if (lowerConstraint.op == IndexConstraintOperator::GREATER_THAN_EQUAL) {
        startIter = m_compressedBitmaps.lower_bound(lowerConstraint.blobVal);
      } else {
        startIter = m_compressedBitmaps.upper_bound(lowerConstraint.blobVal);
      }
      
      while (startIter != m_compressedBitmaps.end()) {
        if (startIter->first.GetData() == nullptr) {
          continue;
        }        

        if (startIter->first < upperConstraint.blobVal) {
          bitmaps.push_back(startIter->second);
        } else if (
            upperConstraint.op == IndexConstraintOperator::LESS_THAN_EQUAL
                && startIter->first == upperConstraint.blobVal) {
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
  EWAHCompressedBitmapIndexerBlob(const IndexStat& indexStat,
                                  std::vector<std::string>& fieldNameTokens)
      : m_indexStat(indexStat),
        m_fieldNameTokens(fieldNameTokens),
        m_lastInsertedDocId(-1) {
  }

  void InsertInternal(std::uint64_t documentID, const Document& document) {
    switch (m_indexStat.GetFieldType()) {
      case FieldType::BASE_TYPE_BLOB: {
        std::size_t size = 0;
        auto val = document.GetBlobValue(m_fieldNameTokens.back(), size);
        BufferImpl buffer(const_cast<char*>(val), size, size, StandardDeleteNoOp);
        auto compressedBitmap = m_compressedBitmaps.find(buffer);
        if (compressedBitmap == m_compressedBitmaps.end()) {
          auto bm = shared_ptr<MamaJenniesBitmap>(new MamaJenniesBitmap());
          assert(documentID == m_lastInsertedDocId + 1);
          bm->Add(documentID);
          m_compressedBitmaps[buffer] = bm;
          m_lastInsertedDocId = documentID;
        } else {
          compressedBitmap->second->Add(documentID);
        }
        break;
      }
      default: {
        // This can never happen
        std::ostringstream ss;
        ss << "FieldType " << GetFieldString(m_indexStat.GetFieldType())
            << " is not valid for EWAHCompressedBitmapIndexerBlob.";
        throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
      }
    }
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapEQ(const Constraint& constraint) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    if (constraint.operandType == OperandType::BLOB) {
      auto iter = m_compressedBitmaps.find(constraint.blobVal);
      if (iter != m_compressedBitmaps.end() &&
          iter->first.GetData() != nullptr) {
        bitmaps.push_back(iter->second);
      }
    }

    return MamaJenniesBitmap::LogicalOR(bitmaps);
  }

  std::shared_ptr<MamaJenniesBitmap> GetBitmapLT(const Constraint& constraint,
                                                 bool orEqual) {
    std::vector<std::shared_ptr<MamaJenniesBitmap>> bitmaps;
    if (constraint.operandType == OperandType::BLOB) {
      for (auto& item : m_compressedBitmaps) {
        if (item.first.GetData() == nullptr) {
          continue;
        }

        if (item.first < constraint.blobVal) {
          bitmaps.push_back(item.second);
        } else {
          if (orEqual && item.first == constraint.blobVal) {
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
    if (constraint.operandType == OperandType::BLOB) {
      std::map<BufferImpl, std::shared_ptr<MamaJenniesBitmap>>::const_iterator
          iter;
      if (orEqual) {
        iter = m_compressedBitmaps.lower_bound(constraint.blobVal);
      } else {
        iter = m_compressedBitmaps.upper_bound(constraint.blobVal);
      }
      
      while (iter != m_compressedBitmaps.end()) {
        if (iter->first.GetData() == nullptr) {
          continue;
        }        

        bitmaps.push_back(iter->second);
        iter++;
      }
    } else {
      auto bm = make_shared<MamaJenniesBitmap>();
      for (std::uint64_t i = DOC_ID_START; i < m_lastInsertedDocId; i++) {
        bm->Add(i);
      }
      bitmaps.push_back(bm);
    }

    return MamaJenniesBitmap::LogicalOR(bitmaps);
  }

  std::int64_t m_lastInsertedDocId;
  IndexStat m_indexStat;
  std::vector<std::string> m_fieldNameTokens;
  std::map<BufferImpl, std::shared_ptr<MamaJenniesBitmap>> m_compressedBitmaps;
};
}  // namespace jonoondb_api
