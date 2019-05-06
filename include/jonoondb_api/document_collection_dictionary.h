#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include "jonoondb_api/field.h"

namespace jonoondb_api {
// Forward Declarations
class DocumentCollection;

struct ColumnInfo {
  ColumnInfo(const std::string& colName, FieldType colType,
             const std::vector<std::string>& colNameTokens)
      : columnName(colName),
        columnType(colType),
        columnNameTokens(colNameTokens) {}
  std::string columnName;
  FieldType columnType;
  std::vector<std::string> columnNameTokens;
};

struct DocumentCollectionInfo {
  std::shared_ptr<DocumentCollection> collection;
  std::vector<ColumnInfo> columnsInfo;
  std::string createVTableStmt;
};

class DocumentCollectionDictionary {
 public:
  DocumentCollectionDictionary(const DocumentCollectionDictionary&) = delete;
  DocumentCollectionDictionary(DocumentCollectionDictionary&&) = delete;

  static DocumentCollectionDictionary* Instance();
  void Insert(const std::string& key,
              const std::shared_ptr<DocumentCollectionInfo>& collection);
  bool TryGet(const std::string& key,
              std::shared_ptr<DocumentCollectionInfo>& collection);
  void Remove(const std::string& key);
  void Clear();

 private:
  DocumentCollectionDictionary();
  static void Init();
  static std::once_flag onceFlag;
  static DocumentCollectionDictionary* instance;
  std::unordered_map<std::string, std::shared_ptr<DocumentCollectionInfo>>
      m_dictionary;
};

}  // namespace jonoondb_api
