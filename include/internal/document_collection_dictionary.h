#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <mutex>

namespace jonoondb_api {
// Forward Declarations
class DocumentCollection;

class DocumentCollectionDictionary {
public:
  DocumentCollectionDictionary(const DocumentCollectionDictionary&) = delete;
  DocumentCollectionDictionary(DocumentCollectionDictionary&&) = delete;
  
  static DocumentCollectionDictionary* Instance();
  void Insert(const std::string& key, const std::shared_ptr<DocumentCollection>& collection);
  bool TryGet(const std::string& key, std::shared_ptr<DocumentCollection>& collection);
  void Remove(const std::string& key);
  void Clear();

private:
  DocumentCollectionDictionary();
  static void Init();
  static std::once_flag onceFlag;
  static DocumentCollectionDictionary* instance;
  std::unordered_map<std::string, std::shared_ptr<DocumentCollection>> m_dictionary;
};
}  // jonoondb_api