#include <mutex>
#include "document_collection_dictionary.h"

using namespace jonoondb_api;

std::once_flag DocumentCollectionDictionary::onceFlag;
DocumentCollectionDictionary* DocumentCollectionDictionary::instance = nullptr;

DocumentCollectionDictionary* DocumentCollectionDictionary::Instance() {  
  std::call_once(onceFlag, Init);
  return instance;
}

void DocumentCollectionDictionary::Insert(const std::string& key, const std::shared_ptr<DocumentCollectionInfo>& collection) {
  m_dictionary[key] = collection;
}

bool DocumentCollectionDictionary::TryGet(const std::string& key, std::shared_ptr<DocumentCollectionInfo>& collection) {
  auto iter = m_dictionary.find(key);
  if (iter != m_dictionary.end()) {
    collection = iter->second;    
    return true;
  } 

  return false;
}

void DocumentCollectionDictionary::Remove(const std::string& key) {
  m_dictionary.erase(key);
}

void DocumentCollectionDictionary::Clear() {
  m_dictionary.clear();
}

void DocumentCollectionDictionary::Init() {
  instance = new DocumentCollectionDictionary();
}

DocumentCollectionDictionary::DocumentCollectionDictionary() {
}