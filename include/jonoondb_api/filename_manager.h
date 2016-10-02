#pragma once

#include <string>
#include <map>
#include <memory>
#include <mutex>
#include <boost/filesystem.hpp>
#include "concurrent_map.h"
#include "jonoondb_api_export.h"

//Forward declarations
struct sqlite3;
struct sqlite3_stmt;

namespace jonoondb_api {
//Forward declaration
struct FileInfo;

class JONOONDB_API_EXPORT  FileNameManager {
 public:
  FileNameManager(const std::string& dbPath, const std::string& dbName,
                  const std::string& collectionName, bool createDBIfMissing);
  ~FileNameManager();
  void GetCurrentDataFileInfo(bool createIfMissing, FileInfo& fileInfo);
  void GetNextDataFileInfo(FileInfo& fileInfo);
  void GetFileInfo(const int fileKey, std::shared_ptr<FileInfo>& fileInfo);
  void UpdateDataFileLength(int fileKey, int64_t length);
 private:
  void AddFileRecord(int fileKey, const std::string& fileName);
  void FinalizeStatements();
  std::string m_dbName;
  boost::filesystem::path m_dbPath;
  std::string m_collectionName;
  std::unique_ptr<sqlite3, void (*)(sqlite3*)> m_db;
  sqlite3_stmt* m_putStatement;
  sqlite3_stmt* m_getFileNameStatement;
  sqlite3_stmt* m_getLastFileKeyStatement;
  sqlite3_stmt* m_updateStatement;
  //std::map<int, std::shared_ptr<FileInfo>> m_fileInfoMap;
  ConcurrentMap<int32_t, FileInfo> m_fileInfoMap;
  std::mutex m_mutex;
};
} // namespace jonoondb_api
