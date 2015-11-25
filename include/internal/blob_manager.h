#pragma once

#include <string>
#include <memory>
#include <map>
#include <mutex>
#include "file_info.h"
#include "memory_mapped_file.h"
#include "concurrent_lru_cache.h"


namespace jonoondb_api {
//Forward Declarations
class Status;
struct BlobMetadata;
class BufferImpl;
class FileNameManager;

// This class is responsible for reading/writing blobs into the data files
class BlobManager {
public:
  BlobManager(std::unique_ptr<FileNameManager> fileNameManager, bool compressionEnabled, size_t maxDataFileSize, bool synchronous);
  ~BlobManager();
  void Initialize();
  void Put(const BufferImpl& blob, BlobMetadata& blobMetadata);
  void MultiPut(const BufferImpl blobs[], const int arrayLength, BlobMetadata blobMetadatas[]);
  void Get(const BlobMetadata& blobMetadata, BufferImpl& blob);
  void UnmapLRUDataFiles();
private:
  FileInfo m_currentBlobFileInfo;
  std::shared_ptr<MemoryMappedFile> m_currentBlobFile;
  std::unique_ptr<FileNameManager> m_fileNameManager;
  bool m_compressionEnabled;
  size_t m_maxDataFileSize;
  bool m_initialized;
  ConcurrentLRUCache<int32_t, MemoryMappedFile> m_readerFiles;
  std::mutex m_writeMutex;
  bool m_synchronous;

  inline void Flush(size_t offset, size_t numBytes);
  void SwitchToNewDataFile();
  void PutInternal(const BufferImpl& blob, BlobMetadata& blobMetadata, size_t& byteWritten);
};
} // namespace jonoondb_api
