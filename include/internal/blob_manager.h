#pragma once

#include <string>
#include <memory>
#include <map>
#include <mutex>
#include <vector>
#include <gsl/span.h>
#include "file_info.h"
#include "memory_mapped_file.h"
#include "concurrent_lru_cache.h"


namespace jonoondb_api {
//Forward Declarations
struct BlobMetadata;
class BufferImpl;
class FileNameManager;

// This class is responsible for reading/writing blobs into the data files
class BlobManager final {
public:
  BlobManager(std::unique_ptr<FileNameManager> fileNameManager, bool compressionEnabled, size_t maxDataFileSize, bool synchronous);
  ~BlobManager();  
  BlobManager(const BlobManager&) = delete;
  BlobManager(BlobManager&&) = delete;
  BlobManager& operator=(const BlobManager&) = delete;
  BlobManager& operator=(BlobManager&&) = delete;
  void Put(const BufferImpl& blob, BlobMetadata& blobMetadata);
  void MultiPut(gsl::span<const BufferImpl*> blobs, std::vector<BlobMetadata>& blobMetadataVec);
  void Get(const BlobMetadata& blobMetadata, BufferImpl& blob);
  void UnmapLRUDataFiles();
private:
  inline void Flush(size_t offset, size_t numBytes);
  void SwitchToNewDataFile();
  void PutInternal(const BufferImpl& blob, BlobMetadata& blobMetadata, size_t& byteWritten);

  FileInfo m_currentBlobFileInfo;
  std::shared_ptr<MemoryMappedFile> m_currentBlobFile;
  std::unique_ptr<FileNameManager> m_fileNameManager;
  bool m_compressionEnabled;
  size_t m_maxDataFileSize;
  ConcurrentLRUCache<int32_t, MemoryMappedFile> m_readerFiles;
  std::mutex m_writeMutex;
  bool m_synchronous;  
};
} // namespace jonoondb_api
