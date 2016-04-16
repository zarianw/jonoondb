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
// Forward Declarations
struct BlobMetadata;
class BufferImpl;
class FileNameManager;

struct BlobHeader {
  uint8_t version;
  bool compressed;
  uint16_t crc;
  uint64_t blobSize;
};

// This class is responsible for reading/writing blobs into the data files
class BlobManager final {
public:
  BlobManager(std::unique_ptr<FileNameManager> fileNameManager,
              bool compressionEnabled, size_t maxDataFileSize,
              bool synchronous);  
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
  void ReadBlobHeader(char*& offset, BlobHeader& blobHeader);

  FileInfo m_currentBlobFileInfo;
  std::shared_ptr<MemoryMappedFile> m_currentBlobFile;
  std::unique_ptr<FileNameManager> m_fileNameManager;
  bool m_compressionEnabled;
  size_t m_maxDataFileSize;
  ConcurrentLRUCache<int32_t, MemoryMappedFile> m_readerFiles;
  std::mutex m_writeMutex;
  bool m_synchronous;  
};

class BlobIterator {
public:
  BlobIterator(FileInfo fileInfo);
  std::size_t GetNextBatch(std::vector<BufferImpl>& blobs,
                           std::vector<BlobMetadata>& metadataVec);
private:
  MemoryMappedFile m_memMapFile;
  std::int64_t m_currentPosition;
  FileInfo m_fileInfo;
  static bool LittleEndianMachine;
};
} // namespace jonoondb_api
