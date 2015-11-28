#include <string>
#include <assert.h>
#include <boost/filesystem.hpp>
#include "blob_manager.h"
#include "status.h"
#include "exception_utils.h"
#include "buffer_impl.h"
#include "blob_metadata.h"
#include "filename_manager.h"
#include "file.h"

using namespace std;
using namespace boost::filesystem;
using namespace jonoondb_api;

#define DEFAULT_MEM_MAP_LRU_CACHE_SIZE 2

BlobManager::BlobManager(unique_ptr<FileNameManager> fileNameManager, bool compressionEnabled, size_t maxDataFileSize, bool synchronous)
  : m_fileNameManager(move(fileNameManager)), m_compressionEnabled(compressionEnabled),
  m_maxDataFileSize(maxDataFileSize), m_currentBlobFile(nullptr), m_synchronous(synchronous), m_readerFiles(DEFAULT_MEM_MAP_LRU_CACHE_SIZE) {
  m_fileNameManager->GetCurrentDataFileInfo(true, m_currentBlobFileInfo);
  path pathObj(m_currentBlobFileInfo.fileNameWithPath);
  //Check if the file exist or do we have to create it
  if (!boost::filesystem::exists(pathObj)) {
    File::FastAllocate(m_currentBlobFileInfo.fileNameWithPath, m_maxDataFileSize);
  }
  //We have the file lets memory map it  
  m_currentBlobFile.reset(new MemoryMappedFile(m_currentBlobFileInfo.fileNameWithPath, MemoryMappedFileMode::ReadWrite, 0, !m_synchronous));

  //Set the MemMapFile offset
  if (m_currentBlobFileInfo.dataLength != -1) {
    m_currentBlobFile->SetCurrentWriteOffset(m_currentBlobFileInfo.dataLength);
    //Reset the data length of the blob file, we will set it on file switch or shutdown
    m_fileNameManager->UpdateDataFileLength(m_currentBlobFileInfo.fileKey, m_currentBlobFile->GetCurrentWriteOffset());
  }
  
  m_readerFiles.Add(m_currentBlobFileInfo.fileKey, m_currentBlobFile, false);
}

BlobManager::~BlobManager(void) {
  //Todo: Error handing
  //We should persist the length of the current blob file
  if (m_fileNameManager && m_currentBlobFile) {
    try {
      m_fileNameManager->UpdateDataFileLength(m_currentBlobFileInfo.fileKey, m_currentBlobFile->GetCurrentWriteOffset());
    } catch (std::exception&) {
      // Todo: Log this error, we should not throw exceptions from dtors
      // Google "throwing exceptions from destructors" to know why
    }

  }
}

void BlobManager::SwitchToNewDataFile() {
  FileInfo fileInfo;
  m_fileNameManager->GetNextDataFileInfo(fileInfo);
  File::FastAllocate(fileInfo.fileNameWithPath, m_maxDataFileSize);  

  auto file = std::make_unique<MemoryMappedFile>(fileInfo.fileNameWithPath, MemoryMappedFileMode::ReadOnly, 0, !m_synchronous);
  m_fileNameManager->UpdateDataFileLength(m_currentBlobFileInfo.fileKey, m_currentBlobFile->GetCurrentWriteOffset());

  //Set the evictable flag on the current file before switching
  bool retVal = m_readerFiles.SetEvictable(m_currentBlobFileInfo.fileKey, true);
  assert(retVal);

  m_currentBlobFileInfo = fileInfo;
  m_currentBlobFile.reset(file.release());
  m_readerFiles.Add(m_currentBlobFileInfo.fileKey, m_currentBlobFile, false);
}

void BlobManager::PutInternal(const BufferImpl& blob, BlobMetadata& blobMetadata, size_t& bytesWritten) {
  // 1. Compress if required and capture the storage size of blob
  uint64_t sizeOfBlob = blob.GetLength();

  //2. Calculate CRC on uncompressed data
  int32_t crc = 0;

  // 3. Record the current offset.
  size_t offset = m_currentBlobFile->GetCurrentWriteOffset();

  // 4. Write the header
  // Header: CompressionEnabledFlag (1 Byte) + CRC (4 Bytes) + SizeOfBlob (8 bytes) + BlobData (SizeOfBlob)
  m_currentBlobFile->WriteAtCurrentPosition(&m_compressionEnabled, 1);
  m_currentBlobFile->WriteAtCurrentPosition(&crc, 4);
  m_currentBlobFile->WriteAtCurrentPosition(&sizeOfBlob, 8);

  // 5. Write the blob contents
  m_currentBlobFile->WriteAtCurrentPosition(blob.GetData(), blob.GetLength());

  bytesWritten = 1 + 4 + 8 + blob.GetLength();

  // 6. Fill and return blobMetaData
  blobMetadata.offset = offset;
  blobMetadata.fileKey = m_currentBlobFileInfo.fileKey;
}

void BlobManager::Put(const BufferImpl& blob, BlobMetadata& blobMetadata) {
  //Lock will be acquired on the next line and released when lock goes out of scope
  lock_guard<mutex> lock(m_writeMutex);
  size_t bytesWritten = 0;
  size_t currentOffsetInFile = m_currentBlobFile->GetCurrentWriteOffset();
  int headerSize = 1 + 4 + 8;

  if (blob.GetLength() + headerSize + currentOffsetInFile > m_maxDataFileSize) {
    SwitchToNewDataFile();
    currentOffsetInFile = m_currentBlobFile->GetCurrentWriteOffset();
  }

  try {
    PutInternal(blob, blobMetadata, bytesWritten);
    //Flush the contents to ensure durability
    Flush(currentOffsetInFile, bytesWritten);
  } catch (...) {
    m_currentBlobFile->SetCurrentWriteOffset(currentOffsetInFile);
    throw;
  }
}

void BlobManager::MultiPut(const BufferImpl blobs[], const int arrayLength, BlobMetadata blobMetadatas[]) {
  //Lock will be acquired on the next line and released when lock goes out of scope
  lock_guard<mutex> lock(m_writeMutex);
  size_t bytesWritten = 0, totalBytesWrittenInFile = 0;
  size_t baseOffsetInFile = m_currentBlobFile->GetCurrentWriteOffset();
  Status status;
  int headerSize = 1 + 4 + 8;
  for (int i = 0; i < arrayLength; i++) {
    size_t currentOffset = m_currentBlobFile->GetCurrentWriteOffset();

    if (blobs[i].GetLength() + headerSize + currentOffset > m_maxDataFileSize) {
      //The file size will exceed the m_maxDataFileSize if blob is written in the current file
      //First flush the contents if required
      try {
        Flush(baseOffsetInFile, totalBytesWrittenInFile);
      } catch (...) {
        m_currentBlobFile->SetCurrentWriteOffset(baseOffsetInFile);
        throw;
      }
      //Now lets switch to a new file
      SwitchToNewDataFile();

      //Reset baseOffset and totalBytesWritten
      baseOffsetInFile = m_currentBlobFile->GetCurrentWriteOffset();
      totalBytesWrittenInFile = 0;
    }

    try {
      PutInternal(blobs[i], blobMetadatas[i], bytesWritten);
    } catch (...) {
      m_currentBlobFile->SetCurrentWriteOffset(baseOffsetInFile);
      throw;
    }

    totalBytesWrittenInFile += bytesWritten;
  }

  //Flush to make sure all blobs are written to disk
  try {
    Flush(baseOffsetInFile, totalBytesWrittenInFile);
  } catch (...) {
    m_currentBlobFile->SetCurrentWriteOffset(baseOffsetInFile);
    throw;
  }
}

inline void BlobManager::Flush(size_t offset, size_t numBytes) {
  m_currentBlobFile->Flush(offset, numBytes);
}

void BlobManager::Get(const BlobMetadata& blobMetaData, BufferImpl& blob) {
  //1. Get the FileInfo
  auto fileInfo = make_shared<FileInfo>();
  m_fileNameManager->GetFileInfo(blobMetaData.fileKey, fileInfo);

  //2. Get the file to read the data from
  std::shared_ptr<MemoryMappedFile> memMapFile;
  if (!m_readerFiles.Find(fileInfo->fileKey, memMapFile)) {
    //Open the memmap file
    memMapFile.reset(new MemoryMappedFile(fileInfo->fileNameWithPath.c_str(), MemoryMappedFileMode::ReadOnly, 0, !m_synchronous));

    //3. Add mmap file in the ConcurrentMap for memmap file for future use	
    m_readerFiles.Add(fileInfo->fileKey, memMapFile, true);
  }

  //3. Read the data from the file
  char* offsetAddress = memMapFile->GetOffsetAddressAsCharPtr(blobMetaData.offset);

  //4. Now read the header. 
  // Header: CompressionEnabledFlag (1 Byte) + CRC (4 Bytes) + SizeOfBlob (8 bytes) + BlobData (SizeOfBlob)
  bool compressionFlag;
  int32_t crc;
  uint64_t blobSize;

  memcpy(&compressionFlag, offsetAddress, 1);
  offsetAddress++;

  memcpy(&crc, offsetAddress, 4);
  offsetAddress += 4;

  memcpy(&blobSize, offsetAddress, 8);
  offsetAddress += 8;

  //5. Read Blob contents
  if (blob.GetLength() < blobSize) {
    //Passed in buffer is not big enough. Lets resize it
    blob.Resize(blobSize);
  }

  memcpy(blob.GetDataForWrite(), offsetAddress, blobSize);
  blob.SetLength(blobSize);
}

void BlobManager::UnmapLRUDataFiles() {
  m_readerFiles.PerformEviction();
}
