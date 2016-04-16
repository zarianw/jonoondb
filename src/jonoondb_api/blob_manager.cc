#include <string>
#include <assert.h>
#include <boost/filesystem.hpp>
#include <boost/endian/conversion.hpp>
#include "blob_manager.h"
#include "exception_utils.h"
#include "buffer_impl.h"
#include "blob_metadata.h"
#include "filename_manager.h"
#include "file.h"
#include "standard_deleters.h"
#include "jonoondb_utils/varint.h"

using namespace std;
using namespace boost::filesystem;
using namespace jonoondb_api;
using namespace jonoondb_utils;

#define DEFAULT_MEM_MAP_LRU_CACHE_SIZE 2

bool LittleEndianMachine = Varint::OnLittleEndianMachine();

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

  // Set the file length
  m_fileNameManager->UpdateDataFileLength(m_currentBlobFileInfo.fileKey, m_currentBlobFile->GetCurrentWriteOffset());
}

void BlobManager::MultiPut(gsl::span<const BufferImpl*> blobs, std::vector<BlobMetadata>& blobMetadataVec) {
  assert(blobs.size() == blobMetadataVec.size());
  size_t bytesWritten = 0, totalBytesWrittenInFile = 0;
  // Lock will be acquired on the next line and released when lock goes out of scope  
  lock_guard<mutex> lock(m_writeMutex);
  size_t baseOffsetInFile = m_currentBlobFile->GetCurrentWriteOffset();
  int headerSize = 1 + 4 + 8;
  for (int i = 0; i < blobs.size(); i++) {
    size_t currentOffset = m_currentBlobFile->GetCurrentWriteOffset();

    if (blobs[i]->GetLength() + headerSize + currentOffset > m_maxDataFileSize) {
      // The file size will exceed the m_maxDataFileSize if blob is written in the current file
      // First flush the contents if required
      try {
        Flush(baseOffsetInFile, totalBytesWrittenInFile);
      } catch (...) {
        m_currentBlobFile->SetCurrentWriteOffset(baseOffsetInFile);
        throw;
      }
      // Now lets switch to a new file
      SwitchToNewDataFile();

      // Reset baseOffset and totalBytesWritten
      baseOffsetInFile = m_currentBlobFile->GetCurrentWriteOffset();
      totalBytesWrittenInFile = 0;
    }

    try {
      PutInternal(*blobs[i], blobMetadataVec[i], bytesWritten);
    } catch (...) {
      m_currentBlobFile->SetCurrentWriteOffset(baseOffsetInFile);
      throw;
    }

    totalBytesWrittenInFile += bytesWritten;
  }

  // Flush to make sure all blobs are written to disk
  try {
    Flush(baseOffsetInFile, totalBytesWrittenInFile);
  } catch (...) {
    m_currentBlobFile->SetCurrentWriteOffset(baseOffsetInFile);
    throw;
  }

  // Set the file length
  m_fileNameManager->UpdateDataFileLength(m_currentBlobFileInfo.fileKey, m_currentBlobFile->GetCurrentWriteOffset());
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
  uint8_t verAndFlags = 0;
  memcpy(&verAndFlags, offsetAddress, 1);
  // Drop last 4 bits
  uint8_t headerVersion = verAndFlags & 0xF0; // 0xF0 is equal to 1111 0000
  headerVersion = headerVersion >> 4;
  
  uint8_t compressed = verAndFlags & 1;
  offsetAddress++;

  uint16_t crc;
  memcpy(&crc, offsetAddress, sizeof(crc));
  offsetAddress += sizeof(crc);
  // swap crc before using it if on big endian machine

  uint8_t varIntBuffer[kMaxVarintBytes];
  uint64_t blobSize;
  memcpy(&varIntBuffer, offsetAddress, sizeof(varIntBuffer));
  auto varIntSize = Varint::DecodeVarint(varIntBuffer, &blobSize);
  if (varIntSize == -1) {
    // throw
  }
  offsetAddress += varIntSize;

  //5. Read Blob contents
  if (blob.GetLength() < blobSize) {
    //Passed in buffer is not big enough. Lets resize it
    blob.Resize(blobSize);
  }

  blob.Copy(offsetAddress, blobSize);
}

void BlobManager::UnmapLRUDataFiles() {
  m_readerFiles.PerformEviction();
}

BlobIterator::BlobIterator(FileInfo fileInfo) :
  m_memMapFile(fileInfo.fileNameWithPath, MemoryMappedFileMode::ReadOnly, 0, true),
  m_fileInfo(std::move(fileInfo)), m_currentPosition(0) {
}

std::size_t BlobIterator::GetNextBatch(std::vector<BufferImpl>& blobs,
                                       std::vector<BlobMetadata>& blobMetadataVec) {
  assert(blobs.size() == blobMetadataVec.size());
  assert(blobs.size() > 0);

  std::size_t batchSize = 0;

  for (size_t i = 0; i < blobs.size(); i++) {
    if (m_currentPosition >= m_fileInfo.dataLength) {
      // We are at the end of file
      break;
    }

    // Read the data from the file
    char* offsetAddress = m_memMapFile.GetOffsetAddressAsCharPtr(m_currentPosition);

    // Now read the header. 
    // Header: CompressionEnabledFlag (1 Byte) + CRC (4 Bytes) + SizeOfBlob (8 bytes) + BlobData (SizeOfBlob)
    bool compressionFlag;
    int32_t crc;
    uint64_t blobSize;

    memcpy(&compressionFlag, offsetAddress, 1);
    ++offsetAddress;

    memcpy(&crc, offsetAddress, 4);
    offsetAddress += 4;

    memcpy(&blobSize, offsetAddress, 8);
    offsetAddress += 8;

    blobs[i] = std::move(BufferImpl(offsetAddress, blobSize, blobSize, StandardDeleteNoOp));
    blobMetadataVec[i].fileKey = m_fileInfo.fileKey;
    blobMetadataVec[i].offset = m_currentPosition;

    m_currentPosition += (1 + 4 + 8) + blobSize;
    ++batchSize;
  }

  return batchSize;
}

inline void BlobManager::Flush(size_t offset, size_t numBytes) {
  m_currentBlobFile->Flush(offset, numBytes);
}

void BlobManager::SwitchToNewDataFile() {
  FileInfo fileInfo;
  m_fileNameManager->GetNextDataFileInfo(fileInfo);
  File::FastAllocate(fileInfo.fileNameWithPath, m_maxDataFileSize);

  auto file = std::make_unique<MemoryMappedFile>(fileInfo.fileNameWithPath, MemoryMappedFileMode::ReadWrite, 0, !m_synchronous);
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
  std::uint8_t varIntBuffer[10];
  auto varIntSize = Varint::EncodeVarint(sizeOfBlob, varIntBuffer);

  //2. Calculate CRC on uncompressed data
  uint16_t crc = 0;
  if (!LittleEndianMachine) {
    boost::endian::endian_reverse_inplace(crc);
  }

  // 3. Record the current offset.
  size_t offset = m_currentBlobFile->GetCurrentWriteOffset();

  // 4. Write the header
  // Header: CompressionEnabledFlag (1 Byte) + CRC (4 Bytes) + SizeOfBlob (8 bytes) + BlobData (SizeOfBlob)
  std::uint8_t verAndFlags = 0;
  verAndFlags |= 1 << 4; // version
  verAndFlags |= m_compressionEnabled ? 1 : 0; // compression flag

  m_currentBlobFile->WriteAtCurrentPosition(&verAndFlags, 1);
  m_currentBlobFile->WriteAtCurrentPosition(&crc, 2);
  m_currentBlobFile->WriteAtCurrentPosition(&varIntBuffer, varIntSize);

  // 5. Write the blob contents
  m_currentBlobFile->WriteAtCurrentPosition(blob.GetData(), blob.GetLength());

  bytesWritten = 1 + 2 + varIntSize + blob.GetLength();

  // 6. Fill and return blobMetaData
  blobMetadata.offset = offset;
  blobMetadata.fileKey = m_currentBlobFileInfo.fileKey;
}

inline void BlobManager::ReadBlobHeader(char*& offsetAddress, BlobHeader& blobHeader) {
  // Header: VerAndFlags (1 Byte) + CRC (2 Bytes) + SizeOfBlob (varint) + BlobData (SizeOfBlob)
  uint8_t verAndFlags = 0;
  memcpy(&verAndFlags, offsetAddress, 1);
  // Drop last 4 bits
  blobHeader.version = verAndFlags & 0xF0; // 0xF0 is equal to 1111 0000
  blobHeader.version = blobHeader.version >> 4;

  blobHeader.compressed = (verAndFlags & 1) == 1;
  offsetAddress++;
  
  memcpy(&blobHeader.crc, offsetAddress, sizeof(blobHeader.crc));
  offsetAddress += sizeof(blobHeader.crc);
  // swap crc before using it if on big endian machine

  uint8_t varIntBuffer[kMaxVarintBytes];
  uint64_t blobSize;
  memcpy(&varIntBuffer, offsetAddress, sizeof(varIntBuffer));
  auto varIntSize = Varint::DecodeVarint(varIntBuffer, &blobSize);
  if (varIntSize == -1) {
    // throw
  }
  offsetAddress += varIntSize;
}


