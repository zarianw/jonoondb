#include <string>
#include <assert.h>
#include <boost/filesystem.hpp>
#include <boost/endian/conversion.hpp>
#include "lz4.h"
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

namespace jonoondb_api {
const uint8_t kBlobHeaderVersion = 1;

struct BlobHeader {
  std::uint8_t version;
  bool compressed;
  std::uint16_t crc;
  std::uint64_t blobSize;

  inline static int GetHeaderSize(std::uint64_t blobSize) {
    const int fixedSize = 3; // verAndFlags + crc
    if (blobSize < 128) {
      return 1 + fixedSize;
    } else if (blobSize < 16384) {
      return 2 + fixedSize;
    } else if (blobSize < 2097152) {
      return 3 + fixedSize;
    } else if (blobSize < 268435456) {
      return 4 + fixedSize;
    } else if (blobSize < 34359738368L) {
      return 5 + fixedSize;
    } else if (blobSize < 4398046511104L) {
      return 6 + fixedSize;
    } else if (blobSize < 562949953421312L) {
      return 7 + fixedSize;
    } else if (blobSize < 72057594037927936L) {
      return 8 + fixedSize;
    } else if (blobSize < 9223372036854775808L) {
      return 9 + fixedSize;
    } else {
      return 10 + fixedSize;
    }
  }

  inline static void ReadBlobHeader(char*& offsetAddress, BlobHeader& header) {
    // Header: VerAndFlags (1 Byte) + CRC (2 Bytes) + SizeOfBlob (varint) + BlobData (SizeOfBlob)
    std::uint8_t verAndFlags = 0;
    memcpy(&verAndFlags, offsetAddress, 1);
    offsetAddress++;
    // Drop last 4 bits
    header.version = verAndFlags & 0xF0; // 0xF0 is equal to 1111 0000
    header.version = header.version >> 4;

    header.compressed = (verAndFlags & 1) == 1;

    memcpy(&header.crc, offsetAddress, sizeof(header.crc));
    offsetAddress += sizeof(header.crc);
    // swap crc before using it if on big endian machine
    if (!LittleEndianMachine) {
      boost::endian::endian_reverse_inplace(header.crc);
    }

    auto varIntSize = Varint::DecodeVarint((std::uint8_t*)*&offsetAddress, &header.blobSize);
    if (varIntSize == -1) {
      std::string msg = "Failed to read the blob header. Varint blobSize is greater than 10 bytes.";
      throw JonoonDBException(msg, __FILE__, __func__, __LINE__);
    }
    offsetAddress += varIntSize;
  }

  inline static int WriteBlobHeader(std::shared_ptr<MemoryMappedFile>& memMappedFile, const BlobHeader& header) {
    std::uint8_t varIntBuffer[kMaxVarintBytes];
    auto varIntSize = Varint::EncodeVarint(header.blobSize, varIntBuffer);

    std::uint16_t crc = header.crc;
    if (!LittleEndianMachine) {
      crc = boost::endian::endian_reverse(header.crc);
    }    

    // Write the header
    // Header: VerAndFlags (1 Byte) + CRC (2 Bytes) + SizeOfBlob (varint)
    std::uint8_t verAndFlags = 0;
    verAndFlags |= 1 << 4; // version
    verAndFlags |= header.compressed ? 1 : 0; // compression flag

    memMappedFile->WriteAtCurrentPosition(&verAndFlags, sizeof(verAndFlags));
    memMappedFile->WriteAtCurrentPosition(&crc, sizeof(crc));
    memMappedFile->WriteAtCurrentPosition(&varIntBuffer, varIntSize);

    // return bytes written
    return sizeof(verAndFlags) + sizeof(crc) + varIntSize;
  }
};
} // namespace jonoondb_api



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
  int headerSize = BlobHeader::GetHeaderSize(blob.GetLength());

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
  
  for (int i = 0; i < blobs.size(); i++) {
    size_t currentOffset = m_currentBlobFile->GetCurrentWriteOffset();
    auto headerSize = BlobHeader::GetHeaderSize(blobs[i]->GetLength());
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
  // Get the FileInfo
  auto fileInfo = make_shared<FileInfo>();
  m_fileNameManager->GetFileInfo(blobMetaData.fileKey, fileInfo);

  // Get the file to read the data from
  std::shared_ptr<MemoryMappedFile> memMapFile;
  if (!m_readerFiles.Find(fileInfo->fileKey, memMapFile)) {
    //Open the memmap file
    memMapFile.reset(new MemoryMappedFile(fileInfo->fileNameWithPath.c_str(), MemoryMappedFileMode::ReadOnly, 0, !m_synchronous));

    // Add mmap file in the ConcurrentMap for memmap file for future use	
    m_readerFiles.Add(fileInfo->fileKey, memMapFile, true);
  }

  // Read the data from the file
  char* offsetAddress = memMapFile->GetOffsetAddressAsCharPtr(blobMetaData.offset);

  // Now read the header. 
  BlobHeader header;
  BlobHeader::ReadBlobHeader(offsetAddress, header);

  // Read Blob contents  
  if (header.compressed) {
    
  } else {
    if (blob.GetLength() < header.blobSize) {
      //Passed in buffer is not big enough. Lets resize it
      blob.Resize(header.blobSize);
    }
    blob.Copy(offsetAddress, header.blobSize);
  }

  
}

void BlobManager::UnmapLRUDataFiles() {
  m_readerFiles.PerformEviction();
}

BlobIterator::BlobIterator(FileInfo fileInfo) :
  m_fileInfo(std::move(fileInfo)),
  m_memMapFile(m_fileInfo.fileNameWithPath, MemoryMappedFileMode::ReadOnly, 0, true),
  m_currentOffsetAddress(m_memMapFile.GetOffsetAddressAsCharPtr(0)) {
}

std::size_t BlobIterator::GetNextBatch(std::vector<BufferImpl>& blobs,
                                       std::vector<BlobMetadata>& blobMetadataVec) {
  assert(blobs.size() == blobMetadataVec.size());
  assert(blobs.size() > 0);

  std::size_t batchSize = 0;

  for (size_t i = 0; i < blobs.size(); i++) {
    auto position = m_currentOffsetAddress - m_memMapFile.GetBaseAddress();
    if (position >= m_fileInfo.dataLength) {
      // We are at the end of file
      break;
    }
    
    // Now read the header. 
    BlobHeader header;
    BlobHeader::ReadBlobHeader(m_currentOffsetAddress, header);

    blobs[i] = std::move(BufferImpl(m_currentOffsetAddress, header.blobSize, header.blobSize, StandardDeleteNoOp));
    blobMetadataVec[i].fileKey = m_fileInfo.fileKey;
    blobMetadataVec[i].offset = position;
    m_currentOffsetAddress += header.blobSize;
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
  BlobHeader header;
  header.version = kBlobHeaderVersion;
  header.compressed = m_compressionEnabled;
  // Record the current offset.
  size_t offset = m_currentBlobFile->GetCurrentWriteOffset();
  // Compress if required and capture the storage size of blob
  if (m_compressionEnabled) {    
    auto maxCompSize = LZ4_compressBound(blob.GetLength());
    if (maxCompSize > m_compBuffer.GetCapacity()) {
      m_compBuffer.Resize(maxCompSize);
    }
    auto compSize = LZ4_compress_default(
      blob.GetData(), m_compBuffer.GetDataForWrite(),
      blob.GetLength(), m_compBuffer.GetCapacity());
    if (compSize == 0) {
      // throw
    }
    m_compBuffer.SetLength(compSize);
    header.blobSize = compSize;
    // Todo: Calculate CRC
    header.crc = 0;
    // Write the header  
    auto headerBytes = BlobHeader::WriteBlobHeader(m_currentBlobFile, header);
    // Write the blob contents
    m_currentBlobFile->WriteAtCurrentPosition(m_compBuffer.GetData(), m_compBuffer.GetLength());
    bytesWritten = headerBytes + m_compBuffer.GetLength();
  } else {
    header.blobSize = blob.GetLength();
    // Todo: Calculate CRC
    header.crc = 0;
    // Write the header  
    auto headerBytes = BlobHeader::WriteBlobHeader(m_currentBlobFile, header);
    // Write the blob contents
    m_currentBlobFile->WriteAtCurrentPosition(blob.GetData(), blob.GetLength());
    bytesWritten = headerBytes + blob.GetLength();
  } 

  // 6. Fill and return blobMetaData
  blobMetadata.offset = offset;
  blobMetadata.fileKey = m_currentBlobFileInfo.fileKey;
}
