#include <sstream>
#include "indexer_factory.h"
#include "status.h"
#include "index_info.h"
#include "enums.h"
#include "ewah_compressed_bitmap_indexer.h"

using namespace std;
using namespace jonoondb_api;

Status CreateEWAHCompressedBitmapIndexer(const IndexInfo& indexInfo,
                                         FieldType fieldType,
                                         Indexer*& indexer) {
  switch (fieldType) {
    case jonoondb_api::FieldType::BASE_TYPE_UINT8: {
      EWAHCompressedBitmapIndexer<uint8_t>* ewahIndexer;
      return EWAHCompressedBitmapIndexer<uint8_t>::Construct(indexInfo,
        fieldType, ewahIndexer);
    }
    case jonoondb_api::FieldType::BASE_TYPE_UINT16: {
      EWAHCompressedBitmapIndexer<uint16_t>* ewahIndexer;
      return EWAHCompressedBitmapIndexer<uint16_t>::Construct(indexInfo,
        fieldType, ewahIndexer);
    }    
    case jonoondb_api::FieldType::BASE_TYPE_UINT32:{
      EWAHCompressedBitmapIndexer<uint32_t>* ewahIndexer;
      return EWAHCompressedBitmapIndexer<uint32_t>::Construct(indexInfo,
        fieldType, ewahIndexer);
    }
    case jonoondb_api::FieldType::BASE_TYPE_UINT64:{
      EWAHCompressedBitmapIndexer<uint64_t>* ewahIndexer;
      return EWAHCompressedBitmapIndexer<uint64_t>::Construct(indexInfo,
        fieldType, ewahIndexer);
    }
    case jonoondb_api::FieldType::BASE_TYPE_INT8:{
      EWAHCompressedBitmapIndexer<int8_t>* ewahIndexer;
      return EWAHCompressedBitmapIndexer<int8_t>::Construct(indexInfo,
        fieldType, ewahIndexer);
    }
    case jonoondb_api::FieldType::BASE_TYPE_INT16:{
      EWAHCompressedBitmapIndexer<int16_t>* ewahIndexer;
      return EWAHCompressedBitmapIndexer<int16_t>::Construct(indexInfo,
        fieldType, ewahIndexer);
    }
    case jonoondb_api::FieldType::BASE_TYPE_INT32:{
      EWAHCompressedBitmapIndexer<int32_t>* ewahIndexer;
      return EWAHCompressedBitmapIndexer<int32_t>::Construct(indexInfo,
        fieldType, ewahIndexer);
    }
    case jonoondb_api::FieldType::BASE_TYPE_INT64:{
      EWAHCompressedBitmapIndexer<int64_t>* ewahIndexer;
      return EWAHCompressedBitmapIndexer<int64_t>::Construct(indexInfo,
        fieldType, ewahIndexer);
    }
    case jonoondb_api::FieldType::BASE_TYPE_FLOAT32:{
      EWAHCompressedBitmapIndexer<float>* ewahIndexer;
      return EWAHCompressedBitmapIndexer<float>::Construct(indexInfo,
        fieldType, ewahIndexer);
    }
    case jonoondb_api::FieldType::BASE_TYPE_DOUBLE:{
      EWAHCompressedBitmapIndexer<double>* ewahIndexer;
      return EWAHCompressedBitmapIndexer<double>::Construct(indexInfo,
        fieldType, ewahIndexer);
    }
    case jonoondb_api::FieldType::BASE_TYPE_STRING:{
      EWAHCompressedBitmapIndexer<string>* ewahIndexer;
      return EWAHCompressedBitmapIndexer<string>::Construct(indexInfo,
        fieldType, ewahIndexer);
    }    
    default: {
      std::ostringstream ss;
      ss << "Argument FieldType " << GetFieldString(fieldType) << " is not valid for EWAHCompressedBitmapIndexer.";
      string errorMsg = ss.str();
      return Status(kStatusInvalidArgumentCode, errorMsg.c_str(), errorMsg.length());
    }
  }

  return Status();
}

Status IndexerFactory::CreateIndexer(
    const IndexInfo& indexInfo,
    std::unordered_map<std::string, FieldType>& fieldType,
    Indexer*& indexer) {
  Status sts;
  switch (indexInfo.GetType()) {
    case IndexType::EWAHCompressedBitmap: {
      auto it = fieldType.find(indexInfo.GetColumn(0));
      if (it == fieldType.end()) {
        ostringstream ss;
        ss << "The column type for " << indexInfo.GetColumn(0)
           << " could not be determined.";
        auto errorMsg = ss.str();
        return Status(kStatusGenericErrorCode, errorMsg.c_str(),
                      errorMsg.length());
      }
      sts = CreateEWAHCompressedBitmapIndexer(indexInfo, it->second, indexer);
      break;
    }
    default:
      break;
  }

  return sts;
}
