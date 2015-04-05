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
    case jonoondb_api::FieldType::BASE_TYPE_UINT8:
      break;
    case jonoondb_api::FieldType::BASE_TYPE_UINT16:
      break;
    case jonoondb_api::FieldType::BASE_TYPE_UINT32:
      break;
    case jonoondb_api::FieldType::BASE_TYPE_UINT64:
      break;
    case jonoondb_api::FieldType::BASE_TYPE_INT8:
      break;
    case jonoondb_api::FieldType::BASE_TYPE_INT16:
      break;
    case jonoondb_api::FieldType::BASE_TYPE_INT32:
      break;
    case jonoondb_api::FieldType::BASE_TYPE_INT64:
      break;
    case jonoondb_api::FieldType::BASE_TYPE_FLOAT32:
      break;
    case jonoondb_api::FieldType::BASE_TYPE_DOUBLE:
      break;
    case jonoondb_api::FieldType::BASE_TYPE_STRING:
      break;
    case jonoondb_api::FieldType::BASE_TYPE_COMPLEX:
      break;
    default:
      break;
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
