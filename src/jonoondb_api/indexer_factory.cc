#include <sstream>
#include "indexer_factory.h"
#include "status.h"
#include "index_info.h"
#include "enums.h"
#include "ewah_compressed_bitmap_indexer.h"

using namespace std;
using namespace jonoondb_api;

Status IndexerFactory::CreateIndexer(
    const IndexInfo& indexInfo,
    std::unordered_map<std::string, FieldType>& fieldType, Indexer*& indexer) {
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

      EWAHCompressedBitmapIndexer* ewahIndexer;
      sts = EWAHCompressedBitmapIndexer::Construct(indexInfo, it->second,
                                                   ewahIndexer);
      indexer = static_cast<Indexer*>(ewahIndexer);
      break;
    }
    default:
      break;
  }

  return sts;
}
