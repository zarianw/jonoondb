#include <sstream>
#include "indexer_factory.h"
#include "status.h"
#include "index_info_impl.h"
#include "enums.h"
#include "ewah_compressed_bitmap_indexer.h"
#include "jonoondb_exceptions.h"

using namespace std;
using namespace jonoondb_api;

Indexer* IndexerFactory::CreateIndexer(
    const IndexInfoImpl& indexInfo,
    const std::unordered_map<std::string, FieldType>& fieldType) {
  Status sts;
  switch (indexInfo.GetType()) {
    case IndexType::EWAHCompressedBitmap: {
      auto it = fieldType.find(indexInfo.GetColumnName());
      if (it == fieldType.end()) {
        ostringstream ss;
        ss << "The column type for " << indexInfo.GetColumnName()
           << " could not be determined.";        
        throw JonoonDBException(ss.str(), __FILE__, "", __LINE__);
      }

      EWAHCompressedBitmapIndexer* ewahIndexer;
      sts = EWAHCompressedBitmapIndexer::Construct(indexInfo, it->second,
                                                   ewahIndexer);
      return static_cast<Indexer*>(ewahIndexer);      
    }
    default:
      std::ostringstream ss;
      ss << "Cannot create Indexer. Index type '" << static_cast<int32_t>(indexInfo.GetType()) << "' is unknown.";
      throw JonoonDBException(ss.str(), __FILE__, "", __LINE__);      
  }
}
