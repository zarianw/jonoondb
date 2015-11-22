#include <sstream>
#include "indexer_factory.h"
#include "status.h"
#include "index_info_impl.h"
#include "enums.h"
#include "ewah_compressed_bitmap_indexer_integer.h"
#include "ewah_compressed_bitmap_indexer_string.h"
#include "ewah_compressed_bitmap_indexer_double.h"
#include "jonoondb_exceptions.h"

using namespace std;
using namespace jonoondb_api;

Indexer* IndexerFactory::CreateIndexer(
  const IndexInfoImpl& indexInfo,
  const FieldType& fieldType) {
  Status sts;
  switch (indexInfo.GetType()) {
    case IndexType::EWAHCompressedBitmap: {
      if (fieldType == FieldType::BASE_TYPE_DOUBLE ||
        fieldType == FieldType::BASE_TYPE_FLOAT32) {
        EWAHCompressedBitmapIndexerDouble* ewahIndexer;
        EWAHCompressedBitmapIndexerDouble::Construct(indexInfo, fieldType, ewahIndexer);
        return static_cast<Indexer*>(ewahIndexer);
      } else if (fieldType == FieldType::BASE_TYPE_STRING) {
        EWAHCompressedBitmapIndexerString* ewahIndexer;
        EWAHCompressedBitmapIndexerString::Construct(indexInfo, fieldType, ewahIndexer);
        return static_cast<Indexer*>(ewahIndexer);
      } else {
        EWAHCompressedBitmapIndexerInteger* ewahIndexer;
        EWAHCompressedBitmapIndexerInteger::Construct(indexInfo, fieldType, ewahIndexer);
        return static_cast<Indexer*>(ewahIndexer);
      }
    }
    default:
      std::ostringstream ss;
      ss << "Cannot create Indexer. Index type '" << static_cast<int32_t>(indexInfo.GetType()) << "' is unknown.";
      throw JonoonDBException(ss.str(), __FILE__, "", __LINE__);
  }
}
