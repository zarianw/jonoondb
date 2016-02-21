#include <sstream>
#include "indexer_factory.h"
#include "index_info_impl.h"
#include "enums.h"
#include "ewah_compressed_bitmap_indexer_integer.h"
#include "ewah_compressed_bitmap_indexer_string.h"
#include "ewah_compressed_bitmap_indexer_double.h"
#include "jonoondb_exceptions.h"
#include "vector_integer_indexer.h"
#include "vector_double_indexer.h"

using namespace std;
using namespace jonoondb_api;

Indexer* IndexerFactory::CreateIndexer(
  const IndexInfoImpl& indexInfo,
  const FieldType& fieldType) {
  switch (indexInfo.GetType()) {
    case IndexType::EWAH_COMPRESSED_BITMAP: {
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
    case IndexType::VECTOR: {
      if (fieldType == FieldType::BASE_TYPE_STRING) {
        throw JonoonDBException("VECTOR indexer is not yet supported for field of type string.",
                                __FILE__, __func__, __LINE__);
      } else if (fieldType == FieldType::BASE_TYPE_DOUBLE ||
                 fieldType == FieldType::BASE_TYPE_FLOAT32) {
        return new VectorDoubleIndexer(indexInfo, fieldType);
      } else {
        return new VectorIntegerIndexer(indexInfo, fieldType);
      }
    }

    default:
      std::ostringstream ss;
      ss << "Cannot create Indexer. Index type '" << static_cast<int32_t>(indexInfo.GetType()) << "' is unknown.";
      throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }
}
