#include "jonoondb_api/indexer_factory.h"
#include <sstream>
#include "jonoondb_api/enums.h"
#include "jonoondb_api/ewah_compressed_bitmap_indexer_blob.h"
#include "jonoondb_api/ewah_compressed_bitmap_indexer_double.h"
#include "jonoondb_api/ewah_compressed_bitmap_indexer_integer.h"
#include "jonoondb_api/ewah_compressed_bitmap_indexer_string.h"
#include "jonoondb_api/index_info_impl.h"
#include "jonoondb_api/jonoondb_exceptions.h"
#include "jonoondb_api/vector_blob_indexer.h"
#include "jonoondb_api/vector_double_indexer.h"
#include "jonoondb_api/vector_integer_indexer.h"
#include "jonoondb_api/vector_string_indexer.h"

using namespace std;
using namespace jonoondb_api;

Indexer* IndexerFactory::CreateIndexer(const IndexInfoImpl& indexInfo,
                                       const FieldType& fieldType) {
  switch (indexInfo.GetType()) {
    case IndexType::INVERTED_COMPRESSED_BITMAP: {
      if (fieldType == FieldType::DOUBLE || fieldType == FieldType::FLOAT) {
        EWAHCompressedBitmapIndexerDouble* ewahIndexer;
        EWAHCompressedBitmapIndexerDouble::Construct(indexInfo, fieldType,
                                                     ewahIndexer);
        return static_cast<Indexer*>(ewahIndexer);
      } else if (fieldType == FieldType::STRING) {
        EWAHCompressedBitmapIndexerString* ewahIndexer;
        EWAHCompressedBitmapIndexerString::Construct(indexInfo, fieldType,
                                                     ewahIndexer);
        return static_cast<Indexer*>(ewahIndexer);
      } else if (fieldType == FieldType::BLOB) {
        EWAHCompressedBitmapIndexerBlob* ewahIndexer;
        EWAHCompressedBitmapIndexerBlob::Construct(indexInfo, fieldType,
                                                   ewahIndexer);
        return static_cast<Indexer*>(ewahIndexer);
      } else {
        EWAHCompressedBitmapIndexerInteger* ewahIndexer;
        EWAHCompressedBitmapIndexerInteger::Construct(indexInfo, fieldType,
                                                      ewahIndexer);
        return static_cast<Indexer*>(ewahIndexer);
      }
    }
    case IndexType::VECTOR: {
      if (fieldType == FieldType::STRING) {
        return new VectorStringIndexer(indexInfo, fieldType);
      } else if (fieldType == FieldType::DOUBLE ||
                 fieldType == FieldType::FLOAT) {
        return new VectorDoubleIndexer(indexInfo, fieldType);
      } else if (fieldType == FieldType::INT64) {
        return new VectorIntegerIndexer<std::int64_t>(indexInfo, fieldType);
      } else if (fieldType == FieldType::BLOB) {
        return new VectorBlobIndexer(indexInfo, fieldType);
      } else {
        return new VectorIntegerIndexer<std::int32_t>(indexInfo, fieldType);
      }
    }

    default:
      std::ostringstream ss;
      ss << "Cannot create Indexer. Index type '"
         << static_cast<int32_t>(indexInfo.GetType()) << "' is unknown.";
      throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
  }
}
