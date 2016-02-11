#pragma once

#include "flatbuffers/flatbuffers.h"
#include "index_info_impl.h"
#include "index_info_fb_generated.h"
#include "buffer_impl.h"
#include "enums.h"

namespace jonoondb_api {
class SerializerUtils {
public:
  static void IndexInfoToBytes(const IndexInfoImpl& indexInfo, BufferImpl& buffer) {
    flatbuffers::FlatBufferBuilder fbb;
    auto name = fbb.CreateString(indexInfo.GetIndexName());
    auto columnName = fbb.CreateString(indexInfo.GetColumnName());
    auto mloc = CreateIndexInfoFB(fbb, name, columnName, indexInfo.GetIsAscending(),
                                  static_cast<std::int32_t>(indexInfo.GetType()));
    fbb.Finish(mloc);
    auto size = fbb.GetSize();
    if (size > buffer.GetCapacity()) {
      buffer.Resize(size);
    }

    buffer.Copy((char*)fbb.GetBufferPointer(), size);
  }

  static IndexInfoImpl BytesToIndexInfo(const BufferImpl& buffer) {
    auto fbObj = flatbuffers::GetRoot<IndexInfoFB>(buffer.GetData());
    auto indexName = std::string(fbObj->name()->c_str(), fbObj->name()->size());
    auto columnName = std::string(fbObj->column()->c_str(), fbObj->name()->size());

    return IndexInfoImpl(std::move(indexName), static_cast<IndexType>(fbObj->type()),
                         std::move(columnName), fbObj->is_ascending());
  }
};
}  // jonoondb_api
