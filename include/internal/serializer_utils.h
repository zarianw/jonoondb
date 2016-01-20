#pragma once

#include "flatbuffers/flatbuffers.h"
#include "index_info_impl.h"
#include "index_info_fb_generated.h"
#include "buffer_impl.h"

namespace jonoondb_api {
class SerializerUtils {
public:
  static void IndexInfoToBytes(const IndexInfoImpl& indexInfo, BufferImpl& buffer) {
    flatbuffers::FlatBufferBuilder fbb;
    auto name = fbb.CreateString(indexInfo.GetIndexName());
    auto mloc = CreateIndexInfoFB(fbb, name, 0, indexInfo.GetIsAscending(),
      (int16_t)indexInfo.GetType());
    fbb.Finish(mloc);
    auto size = fbb.GetSize();
    if (size > buffer.GetCapacity()) {
      buffer.Resize(size);
    }

    buffer.Copy((char*)fbb.GetBufferPointer(), size);
  }  
};
}  // jonoondb_api
