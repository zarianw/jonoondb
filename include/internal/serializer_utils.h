#pragma once

#include "flatbuffers/flatbuffers.h"
#include "index_info.h"
#include "index_info_fb_generated.h"
#include "buffer.h"

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

  static Status IndexInfoFromBytes(const BufferImpl& buffer, IndexInfoImpl& indexInfo) {
    auto ptr = GetIndexInfoFB(0);
  }
};
}  // jonoondb_api
