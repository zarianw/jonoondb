#pragma once

#include <string>
#include "flatbuffers/flatbuffers.h"
#include "status.h"
#include "index_info.h"
#include "index_info_fb_generated.h"
#include "buffer.h"

using namespace std;
using namespace flatbuffers;

namespace jonoondb_api {
class SerializerUtils {
 public:

  static Status IndexInfoToBytes(const IndexInfo& indexInfo, Buffer& buffer) {
    try {
      FlatBufferBuilder fbb;
      auto name = fbb.CreateString(indexInfo.GetName());
      auto mloc = CreateIndexInfoFB(fbb, name, 0, indexInfo.GetIsAscending(),
                                    (int16_t) indexInfo.GetType());
      fbb.Finish(mloc);
      auto size = fbb.GetSize();
      if (size > buffer.GetCapacity()) {
        auto status = buffer.Resize(size);
        if (!status.OK()) {
          return status;
        }
      }

      auto status = buffer.Copy((char*) fbb.GetBufferPointer(), size);
      if (!status.OK()) {
        return status;
      }
    } catch (exception& ex) {
      string errorMessage(ex.what());
      return Status(kStatusGenericErrorCode, errorMessage.c_str(),
                    __FILE__, "", __LINE__);
    }

    return Status();
  }

  static Status IndexInfoFromBytes(const Buffer& buffer, IndexInfo& indexInfo) {
    auto ptr = GetIndexInfoFB(0);

  }
};
}  // jonoondb_api
