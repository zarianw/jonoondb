#pragma once

#include <cstddef>

namespace jonoondb_api {
struct WriteOptionsImpl {
  WriteOptionsImpl() : compress(false), verifyDocuments(true) {}
  WriteOptionsImpl(bool comp, bool verify) :
    compress(comp), verifyDocuments(verify) {}
  bool compress;
  bool verifyDocuments;
};
}  // namespace jonoondb_api
