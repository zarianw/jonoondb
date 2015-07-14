#pragma once

#include <cstdint>
#include <atomic>

namespace jonoondb_api {
class DocumentIDGenerator {
 public:   
   DocumentIDGenerator();
   DocumentIDGenerator(const DocumentIDGenerator&) = delete;
   DocumentIDGenerator(DocumentIDGenerator&&) = delete;
   DocumentIDGenerator& operator=(const DocumentIDGenerator&) = delete;
   std::uint64_t ReserveID(std::uint32_t numOfIDsToReserve);
private:
  std::atomic<std::uint64_t> m_currentID;
};
} // jonoondb_api