#include "document_id_generator.h"

using namespace jonoondb_api;
using namespace std;

DocumentIDGenerator::DocumentIDGenerator()
    : m_currentID(0) {
}

std::uint64_t DocumentIDGenerator::ReserveID(uint32_t numOfIDsToReserve) {
  return m_currentID.fetch_add(numOfIDsToReserve);
}
