#include "document_id_generator.h"

using namespace jonoondb_api;
using namespace std;

DocumentIDGenerator::DocumentIDGenerator() : m_currentID(DOC_ID_START) {}

std::uint64_t DocumentIDGenerator::ReserveID(std::uint64_t numOfIDsToReserve) {
  return m_currentID.fetch_add(numOfIDsToReserve);
}
