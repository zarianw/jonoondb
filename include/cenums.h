#pragma once

#ifdef __cplusplus
extern "C" {
#endif

typedef enum jonoondb_status_codes {
  status_genericerrorcode = 1,
  status_invalidargumentcode = 2,
  status_missingdatabasefilecode = 3,
  status_missingdatabasefoldercode = 4,
  status_outofmemoryerrorcode = 5,
  status_duplicatekeyerrorcode = 6,  
  status_collectionalreadyexistcode = 7,
  status_indexalreadyexistcode = 8,
  status_collectionnotfoundcode = 9,
  status_schemaparseerrorcode = 10,
  status_indexoutofbounderrorcode = 11,
  status_sqlerrorcode = 12,
} jonoondb_status_codes;

#ifdef __cplusplus
} // extern "C"
#endif