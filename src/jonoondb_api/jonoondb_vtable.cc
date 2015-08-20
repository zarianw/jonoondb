#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
;

#include <stdlib.h>
#include <stdio.h>

typedef struct jonoondb_vtab_s {
  sqlite3_vtab vtab;
} jonoondb_vtab;
typedef struct jonoondb_cursor_s {
  sqlite3_vtab_cursor cur;
  sqlite_int64 row;
} jonoondb_cursor;

static int jonoondb_create(sqlite3 *db, void *udp, int argc,
                           const char * const *argv, sqlite3_vtab **vtab,
                           char **errmsg) {
  int i;
  printf("CREATE:\n");
  for (i = 0; i < argc; i++) {
    printf("   %2d: %s\n", i, argv[i]);
  }

  jonoondb_vtab *v = NULL;

  v = (jonoondb_vtab*) sqlite3_malloc(sizeof(jonoondb_vtab));

  if (v == NULL)
    return SQLITE_NOMEM;
  v->vtab.zErrMsg = NULL;

  sqlite3_declare_vtab(db, argv[argc - 1]);
  *vtab = (sqlite3_vtab*) v;
  return SQLITE_OK;
}

static int jonoondb_connect(sqlite3 *db, void *udp, int argc,
                            const char * const *argv, sqlite3_vtab **vtab,
                            char **errmsg) {
  int i;
  printf("CONNECT:\n");
  for (i = 0; i < argc; i++) {
    printf("   %2d: %s\n", i, argv[i]);
  }

  jonoondb_vtab *v = NULL;

  v = (jonoondb_vtab*) sqlite3_malloc(sizeof(jonoondb_vtab));

  if (v == NULL)
    return SQLITE_NOMEM;
  v->vtab.zErrMsg = NULL;

  sqlite3_declare_vtab(db, argv[argc - 1]);
  *vtab = (sqlite3_vtab*) v;
  return SQLITE_OK;
}

static int jonoondb_disconnect(sqlite3_vtab *vtab) {
  printf("DISCONNECT\n");

  sqlite3_free(vtab);
  return SQLITE_OK;
}

static int jonoondb_destroy(sqlite3_vtab *vtab) {
  printf("DESTROY\n");

  sqlite3_free(vtab);
  return SQLITE_OK;
}

static char* op(unsigned char op) {
  if (op == SQLITE_INDEX_CONSTRAINT_EQ)
    return "=";
  if (op == SQLITE_INDEX_CONSTRAINT_GT)
    return ">";
  if (op == SQLITE_INDEX_CONSTRAINT_LE)
    return "<=";
  if (op == SQLITE_INDEX_CONSTRAINT_LT)
    return "<";
  if (op == SQLITE_INDEX_CONSTRAINT_GE)
    return ">=";
  if (op == SQLITE_INDEX_CONSTRAINT_MATCH)
    return "MATCH";
  return "?";
}

static int jonoondb_bestindex(sqlite3_vtab *vtab, sqlite3_index_info *info) {
  int i;
  printf("BEST INDEX:\n");
  for (i = 0; i < info->nConstraint; i++) {
    printf("   CONST[%d]: %d %s %s\n", i, info->aConstraint[i].iColumn,
           op(info->aConstraint[i].op),
           info->aConstraint[i].usable ? "Usable" : "Unusable");
  }
  for (i = 0; i < info->nOrderBy; i++) {
    printf("   ORDER[%d]: %d %s\n", i, info->aOrderBy[i].iColumn,
           info->aOrderBy[i].desc ? "DESC" : "ASC");
  }

  return SQLITE_OK;
}

static int jonoondb_open(sqlite3_vtab *vtab, sqlite3_vtab_cursor **cur) {
  jonoondb_vtab *v = (jonoondb_vtab*) vtab;
  jonoondb_cursor *c;

  printf("OPEN\n");

  c = (jonoondb_cursor*) sqlite3_malloc(sizeof(jonoondb_cursor));
  if (c == NULL)
    return SQLITE_NOMEM;

  *cur = (sqlite3_vtab_cursor*) c;
  c->row = 0;
  return SQLITE_OK;
}

static int jonoondb_close(sqlite3_vtab_cursor *cur) {
  printf("CLOSE\n");

  sqlite3_free(cur);
  return SQLITE_OK;
}

static int jonoondb_filter(sqlite3_vtab_cursor *cur, int idxnum,
                           const char *idxstr, int argc,
                           sqlite3_value **value) {
  printf("FILTER\n");

  return SQLITE_OK;
}

static int jonoondb_next(sqlite3_vtab_cursor *cur) {
  printf("NEXT\n");

  ((jonoondb_cursor*) cur)->row++;
  return SQLITE_OK;
}

static int jonoondb_eof(sqlite3_vtab_cursor *cur) {
  printf("EOF\n");

  return (((jonoondb_cursor*) cur)->row >= 10);
}

static int jonoondb_rowid(sqlite3_vtab_cursor *cur, sqlite3_int64 *rowid) {
  printf("ROWID: %lld\n", ((jonoondb_cursor*) cur)->row);

  *rowid = ((jonoondb_cursor*) cur)->row;
  return SQLITE_OK;
}

static int jonoondb_column(sqlite3_vtab_cursor *cur, sqlite3_context *ctx,
                           int cidx) {
  jonoondb_cursor *c = (jonoondb_cursor*) cur;

  printf("COLUMN: %d\n", cidx);

  sqlite3_result_int(ctx, c->row * 10 + cidx);
  return SQLITE_OK;
}

static int jonoondb_rename(sqlite3_vtab *vtab, const char *newname) {
  printf("RENAME\n");

  return SQLITE_OK;
}

static int jonoondb_update(sqlite3_vtab *vtab, int argc, sqlite3_value **argv,
                           sqlite_int64 *rowid) {
  printf("UPDATE: ");

  if (argc == 1) {
    printf("DELETE %d\n", sqlite3_value_int(argv[0]));

    return SQLITE_OK;
  }

  if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
    if (sqlite3_value_type(argv[1]) == SQLITE_NULL) {
      printf("INSERT -\n");

      *rowid = 10;
    } else {
      printf("INSERT %d\n", sqlite3_value_int(argv[1]));
    }
    return SQLITE_OK;
  }

  printf("UPDATE %d %d\n", sqlite3_value_int(argv[0]),
         sqlite3_value_int(argv[1]));
  return SQLITE_OK;
}

static int jonoondb_begin(sqlite3_vtab *vtab) {
  printf("BEGIN\n");

  return SQLITE_OK;
}

static int jonoondb_sync(sqlite3_vtab *vtab) {
  printf("SYNC\n");

  return SQLITE_OK;
}

static int jonoondb_commit(sqlite3_vtab *vtab) {
  printf("COMMIT\n");

  return SQLITE_OK;
}

static int jonoondb_rollback(sqlite3_vtab *vtab) {
  printf("ROLLBACK\n");

  return SQLITE_OK;
}

static int jonoondb_xFindFunction(
    sqlite3_vtab *pVtab, /* Virtual table handle */
    int nArg, /* Number of SQL function arguments */
    const char *zName, /* Name of SQL function */
    void (**pxFunc)(sqlite3_context*, int, sqlite3_value**), /* OUT: Result */
    void **ppArg /* Unused */
    ) {
}

static sqlite3_module jonoondb_mod = { 1, /* iVersion        */
jonoondb_create, /* xCreate()       */
jonoondb_connect, /* xConnect()      */
jonoondb_bestindex, /* xBestIndex()    */
jonoondb_disconnect, /* xDisconnect()   */
jonoondb_destroy, /* xDestroy()      */
jonoondb_open, /* xOpen()         */
jonoondb_close, /* xClose()        */
jonoondb_filter, /* xFilter()       */
jonoondb_next, /* xNext()         */
jonoondb_eof, /* xEof()          */
jonoondb_column, /* xColumn()       */
jonoondb_rowid, /* xRowid()        */
jonoondb_update, /* xUpdate()       */
jonoondb_begin, /* xBegin()        */
jonoondb_sync, /* xSync()         */
jonoondb_commit, /* xCommit()       */
jonoondb_rollback, /* xRollback()     */
NULL,/* xFindFunction() */
jonoondb_rename /* xRename()       */
};

int jonoondb_vtable_init(sqlite3 *db, char **error,
                         const sqlite3_api_routines *api) {
  SQLITE_EXTENSION_INIT2(api);
  return sqlite3_create_module(db, "stub", &jonoondb_mod, NULL);
}
