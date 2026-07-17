// TreeCRDT SQLite extension glue for wa-sqlite.
// This file is kept out-of-tree from wa-sqlite upstream and is compiled in via
// Makefile variables (CFILES_EXTRA/VPATH_EXTRA) and linked with the TreeCRDT
// static library.

#include <sqlite3.h>
#include <emscripten/emscripten.h>

// The Rust extension entrypoint (static-link build ignores the sqlite3_api_routines pointer).
extern int sqlite3_treecrdt_init(sqlite3 *db, char **pzErrMsg, const void *pApi);

EMSCRIPTEN_KEEPALIVE
int treecrdt_sqlite_init(sqlite3 *db) {
  return sqlite3_treecrdt_init(db, 0, 0);
}
