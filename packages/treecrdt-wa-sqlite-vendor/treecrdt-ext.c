// TreeCRDT SQLite extension glue for wa-sqlite.
// This file is kept out-of-tree from wa-sqlite upstream and is compiled in via
// Makefile variables (CFILES_EXTRA/VPATH_EXTRA) and linked with the TreeCRDT
// static library.

#include <sqlite3.h>

// The Rust extension entrypoint (static-link build ignores the sqlite3_api_routines pointer).
extern int sqlite3_treecrdt_init(sqlite3 *db, char **pzErrMsg, const void *pApi);

__attribute__((used, constructor)) static void treecrdt_register_auto(void) {
  // wa-sqlite builds SQLite with SQLITE_OMIT_AUTOINIT, so ensure initialization.
  sqlite3_initialize();

  // SQLite calls the registered function with (db, err, api); cast to silence
  // the prototype mismatch on platforms that declare xEntryPoint as void(*)(void).
  sqlite3_auto_extension((void (*)(void))sqlite3_treecrdt_init);
}
