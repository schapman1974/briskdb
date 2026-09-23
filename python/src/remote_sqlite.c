/* Original BriskDB read-only SQLite virtual-table bridge. No bundled SQLite
 * calls: even allocation and error handling use the loading host's API. */
#include <sqlite3ext.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

static _Atomic(const sqlite3_api_routines *) host_api;
#define sqlite3_api atomic_load(&host_api)
#define MAX_BYTES (8 * 1024 * 1024)
#define MAX_ROWS 4096
#define MAX_COLUMNS 256

typedef struct {
    sqlite3_vtab base;
    sqlite3 *db;
    char *callback;
    int table;
    int columns;
} RemoteTable;

typedef struct {
    sqlite3_vtab_cursor base;
    unsigned char *data;
    size_t size;
    size_t offset;
    uint32_t rows;
    uint32_t row;
} RemoteCursor;

static uint32_t read32(const unsigned char *p) {
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) |
        ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24);
}

static uint64_t read64(const unsigned char *p) {
    return (uint64_t)read32(p) | ((uint64_t)read32(p + 4) << 32);
}

static int fail(RemoteTable *table, const char *message) {
    sqlite3_free(table->base.zErrMsg);
    table->base.zErrMsg = sqlite3_mprintf("%s", message);
    return SQLITE_ERROR;
}

static int callback(RemoteTable *table, int mode, sqlite3_stmt **statement) {
    char *sql = sqlite3_mprintf("SELECT \"%w\"(%d, %d)",
                              table->callback, table->table, mode);
    if (!sql) return SQLITE_NOMEM;
    int rc = sqlite3_prepare_v2(table->db, sql, -1, statement, 0);
    sqlite3_free(sql);
    if (rc == SQLITE_OK) rc = sqlite3_step(*statement);
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(*statement);
        *statement = 0;
        return fail(table, "BriskDB remote callback failed");
    }
    return SQLITE_OK;
}

static int disconnect(sqlite3_vtab *vtab) {
    RemoteTable *table = (RemoteTable *)vtab;
    sqlite3_free(table->callback);
    sqlite3_free(table);
    return SQLITE_OK;
}

static int small_integer(const char *text, int maximum, int *value) {
    if (!*text) return 0;
    unsigned int n = 0;
    for (const char *p = text; *p; ++p) {
        if (*p < '0' || *p > '9') return 0;
        n = n * 10 + (unsigned int)(*p - '0');
        if (n > (unsigned int)maximum) return 0;
    }
    *value = (int)n;
    return 1;
}

static int connect_remote(sqlite3 *db, void *aux, int argc,
                          const char *const *argv, sqlite3_vtab **out, char **error) {
    (void)aux;
    int table_index, columns;
    if (argc != 6 || strlen(argv[3]) != 55 ||
        strncmp(argv[3], "__briskdb_remote_fetch_", 23) != 0 ||
        !small_integer(argv[4], 255, &table_index) ||
        !small_integer(argv[5], MAX_COLUMNS, &columns) || columns == 0) {
        *error = sqlite3_mprintf("use briskdb.attach_remote to create remote tables");
        return SQLITE_ERROR;
    }
    for (const char *p = argv[3] + 23; *p; ++p) {
        if (!((*p >= '0' && *p <= '9') || (*p >= 'a' && *p <= 'f')))
            return SQLITE_ERROR;
    }
    RemoteTable *table = sqlite3_malloc64(sizeof(*table));
    if (!table) return SQLITE_NOMEM;
    memset(table, 0, sizeof(*table));
    table->db = db;
    table->table = table_index;
    table->columns = columns;
    table->callback = sqlite3_mprintf("%s", argv[3]);
    if (!table->callback) { disconnect(&table->base); return SQLITE_NOMEM; }
    sqlite3_stmt *statement = 0;
    int rc = callback(table, 0, &statement);
    if (rc == SQLITE_OK) {
        if (sqlite3_column_type(statement, 0) != SQLITE_TEXT) rc = SQLITE_ERROR;
        else rc = sqlite3_declare_vtab(db, (const char *)sqlite3_column_text(statement, 0));
        sqlite3_finalize(statement);
    }
    if (rc == SQLITE_OK) rc = sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY);
    if (rc != SQLITE_OK) {
        sqlite3_free(table->base.zErrMsg);
        disconnect(&table->base);
        return rc;
    }
    *out = &table->base;
    return SQLITE_OK;
}

static int best_index(sqlite3_vtab *vtab, sqlite3_index_info *info) {
    (void)vtab;
    /* No predicate, ordering, uniqueness or rowid promises. Host SQLite owns
     * all SQL semantics. Pushdown needs separate equivalence proofs. */
    info->estimatedCost = 1000000.0;
    info->estimatedRows = MAX_ROWS;
    return SQLITE_OK;
}

static int open_cursor(sqlite3_vtab *vtab, sqlite3_vtab_cursor **out) {
    (void)vtab;
    RemoteCursor *cursor = sqlite3_malloc64(sizeof(*cursor));
    if (!cursor) return SQLITE_NOMEM;
    memset(cursor, 0, sizeof(*cursor));
    *out = &cursor->base;
    return SQLITE_OK;
}

static int close_cursor(sqlite3_vtab_cursor *base) {
    RemoteCursor *cursor = (RemoteCursor *)base;
    sqlite3_free(cursor->data);
    sqlite3_free(cursor);
    return SQLITE_OK;
}

/* Validate before any cursor access; every length is checked by subtraction. */
static int skip_cell(const unsigned char *data, size_t size, size_t *offset) {
    if (*offset >= size) return 0;
    unsigned char tag = data[(*offset)++];
    size_t length = 0;
    if (tag == 1 || tag == 2) length = 8;
    else if (tag == 3 || tag == 4) {
        if (size - *offset < 4) return 0;
        length = read32(data + *offset);
        *offset += 4;
    } else if (tag != 0) return 0;
    if (length > size - *offset) return 0;
    *offset += length;
    return 1;
}

static int filter(sqlite3_vtab_cursor *base, int index, const char *detail,
                  int argc, sqlite3_value **argv) {
    (void)index; (void)detail; (void)argc; (void)argv;
    RemoteCursor *cursor = (RemoteCursor *)base;
    RemoteTable *table = (RemoteTable *)base->pVtab;
    sqlite3_free(cursor->data);
    cursor->data = 0;
    cursor->rows = cursor->row = 0;
    sqlite3_stmt *statement = 0;
    int rc = callback(table, 1, &statement);
    if (rc != SQLITE_OK) return rc;
    int type = sqlite3_column_type(statement, 0);
    int bytes = sqlite3_column_bytes(statement, 0);
    const unsigned char *data = sqlite3_column_blob(statement, 0);
    if (type != SQLITE_BLOB || !data || bytes < 1 || bytes > MAX_BYTES) {
        sqlite3_finalize(statement);
        return fail(table, "invalid BriskDB remote frame");
    }
    if (data[0] == 'E') {
        /* Error text is deliberately bounded and excludes URLs/tokens. */
        char *message = sqlite3_mprintf("%.*s", bytes < 256 ? bytes - 1 : 255, data + 1);
        sqlite3_finalize(statement);
        if (!message) return SQLITE_NOMEM;
        rc = fail(table, message);
        sqlite3_free(message);
        return rc;
    }
    if (bytes < 12 || memcmp(data, "BRS1", 4) != 0 ||
        read32(data + 4) != (uint32_t)table->columns || read32(data + 8) > MAX_ROWS) {
        sqlite3_finalize(statement);
        return fail(table, "invalid BriskDB remote frame header");
    }
    uint32_t rows = read32(data + 8);
    size_t offset = 12;
    for (uint32_t row = 0; row < rows; ++row) {
        for (int col = 0; col < table->columns; ++col) {
            if (!skip_cell(data, (size_t)bytes, &offset)) {
                sqlite3_finalize(statement);
                return fail(table, "invalid BriskDB remote cell");
            }
        }
    }
    if (offset != (size_t)bytes) {
        sqlite3_finalize(statement);
        return fail(table, "trailing BriskDB remote frame data");
    }
    cursor->data = sqlite3_malloc64((sqlite3_uint64)bytes);
    if (!cursor->data) { sqlite3_finalize(statement); return SQLITE_NOMEM; }
    memcpy(cursor->data, data, (size_t)bytes);
    cursor->size = (size_t)bytes;
    cursor->rows = rows;
    cursor->offset = 12;
    sqlite3_finalize(statement);
    return SQLITE_OK;
}

static int next(sqlite3_vtab_cursor *base) {
    RemoteCursor *cursor = (RemoteCursor *)base;
    RemoteTable *table = (RemoteTable *)base->pVtab;
    if (cursor->row < cursor->rows) {
        for (int col = 0; col < table->columns; ++col)
            if (!skip_cell(cursor->data, cursor->size, &cursor->offset))
                return fail(table, "invalid BriskDB remote cursor");
        ++cursor->row;
    }
    return SQLITE_OK;
}

static int eof(sqlite3_vtab_cursor *base) {
    RemoteCursor *cursor = (RemoteCursor *)base;
    return cursor->row >= cursor->rows;
}

static int column(sqlite3_vtab_cursor *base, sqlite3_context *ctx, int col) {
    RemoteCursor *cursor = (RemoteCursor *)base;
    RemoteTable *table = (RemoteTable *)base->pVtab;
    if (col < 0 || col >= table->columns || cursor->row >= cursor->rows) return SQLITE_ERROR;
    size_t offset = cursor->offset;
    for (int n = 0; n < col; ++n)
        if (!skip_cell(cursor->data, cursor->size, &offset)) return SQLITE_ERROR;
    unsigned char tag = cursor->data[offset++];
    const unsigned char *value = cursor->data + offset;
    if (tag == 0) sqlite3_result_null(ctx);
    else if (tag == 1) {
        uint64_t bits = read64(value);
        sqlite3_int64 integer;
        memcpy(&integer, &bits, 8);
        sqlite3_result_int64(ctx, integer);
    } else if (tag == 2) {
        uint64_t bits = read64(value);
        double real;
        memcpy(&real, &bits, 8);
        sqlite3_result_double(ctx, real);
    } else if (tag == 3) sqlite3_result_text(ctx, (const char *)value + 4, (int)read32(value), SQLITE_TRANSIENT);
    else if (tag == 4) sqlite3_result_blob(ctx, value + 4, (int)read32(value), SQLITE_TRANSIENT);
    return SQLITE_OK;
}

static int rowid(sqlite3_vtab_cursor *base, sqlite3_int64 *out) {
    (void)out;
    return fail((RemoteTable *)base->pVtab,
                "BriskDB remote rowid is unsupported; select an explicit key column");
}

static const sqlite3_module module = {
    .iVersion = 2, .xCreate = connect_remote, .xConnect = connect_remote,
    .xBestIndex = best_index, .xDisconnect = disconnect, .xDestroy = disconnect,
    .xOpen = open_cursor, .xClose = close_cursor, .xFilter = filter,
    .xNext = next, .xEof = eof, .xColumn = column, .xRowid = rowid,
};

int briskdb_remote_init(sqlite3 *db, char **error, const sqlite3_api_routines *api) {
    const sqlite3_api_routines *expected = 0;
    if (api->libversion_number() < 3031000) {
        *error = api->mprintf("BriskDB remote requires host SQLite 3.31 or newer");
        return SQLITE_ERROR;
    }
    if (!atomic_compare_exchange_strong(&host_api, &expected, api) && expected != api) {
        *error = api->mprintf("BriskDB remote cannot mix different host SQLite libraries");
        return SQLITE_ERROR;
    }
    return sqlite3_create_module_v2(db, "briskdb_remote", &module, 0, 0);
}
