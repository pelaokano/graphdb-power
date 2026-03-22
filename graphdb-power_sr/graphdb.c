#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "graphdb.h"

/* ------------------------------------------------------------------ */
/*  Internal helpers                                                    */
/* ------------------------------------------------------------------ */

static void set_error(Graph *g, const char *msg)
{
    strncpy(g->last_error, msg, MAX_ERROR_LEN - 1);
    g->last_error[MAX_ERROR_LEN - 1] = '\0';
}

static int exec_sql(Graph *g, const char *sql)
{
    char *errmsg = NULL;
    int rc = sqlite3_exec(g->db, sql, NULL, NULL, &errmsg);
    if (rc != SQLITE_OK) {
        set_error(g, errmsg ? errmsg : "unknown sqlite error");
        sqlite3_free(errmsg);
        return GRAPHDB_ERR;
    }
    return GRAPHDB_OK;
}

static int init_schema(Graph *g)
{
    const char *ddl =
        "PRAGMA journal_mode=WAL;"
        "PRAGMA foreign_keys=ON;"
        "CREATE TABLE IF NOT EXISTS nodes ("
        "    id         INTEGER PRIMARY KEY AUTOINCREMENT,"
        "    labels     TEXT    NOT NULL DEFAULT '',"
        "    properties TEXT    NOT NULL DEFAULT '{}'"
        ");"
        "CREATE TABLE IF NOT EXISTS relationships ("
        "    id         INTEGER PRIMARY KEY AUTOINCREMENT,"
        "    src_id     INTEGER NOT NULL,"
        "    dst_id     INTEGER NOT NULL,"
        "    type       TEXT    NOT NULL,"
        "    properties TEXT    NOT NULL DEFAULT '{}',"
        "    FOREIGN KEY (src_id) REFERENCES nodes(id),"
        "    FOREIGN KEY (dst_id) REFERENCES nodes(id)"
        ");"
        "CREATE TABLE IF NOT EXISTS graph_meta ("
        "    key   TEXT PRIMARY KEY,"
        "    value TEXT"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_node_labels ON nodes(labels);"
        "CREATE INDEX IF NOT EXISTS idx_rel_type    ON relationships(type);"
        "CREATE INDEX IF NOT EXISTS idx_rel_src     ON relationships(src_id);"
        "CREATE INDEX IF NOT EXISTS idx_rel_dst     ON relationships(dst_id);"
        "INSERT OR IGNORE INTO graph_meta(key,value) VALUES('version','0.1.0');";

    return exec_sql(g, ddl);
}

/* ------------------------------------------------------------------ */
/*  Graph lifecycle                                                     */
/* ------------------------------------------------------------------ */

Graph *graph_open(const char *path)
{
    Graph *g = calloc(1, sizeof(Graph));
    if (!g) return NULL;

    strncpy(g->path, path, sizeof(g->path) - 1);

    int rc = sqlite3_open(path, &g->db);
    if (rc != SQLITE_OK) {
        set_error(g, sqlite3_errmsg(g->db));
        sqlite3_close(g->db);
        free(g);
        return NULL;
    }

    if (init_schema(g) != GRAPHDB_OK) {
        sqlite3_close(g->db);
        free(g);
        return NULL;
    }

    return g;
}

void graph_close(Graph *g)
{
    if (!g) return;
    sqlite3_close(g->db);
    free(g);
}

const char *graph_last_error(Graph *g)
{
    return g ? g->last_error : "null graph handle";
}

/* ------------------------------------------------------------------ */
/*  Node operations                                                     */
/* ------------------------------------------------------------------ */

int graph_create_node(Graph *g, const char *labels, const char *props_json,
                      int64_t *out_id)
{
    const char *sql =
        "INSERT INTO nodes(labels, properties) VALUES(?, ?)";
    sqlite3_stmt *stmt;

    if (sqlite3_prepare_v2(g->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        set_error(g, sqlite3_errmsg(g->db));
        return GRAPHDB_ERR;
    }

    sqlite3_bind_text(stmt, 1, labels ? labels : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, props_json ? props_json : "{}", -1, SQLITE_STATIC);

    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        set_error(g, sqlite3_errmsg(g->db));
        return GRAPHDB_ERR;
    }

    if (out_id) *out_id = sqlite3_last_insert_rowid(g->db);
    return GRAPHDB_OK;
}

int graph_get_node(Graph *g, int64_t id, Node *out)
{
    const char *sql =
        "SELECT id, labels, properties FROM nodes WHERE id = ?";
    sqlite3_stmt *stmt;

    if (sqlite3_prepare_v2(g->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        set_error(g, sqlite3_errmsg(g->db));
        return GRAPHDB_ERR;
    }

    sqlite3_bind_int64(stmt, 1, id);
    int rc = sqlite3_step(stmt);

    if (rc == SQLITE_ROW) {
        out->id = sqlite3_column_int64(stmt, 0);
        strncpy(out->labels,
                (const char *)sqlite3_column_text(stmt, 1),
                MAX_LABEL_LEN - 1);
        strncpy(out->properties,
                (const char *)sqlite3_column_text(stmt, 2),
                MAX_PROP_LEN - 1);
        sqlite3_finalize(stmt);
        return GRAPHDB_OK;
    }

    sqlite3_finalize(stmt);
    return GRAPHDB_NOT_FOUND;
}

int graph_set_node_property(Graph *g, int64_t id, const char *key,
                            const char *value_json)
{
    const char *sql =
        "UPDATE nodes SET properties = json_set(properties, ?, json(?)) "
        "WHERE id = ?";
    sqlite3_stmt *stmt;

    char path[MAX_LABEL_LEN];
    snprintf(path, sizeof(path), "$.%s", key);

    if (sqlite3_prepare_v2(g->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        set_error(g, sqlite3_errmsg(g->db));
        return GRAPHDB_ERR;
    }

    sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, value_json, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 3, id);

    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        set_error(g, sqlite3_errmsg(g->db));
        return GRAPHDB_ERR;
    }
    return GRAPHDB_OK;
}

int graph_remove_node_property(Graph *g, int64_t id, const char *key)
{
    const char *sql =
        "UPDATE nodes SET properties = json_remove(properties, ?) WHERE id = ?";
    sqlite3_stmt *stmt;

    char path[MAX_LABEL_LEN];
    snprintf(path, sizeof(path), "$.%s", key);

    if (sqlite3_prepare_v2(g->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        set_error(g, sqlite3_errmsg(g->db));
        return GRAPHDB_ERR;
    }

    sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 2, id);

    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    return (rc == SQLITE_DONE) ? GRAPHDB_OK : GRAPHDB_ERR;
}

int graph_add_label(Graph *g, int64_t id, const char *label)
{
    /* labels stored as space-separated string: "Person Employee" */
    const char *sql =
        "UPDATE nodes SET labels = CASE "
        "  WHEN labels = '' THEN ? "
        "  ELSE labels || ' ' || ? "
        "END WHERE id = ? AND instr(labels, ?) = 0";
    sqlite3_stmt *stmt;

    if (sqlite3_prepare_v2(g->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        set_error(g, sqlite3_errmsg(g->db));
        return GRAPHDB_ERR;
    }

    sqlite3_bind_text(stmt, 1, label, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, label, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 3, id);
    sqlite3_bind_text(stmt, 4, label, -1, SQLITE_STATIC);

    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    return (rc == SQLITE_DONE) ? GRAPHDB_OK : GRAPHDB_ERR;
}

int graph_remove_label(Graph *g, int64_t id, const char *label)
{
    /* trim the label from space-separated string */
    const char *sql =
        "UPDATE nodes SET labels = trim(replace(' ' || labels || ' ', "
        "' ' || ? || ' ', ' ')) WHERE id = ?";
    sqlite3_stmt *stmt;

    if (sqlite3_prepare_v2(g->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        set_error(g, sqlite3_errmsg(g->db));
        return GRAPHDB_ERR;
    }

    sqlite3_bind_text(stmt, 1, label, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 2, id);

    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    return (rc == SQLITE_DONE) ? GRAPHDB_OK : GRAPHDB_ERR;
}

int graph_delete_node(Graph *g, int64_t id, int detach)
{
    if (detach) {
        const char *del_rels =
            "DELETE FROM relationships WHERE src_id = ? OR dst_id = ?";
        sqlite3_stmt *stmt;
        if (sqlite3_prepare_v2(g->db, del_rels, -1, &stmt, NULL) != SQLITE_OK) {
            set_error(g, sqlite3_errmsg(g->db));
            return GRAPHDB_ERR;
        }
        sqlite3_bind_int64(stmt, 1, id);
        sqlite3_bind_int64(stmt, 2, id);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

    const char *sql = "DELETE FROM nodes WHERE id = ?";
    sqlite3_stmt *stmt;

    if (sqlite3_prepare_v2(g->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        set_error(g, sqlite3_errmsg(g->db));
        return GRAPHDB_ERR;
    }

    sqlite3_bind_int64(stmt, 1, id);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    return (rc == SQLITE_DONE) ? GRAPHDB_OK : GRAPHDB_ERR;
}

/* ------------------------------------------------------------------ */
/*  Relationship operations                                             */
/* ------------------------------------------------------------------ */

int graph_create_rel(Graph *g, int64_t src, int64_t dst, const char *type,
                     const char *props_json, int64_t *out_id)
{
    const char *sql =
        "INSERT INTO relationships(src_id, dst_id, type, properties) "
        "VALUES(?, ?, ?, ?)";
    sqlite3_stmt *stmt;

    if (sqlite3_prepare_v2(g->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        set_error(g, sqlite3_errmsg(g->db));
        return GRAPHDB_ERR;
    }

    sqlite3_bind_int64(stmt, 1, src);
    sqlite3_bind_int64(stmt, 2, dst);
    sqlite3_bind_text(stmt, 3, type, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, props_json ? props_json : "{}", -1, SQLITE_STATIC);

    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    if (rc != SQLITE_DONE) {
        set_error(g, sqlite3_errmsg(g->db));
        return GRAPHDB_ERR;
    }

    if (out_id) *out_id = sqlite3_last_insert_rowid(g->db);
    return GRAPHDB_OK;
}

int graph_get_rel(Graph *g, int64_t id, Relationship *out)
{
    const char *sql =
        "SELECT id, src_id, dst_id, type, properties "
        "FROM relationships WHERE id = ?";
    sqlite3_stmt *stmt;

    if (sqlite3_prepare_v2(g->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        set_error(g, sqlite3_errmsg(g->db));
        return GRAPHDB_ERR;
    }

    sqlite3_bind_int64(stmt, 1, id);
    int rc = sqlite3_step(stmt);

    if (rc == SQLITE_ROW) {
        out->id     = sqlite3_column_int64(stmt, 0);
        out->src_id = sqlite3_column_int64(stmt, 1);
        out->dst_id = sqlite3_column_int64(stmt, 2);
        strncpy(out->type,
                (const char *)sqlite3_column_text(stmt, 3),
                MAX_TYPE_LEN - 1);
        strncpy(out->properties,
                (const char *)sqlite3_column_text(stmt, 4),
                MAX_PROP_LEN - 1);
        sqlite3_finalize(stmt);
        return GRAPHDB_OK;
    }

    sqlite3_finalize(stmt);
    return GRAPHDB_NOT_FOUND;
}

int graph_set_rel_property(Graph *g, int64_t id, const char *key,
                           const char *value_json)
{
    const char *sql =
        "UPDATE relationships SET properties = json_set(properties, ?, json(?)) "
        "WHERE id = ?";
    sqlite3_stmt *stmt;

    char path[MAX_LABEL_LEN];
    snprintf(path, sizeof(path), "$.%s", key);

    if (sqlite3_prepare_v2(g->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        set_error(g, sqlite3_errmsg(g->db));
        return GRAPHDB_ERR;
    }

    sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, value_json, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 3, id);

    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    return (rc == SQLITE_DONE) ? GRAPHDB_OK : GRAPHDB_ERR;
}

int graph_remove_rel_property(Graph *g, int64_t id, const char *key)
{
    const char *sql =
        "UPDATE relationships SET properties = json_remove(properties, ?) "
        "WHERE id = ?";
    sqlite3_stmt *stmt;

    char path[MAX_LABEL_LEN];
    snprintf(path, sizeof(path), "$.%s", key);

    if (sqlite3_prepare_v2(g->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        set_error(g, sqlite3_errmsg(g->db));
        return GRAPHDB_ERR;
    }

    sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt, 2, id);

    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    return (rc == SQLITE_DONE) ? GRAPHDB_OK : GRAPHDB_ERR;
}

int graph_delete_rel(Graph *g, int64_t id)
{
    const char *sql = "DELETE FROM relationships WHERE id = ?";
    sqlite3_stmt *stmt;

    if (sqlite3_prepare_v2(g->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        set_error(g, sqlite3_errmsg(g->db));
        return GRAPHDB_ERR;
    }

    sqlite3_bind_int64(stmt, 1, id);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);

    return (rc == SQLITE_DONE) ? GRAPHDB_OK : GRAPHDB_ERR;
}

/* ------------------------------------------------------------------ */
/*  Merge                                                               */
/* ------------------------------------------------------------------ */

int graph_merge_node(Graph *g, const char *labels, const char *match_props_json,
                     const char *on_create_json, const char *on_match_json,
                     int64_t *out_id)
{
    /* try to find existing node matching labels and all match properties */
    const char *find_sql =
        "SELECT id FROM nodes WHERE labels = ? "
        "AND (? IS NULL OR properties = json_patch(properties, ?)) LIMIT 1";
    sqlite3_stmt *stmt;

    if (sqlite3_prepare_v2(g->db, find_sql, -1, &stmt, NULL) != SQLITE_OK) {
        set_error(g, sqlite3_errmsg(g->db));
        return GRAPHDB_ERR;
    }

    sqlite3_bind_text(stmt, 1, labels ? labels : "", -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, match_props_json, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, match_props_json, -1, SQLITE_STATIC);

    int rc = sqlite3_step(stmt);

    if (rc == SQLITE_ROW) {
        int64_t id = sqlite3_column_int64(stmt, 0);
        sqlite3_finalize(stmt);
        if (out_id) *out_id = id;

        if (on_match_json) {
            const char *upd =
                "UPDATE nodes SET properties = json_patch(properties, ?) "
                "WHERE id = ?";
            sqlite3_stmt *us;
            if (sqlite3_prepare_v2(g->db, upd, -1, &us, NULL) == SQLITE_OK) {
                sqlite3_bind_text(us, 1, on_match_json, -1, SQLITE_STATIC);
                sqlite3_bind_int64(us, 2, id);
                sqlite3_step(us);
                sqlite3_finalize(us);
            }
        }
        return GRAPHDB_OK;
    }

    sqlite3_finalize(stmt);

    /* not found: create */
    char merged[MAX_PROP_LEN];
    if (match_props_json && on_create_json) {
        snprintf(merged, sizeof(merged),
                 "json_patch('%s', '%s')", match_props_json, on_create_json);
    } else if (match_props_json) {
        strncpy(merged, match_props_json, sizeof(merged) - 1);
    } else {
        strncpy(merged, "{}", sizeof(merged) - 1);
    }

    return graph_create_node(g, labels, merged, out_id);
}

/* ------------------------------------------------------------------ */
/*  ResultSet management                                                */
/* ------------------------------------------------------------------ */

static ResultSet *resultset_alloc(void)
{
    ResultSet *rs = calloc(1, sizeof(ResultSet));
    if (!rs) return NULL;
    rs->capacity = 16;
    rs->rows = calloc(rs->capacity, sizeof(ResultRow));
    if (!rs->rows) { free(rs); return NULL; }
    return rs;
}

static int resultset_push(ResultSet *rs, ResultRow *row)
{
    if (rs->count >= rs->capacity) {
        int newcap = rs->capacity * 2;
        ResultRow *tmp = realloc(rs->rows, newcap * sizeof(ResultRow));
        if (!tmp) return GRAPHDB_OOM;
        rs->rows = tmp;
        rs->capacity = newcap;
    }
    rs->rows[rs->count++] = *row;
    return GRAPHDB_OK;
}

void resultset_free(ResultSet *rs)
{
    if (!rs) return;
    free(rs->rows);
    free(rs);
}

/* ------------------------------------------------------------------ */
/*  Path utilities                                                      */
/* ------------------------------------------------------------------ */

Path *path_alloc(void)
{
    return calloc(1, sizeof(Path));
}

void path_push(Path *p, int64_t node_id, int64_t rel_id)
{
    PathNode *pn = calloc(1, sizeof(PathNode));
    if (!pn) return;
    pn->node_id = node_id;
    pn->rel_id  = rel_id;
    pn->next    = NULL;

    if (!p->head) {
        p->head = pn;
    } else {
        PathNode *cur = p->head;
        while (cur->next) cur = cur->next;
        cur->next = pn;
    }
    p->length++;
}

void path_free(Path *p)
{
    if (!p) return;
    PathNode *cur = p->head;
    while (cur) {
        PathNode *next = cur->next;
        free(cur);
        cur = next;
    }
    free(p);
}

/* ------------------------------------------------------------------ */
/*  BFS shortest path                                                   */
/* ------------------------------------------------------------------ */

#define BFS_MAX_NODES 65536

Path *graph_shortest_path(Graph *g, int64_t src, int64_t dst,
                          const char *rel_type)
{
    int64_t  *parent_node = calloc(BFS_MAX_NODES, sizeof(int64_t));
    int64_t  *parent_rel  = calloc(BFS_MAX_NODES, sizeof(int64_t));
    int      *visited     = calloc(BFS_MAX_NODES, sizeof(int));
    int64_t  *queue       = calloc(BFS_MAX_NODES, sizeof(int64_t));

    if (!parent_node || !parent_rel || !visited || !queue) {
        free(parent_node); free(parent_rel);
        free(visited); free(queue);
        return NULL;
    }

    for (int i = 0; i < BFS_MAX_NODES; i++) {
        parent_node[i] = -1;
        parent_rel[i]  = -1;
    }

    int head = 0, tail = 0;
    queue[tail++] = src;
    visited[src % BFS_MAX_NODES] = 1;

    int found = 0;
    const char *sql_typed =
        "SELECT id, dst_id FROM relationships WHERE src_id = ? AND type = ?";
    const char *sql_any =
        "SELECT id, dst_id FROM relationships WHERE src_id = ?";

    while (head < tail && !found) {
        int64_t cur = queue[head++];

        sqlite3_stmt *stmt;
        const char *sql = rel_type ? sql_typed : sql_any;

        if (sqlite3_prepare_v2(g->db, sql, -1, &stmt, NULL) != SQLITE_OK)
            break;

        sqlite3_bind_int64(stmt, 1, cur);
        if (rel_type) sqlite3_bind_text(stmt, 2, rel_type, -1, SQLITE_STATIC);

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            int64_t rid  = sqlite3_column_int64(stmt, 0);
            int64_t next = sqlite3_column_int64(stmt, 1);
            int idx = (int)(next % BFS_MAX_NODES);

            if (!visited[idx]) {
                visited[idx]     = 1;
                parent_node[idx] = cur;
                parent_rel[idx]  = rid;
                queue[tail++]    = next;

                if (next == dst) { found = 1; break; }
                if (tail >= BFS_MAX_NODES) { found = -1; break; }
            }
        }
        sqlite3_finalize(stmt);
    }

    Path *path = NULL;

    if (found == 1) {
        /* reconstruct path by backtracking */
        int64_t *rev_nodes = calloc(BFS_MAX_NODES, sizeof(int64_t));
        int64_t *rev_rels  = calloc(BFS_MAX_NODES, sizeof(int64_t));
        int plen = 0;

        int64_t cur = dst;
        while (cur != src) {
            int idx = (int)(cur % BFS_MAX_NODES);
            rev_nodes[plen] = cur;
            rev_rels[plen]  = parent_rel[idx];
            plen++;
            cur = parent_node[idx];
        }
        rev_nodes[plen++] = src;

        path = path_alloc();
        for (int i = plen - 1; i >= 0; i--)
            path_push(path, rev_nodes[i], rev_rels[i]);

        free(rev_nodes);
        free(rev_rels);
    }

    free(parent_node);
    free(parent_rel);
    free(visited);
    free(queue);

    return path;
}

/* expose resultset_alloc and resultset_push to cypher_exec.c */
ResultSet *_resultset_alloc(void)      { return resultset_alloc(); }
int        _resultset_push(ResultSet *rs, ResultRow *row)
                                       { return resultset_push(rs, row); }
