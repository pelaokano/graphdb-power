#ifndef GRAPHDB_H
#define GRAPHDB_H

#include <stdint.h>
#include <sqlite3.h>

#define GRAPHDB_VERSION     "0.1.0"
#define GRAPHDB_OK          0
#define GRAPHDB_ERR         -1
#define GRAPHDB_NOT_FOUND   -2
#define GRAPHDB_OOM         -3

#define MAX_LABEL_LEN       128
#define MAX_PROP_LEN        4096
#define MAX_TYPE_LEN        128
#define MAX_SQL_LEN         8192
#define MAX_ERROR_LEN       512

/* ------------------------------------------------------------------ */
/*  Core records                                                        */
/* ------------------------------------------------------------------ */

typedef struct {
    int64_t  id;
    char     labels[MAX_LABEL_LEN];
    char     properties[MAX_PROP_LEN];   /* JSON string */
} Node;

typedef struct {
    int64_t  id;
    int64_t  src_id;
    int64_t  dst_id;
    char     type[MAX_TYPE_LEN];
    char     properties[MAX_PROP_LEN];   /* JSON string */
} Relationship;

/* ------------------------------------------------------------------ */
/*  Result row: union of node / relationship / scalar                  */
/* ------------------------------------------------------------------ */

typedef enum {
    ROW_NODE = 0,
    ROW_REL,
    ROW_SCALAR
} RowType;

typedef struct {
    RowType      type;
    Node         node;
    Relationship rel;
    char         scalar[MAX_PROP_LEN];
} ResultRow;

typedef struct {
    ResultRow   *rows;
    int          count;
    int          capacity;
    char         error[MAX_ERROR_LEN];
} ResultSet;

/* ------------------------------------------------------------------ */
/*  Graph handle                                                        */
/* ------------------------------------------------------------------ */

typedef struct {
    sqlite3     *db;
    char         path[512];
    char         last_error[MAX_ERROR_LEN];
} Graph;

/* ------------------------------------------------------------------ */
/*  Graph lifecycle                                                     */
/* ------------------------------------------------------------------ */

Graph      *graph_open(const char *path);
void        graph_close(Graph *g);
const char *graph_last_error(Graph *g);

/* ------------------------------------------------------------------ */
/*  Node operations                                                     */
/* ------------------------------------------------------------------ */

int  graph_create_node(Graph *g, const char *labels, const char *props_json,
                       int64_t *out_id);
int  graph_get_node(Graph *g, int64_t id, Node *out);
int  graph_set_node_property(Graph *g, int64_t id, const char *key,
                             const char *value_json);
int  graph_remove_node_property(Graph *g, int64_t id, const char *key);
int  graph_add_label(Graph *g, int64_t id, const char *label);
int  graph_remove_label(Graph *g, int64_t id, const char *label);
int  graph_delete_node(Graph *g, int64_t id, int detach);

/* ------------------------------------------------------------------ */
/*  Relationship operations                                             */
/* ------------------------------------------------------------------ */

int  graph_create_rel(Graph *g, int64_t src, int64_t dst, const char *type,
                      const char *props_json, int64_t *out_id);
int  graph_get_rel(Graph *g, int64_t id, Relationship *out);
int  graph_set_rel_property(Graph *g, int64_t id, const char *key,
                            const char *value_json);
int  graph_remove_rel_property(Graph *g, int64_t id, const char *key);
int  graph_delete_rel(Graph *g, int64_t id);

/* ------------------------------------------------------------------ */
/*  Merge                                                               */
/* ------------------------------------------------------------------ */

/* on_create_json / on_match_json may be NULL */
int  graph_merge_node(Graph *g, const char *labels, const char *match_props_json,
                      const char *on_create_json, const char *on_match_json,
                      int64_t *out_id);

/* ------------------------------------------------------------------ */
/*  Cypher query                                                        */
/* ------------------------------------------------------------------ */

ResultSet  *graph_query(Graph *g, const char *cypher);
void        resultset_free(ResultSet *rs);

/* ------------------------------------------------------------------ */
/*  Path utilities (used by cypher executor)                           */
/* ------------------------------------------------------------------ */

typedef struct PathNode {
    int64_t          node_id;
    int64_t          rel_id;    /* -1 for first node */
    struct PathNode *next;
} PathNode;

typedef struct {
    PathNode *head;
    int       length;
} Path;

Path *path_alloc(void);
void  path_push(Path *p, int64_t node_id, int64_t rel_id);
void  path_free(Path *p);

/* ------------------------------------------------------------------ */
/*  BFS / shortest path                                                 */
/* ------------------------------------------------------------------ */

Path *graph_shortest_path(Graph *g, int64_t src, int64_t dst,
                          const char *rel_type);  /* NULL = any type */

#endif /* GRAPHDB_H */
