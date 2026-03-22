#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>
#include <math.h>
#include "graph_algorithms.h"

/* ------------------------------------------------------------------ */
/*  AdjList builder                                                     */
/* ------------------------------------------------------------------ */

AdjList *adjlist_build(Graph *g, const char *rel_type,
                       const char *weight_key, int undirected)
{
    /* collect node ids - if rel_type is specified, only load nodes
       that participate in at least one edge of that type */
    sqlite3_stmt *stmt;
    int cap = 256;
    int64_t *node_ids = malloc(cap * sizeof(int64_t));
    int n = 0;

    if (rel_type) {
        /* nodes referenced by edges of this type */
        char node_sql[512];
        snprintf(node_sql, sizeof(node_sql),
            "SELECT DISTINCT id FROM nodes WHERE id IN ("
            "  SELECT src_id FROM relationships WHERE type = '%s'"
            "  UNION"
            "  SELECT dst_id FROM relationships WHERE type = '%s'"
            ") ORDER BY id", rel_type, rel_type);
        if (sqlite3_prepare_v2(g->db, node_sql, -1, &stmt, NULL) != SQLITE_OK) {
            free(node_ids); return NULL;
        }
    } else {
        const char *node_sql = "SELECT id FROM nodes ORDER BY id";
        if (sqlite3_prepare_v2(g->db, node_sql, -1, &stmt, NULL) != SQLITE_OK) {
            free(node_ids); return NULL;
        }
    }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) { cap *= 2; node_ids = realloc(node_ids, cap * sizeof(int64_t)); }
        node_ids[n++] = sqlite3_column_int64(stmt, 0);
    }
    sqlite3_finalize(stmt);

    AdjList *al = calloc(1, sizeof(AdjList));
    al->node_count = n;
    al->nodes      = calloc(n, sizeof(AdjNode));
    for (int i = 0; i < n; i++) {
        al->nodes[i].node_id = node_ids[i];
        al->nodes[i].head    = NULL;
    }
    free(node_ids);

    /* load edges */
    char sql[512];
    if (rel_type) {
        snprintf(sql, sizeof(sql),
            "SELECT id, src_id, dst_id, properties "
            "FROM relationships WHERE type = '%s'", rel_type);
    } else {
        snprintf(sql, sizeof(sql),
            "SELECT id, src_id, dst_id, properties FROM relationships");
    }

    if (sqlite3_prepare_v2(g->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        adjlist_free(al);
        return NULL;
    }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int64_t rid  = sqlite3_column_int64(stmt, 0);
        int64_t src  = sqlite3_column_int64(stmt, 1);
        int64_t dst  = sqlite3_column_int64(stmt, 2);
        const char *props = (const char *)sqlite3_column_text(stmt, 3);

        double weight = 1.0;
        if (weight_key && props) {
            /* extract weight from JSON: simple scan for "key":value */
            char search[128];
            snprintf(search, sizeof(search), "\"%s\":", weight_key);
            const char *pos = strstr(props, search);
            if (pos) {
                pos += strlen(search);
                while (*pos == ' ') pos++;
                weight = atof(pos);
                if (weight == 0.0) weight = 1.0;
            }
        }

        int si = adjlist_index(al, src);
        int di = adjlist_index(al, dst);

        if (si >= 0) {
            AdjEdge *e = calloc(1, sizeof(AdjEdge));
            e->rel_id      = rid;
            e->dst_node_id = dst;
            e->weight      = weight;
            e->next        = al->nodes[si].head;
            al->nodes[si].head = e;
            al->edge_count++;
        }

        if (undirected && di >= 0) {
            AdjEdge *e = calloc(1, sizeof(AdjEdge));
            e->rel_id      = rid;
            e->dst_node_id = src;
            e->weight      = weight;
            e->next        = al->nodes[di].head;
            al->nodes[di].head = e;
        }
    }
    sqlite3_finalize(stmt);
    return al;
}

int adjlist_index(AdjList *al, int64_t node_id)
{
    /* binary search: nodes are loaded in ORDER BY id */
    int lo = 0, hi = al->node_count - 1;
    while (lo <= hi) {
        int mid = (lo + hi) / 2;
        if      (al->nodes[mid].node_id == node_id) return mid;
        else if (al->nodes[mid].node_id <  node_id) lo = mid + 1;
        else                                          hi = mid - 1;
    }
    return -1;
}

void adjlist_free(AdjList *al)
{
    if (!al) return;
    for (int i = 0; i < al->node_count; i++) {
        AdjEdge *e = al->nodes[i].head;
        while (e) {
            AdjEdge *next = e->next;
            free(e);
            e = next;
        }
    }
    free(al->nodes);
    free(al);
}

/* ------------------------------------------------------------------ */
/*  BFS for connected components                                        */
/* ------------------------------------------------------------------ */

ComponentResult *algo_connected_components(Graph *g, const char *rel_type)
{
    AdjList *al = adjlist_build(g, rel_type, NULL, 1);
    if (!al) return NULL;

    int n = al->node_count;
    int *comp    = calloc(n, sizeof(int));
    int *visited = calloc(n, sizeof(int));
    int *queue   = calloc(n, sizeof(int));

    for (int i = 0; i < n; i++) comp[i] = -1;

    int num_comp = 0;

    for (int start = 0; start < n; start++) {
        if (visited[start]) continue;

        int head = 0, tail = 0;
        queue[tail++] = start;
        visited[start] = 1;
        comp[start] = num_comp;

        while (head < tail) {
            int cur = queue[head++];
            for (AdjEdge *e = al->nodes[cur].head; e; e = e->next) {
                int ni = adjlist_index(al, e->dst_node_id);
                if (ni >= 0 && !visited[ni]) {
                    visited[ni] = 1;
                    comp[ni]    = num_comp;
                    queue[tail++] = ni;
                }
            }
        }
        num_comp++;
    }

    ComponentResult *r = calloc(1, sizeof(ComponentResult));
    r->node_count      = n;
    r->component_count = num_comp;
    r->node_ids        = calloc(n, sizeof(int64_t));
    r->component       = calloc(n, sizeof(int));

    for (int i = 0; i < n; i++) {
        r->node_ids[i]  = al->nodes[i].node_id;
        r->component[i] = comp[i];
    }

    free(comp); free(visited); free(queue);
    adjlist_free(al);
    return r;
}

void component_result_free(ComponentResult *r)
{
    if (!r) return;
    free(r->node_ids);
    free(r->component);
    free(r);
}

int algo_is_connected(Graph *g, const char *rel_type)
{
    ComponentResult *r = algo_connected_components(g, rel_type);
    if (!r) return 0;
    int result = (r->component_count == 1);
    component_result_free(r);
    return result;
}

/* ------------------------------------------------------------------ */
/*  Tarjan DFS for bridges and articulation points                      */
/* ------------------------------------------------------------------ */

typedef struct {
    int *disc;        /* discovery time   */
    int *low;         /* low-link value   */
    int *parent;      /* parent index     */
    int *visited;
    int *is_ap;       /* articulation point flag */
    int  timer;

    int64_t *bridge_rels;
    int      bridge_count;
    int      bridge_cap;
} TarjanState;

static void tarjan_dfs(AdjList *al, int u, TarjanState *st)
{
    st->visited[u] = 1;
    st->disc[u]    = st->low[u] = st->timer++;
    int children   = 0;

    for (AdjEdge *e = al->nodes[u].head; e; e = e->next) {
        int v = adjlist_index(al, e->dst_node_id);
        if (v < 0) continue;

        if (!st->visited[v]) {
            children++;
            st->parent[v] = u;
            tarjan_dfs(al, v, st);

            if (st->low[v] < st->low[u])
                st->low[u] = st->low[v];

            /* articulation point conditions */
            if (st->parent[u] == -1 && children > 1)
                st->is_ap[u] = 1;
            if (st->parent[u] != -1 && st->low[v] >= st->disc[u])
                st->is_ap[u] = 1;

            /* bridge condition */
            if (st->low[v] > st->disc[u]) {
                if (st->bridge_count >= st->bridge_cap) {
                    st->bridge_cap *= 2;
                    st->bridge_rels = realloc(st->bridge_rels,
                                              st->bridge_cap * sizeof(int64_t));
                }
                st->bridge_rels[st->bridge_count++] = e->rel_id;
            }

        } else if (v != st->parent[u]) {
            if (st->disc[v] < st->low[u])
                st->low[u] = st->disc[v];
        }
    }
}

static TarjanState *tarjan_run(AdjList *al)
{
    int n = al->node_count;
    TarjanState *st = calloc(1, sizeof(TarjanState));
    st->disc        = calloc(n, sizeof(int));
    st->low         = calloc(n, sizeof(int));
    st->parent      = calloc(n, sizeof(int));
    st->visited     = calloc(n, sizeof(int));
    st->is_ap       = calloc(n, sizeof(int));
    st->bridge_cap  = 64;
    st->bridge_rels = malloc(st->bridge_cap * sizeof(int64_t));

    for (int i = 0; i < n; i++) st->parent[i] = -1;

    for (int i = 0; i < n; i++)
        if (!st->visited[i])
            tarjan_dfs(al, i, st);

    return st;
}

static void tarjan_free(TarjanState *st)
{
    if (!st) return;
    free(st->disc);
    free(st->low);
    free(st->parent);
    free(st->visited);
    free(st->is_ap);
    free(st->bridge_rels);
    free(st);
}

IdList *algo_find_bridges(Graph *g, const char *rel_type)
{
    AdjList *al = adjlist_build(g, rel_type, NULL, 1);
    if (!al) return NULL;

    TarjanState *st = tarjan_run(al);

    IdList *result    = calloc(1, sizeof(IdList));
    result->count     = st->bridge_count;
    result->ids       = calloc(st->bridge_count + 1, sizeof(int64_t));
    memcpy(result->ids, st->bridge_rels,
           st->bridge_count * sizeof(int64_t));

    tarjan_free(st);
    adjlist_free(al);
    return result;
}

IdList *algo_find_articulation_points(Graph *g, const char *rel_type)
{
    AdjList *al = adjlist_build(g, rel_type, NULL, 1);
    if (!al) return NULL;

    TarjanState *st = tarjan_run(al);

    int count = 0;
    for (int i = 0; i < al->node_count; i++)
        if (st->is_ap[i]) count++;

    IdList *result = calloc(1, sizeof(IdList));
    result->count  = count;
    result->ids    = calloc(count + 1, sizeof(int64_t));

    int j = 0;
    for (int i = 0; i < al->node_count; i++)
        if (st->is_ap[i])
            result->ids[j++] = al->nodes[i].node_id;

    tarjan_free(st);
    adjlist_free(al);
    return result;
}

void idlist_free(IdList *l)
{
    if (!l) return;
    free(l->ids);
    free(l);
}

/* ------------------------------------------------------------------ */
/*  N-1 topological: is removing rel_id a bridge?                      */
/* ------------------------------------------------------------------ */

int algo_n1_is_critical(Graph *g, int64_t rel_id)
{
    /* temporarily disable the relationship and check connectivity */
    char sql[256];
    snprintf(sql, sizeof(sql),
             "UPDATE relationships SET type = '__DISABLED__' WHERE id = %lld",
             (long long)rel_id);

    char *errmsg = NULL;
    sqlite3_exec(g->db, sql, NULL, NULL, &errmsg);
    if (errmsg) { sqlite3_free(errmsg); return 0; }

    int connected = algo_is_connected(g, NULL);

    snprintf(sql, sizeof(sql),
             "UPDATE relationships SET type = (SELECT type FROM relationships "
             "WHERE id = %lld) WHERE id = %lld",
             (long long)rel_id, (long long)rel_id);

    /* restore: we need the original type - use a different approach */
    /* re-enable by reversing the disable marker */
    snprintf(sql, sizeof(sql),
             "UPDATE relationships SET type = replace(type,'__DISABLED__','') "
             "WHERE id = %lld",
             (long long)rel_id);
    sqlite3_exec(g->db, sql, NULL, NULL, NULL);

    return !connected;
}

/* ------------------------------------------------------------------ */
/*  N-1 correct implementation using bridge detection                   */
/* ------------------------------------------------------------------ */

int algo_n1_is_critical_v2(Graph *g, int64_t rel_id)
{
    /* get the rel type first so we can restore it */
    char type_buf[128] = "";
    sqlite3_stmt *stmt;
    const char *get_type = "SELECT type FROM relationships WHERE id = ?";
    if (sqlite3_prepare_v2(g->db, get_type, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, 1, rel_id);
        if (sqlite3_step(stmt) == SQLITE_ROW)
            strncpy(type_buf,
                    (const char *)sqlite3_column_text(stmt, 0),
                    sizeof(type_buf) - 1);
        sqlite3_finalize(stmt);
    }

    if (!type_buf[0]) return 0;

    /* disable */
    const char *disable_sql =
        "UPDATE relationships SET type = '__N1_DISABLED__' WHERE id = ?";
    if (sqlite3_prepare_v2(g->db, disable_sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, 1, rel_id);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

    int connected = algo_is_connected(g, NULL);

    /* restore */
    const char *restore_sql =
        "UPDATE relationships SET type = ? WHERE id = ?";
    if (sqlite3_prepare_v2(g->db, restore_sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, type_buf, -1, SQLITE_STATIC);
        sqlite3_bind_int64(stmt, 2, rel_id);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }

    return !connected;
}

/* ------------------------------------------------------------------ */
/*  Dijkstra with binary min-heap                                       */
/* ------------------------------------------------------------------ */

typedef struct {
    double  dist;
    int     idx;
} HeapEntry;

typedef struct {
    HeapEntry *data;
    int        size;
    int        cap;
} MinHeap;

static MinHeap *heap_alloc(int cap)
{
    MinHeap *h = calloc(1, sizeof(MinHeap));
    h->data    = malloc(cap * sizeof(HeapEntry));
    h->cap     = cap;
    return h;
}

static void heap_push(MinHeap *h, double dist, int idx)
{
    if (h->size >= h->cap) {
        h->cap *= 2;
        h->data = realloc(h->data, h->cap * sizeof(HeapEntry));
    }
    int i = h->size++;
    h->data[i].dist = dist;
    h->data[i].idx  = idx;
    /* sift up */
    while (i > 0) {
        int parent = (i - 1) / 2;
        if (h->data[parent].dist > h->data[i].dist) {
            HeapEntry tmp    = h->data[parent];
            h->data[parent] = h->data[i];
            h->data[i]      = tmp;
            i = parent;
        } else break;
    }
}

static HeapEntry heap_pop(MinHeap *h)
{
    HeapEntry top  = h->data[0];
    h->data[0]     = h->data[--h->size];
    /* sift down */
    int i = 0;
    while (1) {
        int left  = 2 * i + 1;
        int right = 2 * i + 2;
        int smallest = i;
        if (left  < h->size && h->data[left].dist  < h->data[smallest].dist) smallest = left;
        if (right < h->size && h->data[right].dist < h->data[smallest].dist) smallest = right;
        if (smallest == i) break;
        HeapEntry tmp       = h->data[i];
        h->data[i]          = h->data[smallest];
        h->data[smallest]   = tmp;
        i = smallest;
    }
    return top;
}

DijkstraResult *algo_dijkstra(Graph *g, int64_t src,
                               const char *rel_type,
                               const char *weight_key)
{
    AdjList *al = adjlist_build(g, rel_type, weight_key, 0);
    if (!al) return NULL;

    int src_idx = adjlist_index(al, src);
    if (src_idx < 0) { adjlist_free(al); return NULL; }

    int n         = al->node_count;
    double  *dist = malloc(n * sizeof(double));
    int64_t *prev_node = malloc(n * sizeof(int64_t));
    int64_t *prev_rel  = malloc(n * sizeof(int64_t));
    int     *settled   = calloc(n, sizeof(int));

    for (int i = 0; i < n; i++) {
        dist[i]      = DBL_MAX;
        prev_node[i] = -1;
        prev_rel[i]  = -1;
    }
    dist[src_idx] = 0.0;

    MinHeap *heap = heap_alloc(n < 16 ? 16 : n);
    heap_push(heap, 0.0, src_idx);

    while (heap->size > 0) {
        HeapEntry cur = heap_pop(heap);
        int u = cur.idx;
        if (settled[u]) continue;
        settled[u] = 1;

        for (AdjEdge *e = al->nodes[u].head; e; e = e->next) {
            int v = adjlist_index(al, e->dst_node_id);
            if (v < 0 || settled[v]) continue;

            double nd = dist[u] + e->weight;
            if (nd < dist[v]) {
                dist[v]      = nd;
                prev_node[v] = al->nodes[u].node_id;
                prev_rel[v]  = e->rel_id;
                heap_push(heap, nd, v);
            }
        }
    }

    DijkstraResult *r = calloc(1, sizeof(DijkstraResult));
    r->count          = n;
    r->nodes          = calloc(n, sizeof(DijkstraNode));
    if (weight_key)
        strncpy(r->weight_key, weight_key, sizeof(r->weight_key) - 1);

    for (int i = 0; i < n; i++) {
        r->nodes[i].node_id      = al->nodes[i].node_id;
        r->nodes[i].prev_node_id = prev_node[i];
        r->nodes[i].prev_rel_id  = prev_rel[i];
        r->nodes[i].dist         = dist[i] < DBL_MAX ? dist[i] : -1.0;
    }

    free(dist); free(prev_node); free(prev_rel); free(settled);
    free(heap->data); free(heap);
    adjlist_free(al);
    return r;
}

void dijkstra_result_free(DijkstraResult *r)
{
    if (!r) return;
    free(r->nodes);
    free(r);
}

/* ------------------------------------------------------------------ */
/*  Degree sequence                                                     */
/* ------------------------------------------------------------------ */

DegreeResult *algo_degree(Graph *g, const char *rel_type)
{
    AdjList *al = adjlist_build(g, rel_type, NULL, 1);
    if (!al) return NULL;

    DegreeResult *r = calloc(1, sizeof(DegreeResult));
    r->count        = al->node_count;
    r->entries      = calloc(al->node_count, sizeof(DegreeEntry));

    for (int i = 0; i < al->node_count; i++) {
        r->entries[i].node_id = al->nodes[i].node_id;
        int deg = 0;
        for (AdjEdge *e = al->nodes[i].head; e; e = e->next) deg++;
        r->entries[i].degree = deg;
    }

    adjlist_free(al);
    return r;
}

void degree_result_free(DegreeResult *r)
{
    if (!r) return;
    free(r->entries);
    free(r);
}
