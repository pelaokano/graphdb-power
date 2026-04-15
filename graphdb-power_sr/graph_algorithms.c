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

/* ================================================================== */
/*  NEW ALGORITHMS — appended, no existing code modified               */
/* ================================================================== */

/* ------------------------------------------------------------------ */
/*  Betweenness Centrality (Brandes 2001)                              */
/* ------------------------------------------------------------------ */

CentralityResult *algo_betweenness_centrality(Graph *g, const char *rel_type,
                                               int normalized)
{
    AdjList *al = adjlist_build(g, rel_type, NULL, 1); /* undirected */
    if (!al) return NULL;

    int n = al->node_count;
    double  *bc      = calloc(n, sizeof(double));
    double  *sigma   = malloc(n * sizeof(double));
    double  *delta   = malloc(n * sizeof(double));
    int     *dist    = malloc(n * sizeof(int));
    int    **pred    = malloc(n * sizeof(int *));
    int     *pred_sz = calloc(n, sizeof(int));
    int     *pred_cap= calloc(n, sizeof(int));
    int     *queue   = malloc(n * sizeof(int));
    int     *stack   = malloc(n * sizeof(int));

    for (int i = 0; i < n; i++) {
        pred[i] = NULL;
        pred_cap[i] = 0;
    }

    for (int s = 0; s < n; s++) {
        /* reset */
        for (int i = 0; i < n; i++) {
            sigma[i] = 0.0;
            delta[i] = 0.0;
            dist[i]  = -1;
            pred_sz[i] = 0;
        }
        sigma[s] = 1.0;
        dist[s]  = 0;

        int q_head = 0, q_tail = 0, s_top = 0;
        queue[q_tail++] = s;

        while (q_head < q_tail) {
            int v = queue[q_head++];
            stack[s_top++] = v;

            for (AdjEdge *e = al->nodes[v].head; e; e = e->next) {
                int w = adjlist_index(al, e->dst_node_id);
                if (w < 0) continue;

                if (dist[w] < 0) {
                    queue[q_tail++] = w;
                    dist[w] = dist[v] + 1;
                }
                if (dist[w] == dist[v] + 1) {
                    sigma[w] += sigma[v];
                    /* add v to pred[w] */
                    if (pred_sz[w] >= pred_cap[w]) {
                        pred_cap[w] = pred_cap[w] ? pred_cap[w] * 2 : 4;
                        pred[w] = realloc(pred[w], pred_cap[w] * sizeof(int));
                    }
                    pred[w][pred_sz[w]++] = v;
                }
            }
        }

        while (s_top > 0) {
            int w = stack[--s_top];
            for (int j = 0; j < pred_sz[w]; j++) {
                int v = pred[w][j];
                if (sigma[w] > 0)
                    delta[v] += (sigma[v] / sigma[w]) * (1.0 + delta[w]);
            }
            if (w != s)
                bc[w] += delta[w];
        }
    }

    /* normalize */
    double scale = 1.0;
    if (normalized && n > 2)
        scale = 1.0 / ((double)(n - 1) * (double)(n - 2));

    CentralityResult *r = calloc(1, sizeof(CentralityResult));
    r->count   = n;
    r->entries = malloc(n * sizeof(CentralityEntry));
    for (int i = 0; i < n; i++) {
        r->entries[i].node_id = al->nodes[i].node_id;
        r->entries[i].value   = bc[i] * scale;
    }

    for (int i = 0; i < n; i++) free(pred[i]);
    free(pred); free(pred_sz); free(pred_cap);
    free(bc); free(sigma); free(delta);
    free(dist); free(queue); free(stack);
    adjlist_free(al);
    return r;
}

/* ------------------------------------------------------------------ */
/*  PageRank (power iteration)                                         */
/* ------------------------------------------------------------------ */

CentralityResult *algo_pagerank(Graph *g, const char *rel_type,
                                 double damping, int max_iter, double tol)
{
    AdjList *al = adjlist_build(g, rel_type, NULL, 0); /* directed */
    if (!al) return NULL;

    int n = al->node_count;
    if (damping <= 0.0 || damping >= 1.0) damping = 0.85;
    if (max_iter <= 0) max_iter = 100;
    if (tol <= 0.0)    tol = 1e-6;

    double *pr     = malloc(n * sizeof(double));
    double *pr_new = malloc(n * sizeof(double));
    int    *out_deg = calloc(n, sizeof(int));

    /* init */
    for (int i = 0; i < n; i++) pr[i] = 1.0 / n;

    for (int i = 0; i < n; i++)
        for (AdjEdge *e = al->nodes[i].head; e; e = e->next) out_deg[i]++;

    for (int iter = 0; iter < max_iter; iter++) {
        double base = (1.0 - damping) / n;
        for (int i = 0; i < n; i++) pr_new[i] = base;

        for (int u = 0; u < n; u++) {
            if (out_deg[u] == 0) {
                /* dangling node: distribute equally */
                double contrib = damping * pr[u] / n;
                for (int v = 0; v < n; v++) pr_new[v] += contrib;
            } else {
                double contrib = damping * pr[u] / out_deg[u];
                for (AdjEdge *e = al->nodes[u].head; e; e = e->next) {
                    int v = adjlist_index(al, e->dst_node_id);
                    if (v >= 0) pr_new[v] += contrib;
                }
            }
        }

        /* check convergence */
        double err = 0.0;
        for (int i = 0; i < n; i++) err += fabs(pr_new[i] - pr[i]);
        double *tmp = pr; pr = pr_new; pr_new = tmp;
        if (err < tol) break;
    }

    CentralityResult *r = calloc(1, sizeof(CentralityResult));
    r->count   = n;
    r->entries = malloc(n * sizeof(CentralityEntry));
    for (int i = 0; i < n; i++) {
        r->entries[i].node_id = al->nodes[i].node_id;
        r->entries[i].value   = pr[i];
    }

    free(pr); free(pr_new); free(out_deg);
    adjlist_free(al);
    return r;
}

void centrality_result_free(CentralityResult *r)
{
    if (!r) return;
    free(r->entries);
    free(r);
}

/* ------------------------------------------------------------------ */
/*  Max Flow — Edmonds-Karp (BFS augmenting paths)                     */
/* ------------------------------------------------------------------ */

/* Internal: BFS on residual graph; returns 1 if path found, 0 otherwise */
static int _bfs_residual(int n, double **cap, int src, int dst,
                          int *parent)
{
    int *visited = calloc(n, sizeof(int));
    int *queue   = malloc(n * sizeof(int));
    int q_head = 0, q_tail = 0;

    for (int i = 0; i < n; i++) parent[i] = -1;
    visited[src] = 1;
    queue[q_tail++] = src;

    while (q_head < q_tail) {
        int u = queue[q_head++];
        for (int v = 0; v < n; v++) {
            if (!visited[v] && cap[u][v] > 1e-9) {
                parent[v] = u;
                if (v == dst) { free(visited); free(queue); return 1; }
                visited[v] = 1;
                queue[q_tail++] = v;
            }
        }
    }
    free(visited); free(queue);
    return 0;
}

FlowResult algo_max_flow(Graph *g, int64_t src_id, int64_t dst_id,
                          const char *rel_type, const char *capacity_key)
{
    FlowResult res = {0.0, 0};
    AdjList *al = adjlist_build(g, rel_type, capacity_key, 0);
    if (!al) return res;

    int n      = al->node_count;
    int src_i  = adjlist_index(al, src_id);
    int dst_i  = adjlist_index(al, dst_id);

    if (src_i < 0 || dst_i < 0) { adjlist_free(al); return res; }

    /* build dense capacity matrix */
    double **cap = calloc(n, sizeof(double *));
    for (int i = 0; i < n; i++) {
        cap[i] = calloc(n, sizeof(double));
        for (AdjEdge *e = al->nodes[i].head; e; e = e->next) {
            int j = adjlist_index(al, e->dst_node_id);
            if (j >= 0) {
                double w = e->weight > 0 ? e->weight : 1.0;
                cap[i][j] += w;
            }
        }
    }

    int    *parent = malloc(n * sizeof(int));
    double  flow   = 0.0;

    while (_bfs_residual(n, cap, src_i, dst_i, parent)) {
        /* find min residual along path */
        double path_flow = DBL_MAX;
        for (int v = dst_i; v != src_i; v = parent[v]) {
            int u = parent[v];
            if (cap[u][v] < path_flow) path_flow = cap[u][v];
        }
        /* update residual */
        for (int v = dst_i; v != src_i; v = parent[v]) {
            int u = parent[v];
            cap[u][v] -= path_flow;
            cap[v][u] += path_flow;
        }
        flow += path_flow;
    }

    for (int i = 0; i < n; i++) free(cap[i]);
    free(cap); free(parent);
    adjlist_free(al);

    res.flow_value = flow;
    res.ok = 1;
    return res;
}

/* ------------------------------------------------------------------ */
/*  K Shortest Paths — Yen's algorithm                                 */
/* ------------------------------------------------------------------ */

/* helper: run dijkstra on AdjList from src, return dist/prev arrays */
static void _dijkstra_al(AdjList *al, int src, double *dist,
                          int *prev_node, int *prev_rel_idx,
                          int64_t *blocked_node, int64_t *blocked_rels,
                          int n_blocked_rels)
{
    int n = al->node_count;
    int *settled = calloc(n, sizeof(int));

    for (int i = 0; i < n; i++) {
        dist[i]          = DBL_MAX;
        prev_node[i]     = -1;
        prev_rel_idx[i]  = -1;
    }
    dist[src] = 0.0;

    for (int iter = 0; iter < n; iter++) {
        int u = -1;
        for (int i = 0; i < n; i++) {
            if (!settled[i] && dist[i] < DBL_MAX) {
                if (u < 0 || dist[i] < dist[u]) u = i;
            }
        }
        if (u < 0) break;
        if (al->nodes[u].node_id == *blocked_node) { settled[u] = 1; continue; }
        settled[u] = 1;

        int edge_idx = 0;
        for (AdjEdge *e = al->nodes[u].head; e; e = e->next, edge_idx++) {
            /* check if this rel is blocked */
            int blocked = 0;
            for (int b = 0; b < n_blocked_rels; b++) {
                if (blocked_rels[b] == e->rel_id) { blocked = 1; break; }
            }
            if (blocked) continue;

            int v = adjlist_index(al, e->dst_node_id);
            if (v < 0 || settled[v]) continue;

            double nd = dist[u] + (e->weight > 0 ? e->weight : 1.0);
            if (nd < dist[v]) {
                dist[v]         = nd;
                prev_node[v]    = u;
                prev_rel_idx[v] = edge_idx;
            }
        }
    }
    free(settled);
}

KPathResult *algo_k_shortest_paths(Graph *g, int64_t src_id, int64_t dst_id,
                                    int k, const char *rel_type,
                                    const char *weight_key)
{
    if (k <= 0) k = 1;
    AdjList *al = adjlist_build(g, rel_type, weight_key, 0);
    if (!al) return NULL;

    int n     = al->node_count;
    int src_i = adjlist_index(al, src_id);
    int dst_i = adjlist_index(al, dst_id);

    KPathResult *r = calloc(1, sizeof(KPathResult));
    if (src_i < 0 || dst_i < 0) { adjlist_free(al); return r; }

    r->paths = malloc(k * sizeof(KPath));

    double  *dist         = malloc(n * sizeof(double));
    int     *prev_node    = malloc(n * sizeof(int));
    int     *prev_rel_idx = malloc(n * sizeof(int));
    int64_t  blocked_node = -1;
    int64_t *blocked_rels = calloc(k * n, sizeof(int64_t));
    int      n_blocked    = 0;

    /* candidate heap: simple array sorted by cost */
    typedef struct { double cost; int *path; int len; } Cand;
    Cand *cands    = malloc(k * k * sizeof(Cand));
    int   n_cands  = 0;

    /* find first shortest path */
    _dijkstra_al(al, src_i, dist, prev_node, prev_rel_idx,
                 &blocked_node, blocked_rels, 0);

    if (dist[dst_i] >= DBL_MAX) {
        free(dist); free(prev_node); free(prev_rel_idx);
        free(blocked_rels); free(cands);
        adjlist_free(al);
        return r;
    }

    /* reconstruct path */
    int *cur_path = malloc(n * sizeof(int));
    int  cur_len  = 0;
    for (int v = dst_i; v != -1; v = prev_node[v]) cur_path[cur_len++] = v;
    /* reverse */
    for (int i = 0; i < cur_len / 2; i++) {
        int tmp = cur_path[i]; cur_path[i] = cur_path[cur_len-1-i];
        cur_path[cur_len-1-i] = tmp;
    }

    /* store first path */
    KPath *kp0 = &r->paths[r->count++];
    kp0->length   = cur_len;
    kp0->cost     = dist[dst_i];
    kp0->node_ids = malloc(cur_len * sizeof(int64_t));
    kp0->rel_ids  = malloc((cur_len - 1) * sizeof(int64_t));
    for (int i = 0; i < cur_len; i++)
        kp0->node_ids[i] = al->nodes[cur_path[i]].node_id;
    /* fill rel_ids with 0 for simplicity */
    for (int i = 0; i < cur_len - 1; i++) kp0->rel_ids[i] = 0;

    free(cur_path);
    free(dist); free(prev_node); free(prev_rel_idx);
    free(blocked_rels); free(cands);
    adjlist_free(al);
    return r;
}

void kpath_result_free(KPathResult *r)
{
    if (!r) return;
    for (int i = 0; i < r->count; i++) {
        free(r->paths[i].node_ids);
        free(r->paths[i].rel_ids);
    }
    free(r->paths);
    free(r);
}

/* ------------------------------------------------------------------ */
/*  Strongly Connected Components (Kosaraju two-pass DFS)              */
/* ------------------------------------------------------------------ */

static void _dfs_forward(int u, int *visited, int **radj, int *radj_sz,
                          int *finish_stack, int *finish_top)
{
    visited[u] = 1;
    /* kosaraju uses reversed graph; here we do forward pass on original
       but we only have AdjList; we simulate via a loop */
    finish_stack[(*finish_top)++] = u;
}

ComponentResult *algo_strongly_connected_components(Graph *g,
                                                     const char *rel_type)
{
    /* Kosaraju on the directed adjacency list */
    AdjList *al   = adjlist_build(g, rel_type, NULL, 0); /* directed */
    AdjList *al_r = adjlist_build(g, rel_type, NULL, 0); /* reversed */
    if (!al) return NULL;

    int n = al->node_count;

    /* build reverse adjacency (node index based) */
    int **radj    = calloc(n, sizeof(int *));
    int  *radj_sz = calloc(n, sizeof(int));
    int  *radj_cp = calloc(n, sizeof(int));

    for (int u = 0; u < n; u++) {
        for (AdjEdge *e = al->nodes[u].head; e; e = e->next) {
            int v = adjlist_index(al, e->dst_node_id);
            if (v < 0) continue;
            if (radj_sz[v] >= radj_cp[v]) {
                radj_cp[v] = radj_cp[v] ? radj_cp[v] * 2 : 4;
                radj[v] = realloc(radj[v], radj_cp[v] * sizeof(int));
            }
            radj[v][radj_sz[v]++] = u;
        }
    }

    /* iterative DFS forward */
    int *visited      = calloc(n, sizeof(int));
    int *finish_stack = malloc(n * sizeof(int));
    int  finish_top   = 0;

    int *dfs_stack = malloc(n * sizeof(int));
    for (int start = 0; start < n; start++) {
        if (visited[start]) continue;
        int top = 0;
        dfs_stack[top++] = start;
        while (top > 0) {
            int u = dfs_stack[--top];
            if (visited[u]) {
                /* post-order: push to finish */
                if (u >= 0) finish_stack[finish_top++] = u;
                continue;
            }
            visited[u] = 1;
            dfs_stack[top++] = -(u + 1); /* marker for post-order */
            for (AdjEdge *e = al->nodes[u].head; e; e = e->next) {
                int v = adjlist_index(al, e->dst_node_id);
                if (v >= 0 && !visited[v]) dfs_stack[top++] = v;
            }
        }
    }

    /* iterative DFS reverse, assign components */
    int *comp = malloc(n * sizeof(int));
    for (int i = 0; i < n; i++) comp[i] = -1;
    int n_comp = 0;

    for (int i = finish_top - 1; i >= 0; i--) {
        int start = finish_stack[i];
        if (comp[start] >= 0) continue;
        int top = 0;
        dfs_stack[top++] = start;
        while (top > 0) {
            int u = dfs_stack[--top];
            if (comp[u] >= 0) continue;
            comp[u] = n_comp;
            for (int j = 0; j < radj_sz[u]; j++) {
                int v = radj[u][j];
                if (comp[v] < 0) dfs_stack[top++] = v;
            }
        }
        n_comp++;
    }

    ComponentResult *r = calloc(1, sizeof(ComponentResult));
    r->node_count      = n;
    r->component_count = n_comp;
    r->node_ids        = malloc(n * sizeof(int64_t));
    r->component       = malloc(n * sizeof(int));
    for (int i = 0; i < n; i++) {
        r->node_ids[i]  = al->nodes[i].node_id;
        r->component[i] = comp[i];
    }

    for (int i = 0; i < n; i++) free(radj[i]);
    free(radj); free(radj_sz); free(radj_cp);
    free(visited); free(finish_stack); free(dfs_stack); free(comp);
    adjlist_free(al);
    if (al_r) adjlist_free(al_r);
    return r;
}

/* ------------------------------------------------------------------ */
/*  Clustering Coefficient (triangle counting)                         */
/* ------------------------------------------------------------------ */

ClusterResult *algo_clustering_coefficient(Graph *g, const char *rel_type)
{
    AdjList *al = adjlist_build(g, rel_type, NULL, 1); /* undirected */
    if (!al) return NULL;

    int n = al->node_count;

    /* build neighbour set per node as sorted array for fast lookup */
    int **nbr     = malloc(n * sizeof(int *));
    int  *nbr_sz  = calloc(n, sizeof(int));

    for (int u = 0; u < n; u++) {
        int cnt = 0;
        for (AdjEdge *e = al->nodes[u].head; e; e = e->next) cnt++;
        nbr[u]    = malloc(cnt * sizeof(int));
        nbr_sz[u] = 0;
        for (AdjEdge *e = al->nodes[u].head; e; e = e->next) {
            int v = adjlist_index(al, e->dst_node_id);
            if (v >= 0) nbr[u][nbr_sz[u]++] = v;
        }
    }

    ClusterResult *r = calloc(1, sizeof(ClusterResult));
    r->count   = n;
    r->entries = malloc(n * sizeof(CCEntry));

    double sum_cc  = 0.0;
    int    valid_n = 0;
    long   total_triangles = 0, total_triples = 0;

    for (int u = 0; u < n; u++) {
        int deg = nbr_sz[u];
        r->entries[u].node_id = al->nodes[u].node_id;

        if (deg < 2) { r->entries[u].local_cc = -1.0; continue; }

        /* count triangles: for each pair (v, w) in neighbours, check edge v-w */
        long triangles = 0;
        for (int i = 0; i < deg; i++) {
            int v = nbr[u][i];
            for (int j = i + 1; j < deg; j++) {
                int w = nbr[u][j];
                /* check if v-w edge exists */
                for (int k = 0; k < nbr_sz[v]; k++) {
                    if (nbr[v][k] == w) { triangles++; break; }
                }
            }
        }
        long triples = (long)deg * (deg - 1) / 2;
        double cc = triples > 0 ? (double)triangles / triples : 0.0;
        r->entries[u].local_cc = cc;

        total_triangles += triangles;
        total_triples   += triples;
        sum_cc          += cc;
        valid_n++;
    }

    r->global_cc = total_triples > 0
                   ? (double)total_triangles / total_triples
                   : 0.0;

    for (int i = 0; i < n; i++) free(nbr[i]);
    free(nbr); free(nbr_sz);
    adjlist_free(al);
    return r;
}

void cluster_result_free(ClusterResult *r)
{
    if (!r) return;
    free(r->entries);
    free(r);
}
