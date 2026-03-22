#ifndef GRAPH_ALGORITHMS_H
#define GRAPH_ALGORITHMS_H

#include <stdint.h>
#include "graphdb.h"

/* ------------------------------------------------------------------ */
/*  Connected components                                                */
/* ------------------------------------------------------------------ */

typedef struct {
    int64_t *node_ids;       /* flat array of all node ids             */
    int     *component;      /* component[i] = component index of node_ids[i] */
    int      node_count;
    int      component_count;
} ComponentResult;

ComponentResult *algo_connected_components(Graph *g, const char *rel_type);
void             component_result_free(ComponentResult *r);
int              algo_is_connected(Graph *g, const char *rel_type);

/* ------------------------------------------------------------------ */
/*  Bridges and articulation points (Tarjan DFS)                       */
/* ------------------------------------------------------------------ */

typedef struct {
    int64_t *ids;
    int      count;
} IdList;

IdList *algo_find_bridges(Graph *g, const char *rel_type);
IdList *algo_find_articulation_points(Graph *g, const char *rel_type);
void    idlist_free(IdList *l);

/* ------------------------------------------------------------------ */
/*  N-1 topological check                                               */
/* ------------------------------------------------------------------ */

/* returns 1 if removing rel_id disconnects the graph, 0 otherwise */
int algo_n1_is_critical(Graph *g, int64_t rel_id);
int algo_n1_is_critical_v2(Graph *g, int64_t rel_id);

/* ------------------------------------------------------------------ */
/*  Dijkstra shortest path (weighted)                                   */
/* ------------------------------------------------------------------ */

typedef struct {
    int64_t  node_id;
    int64_t  prev_node_id;   /* -1 for source */
    int64_t  prev_rel_id;    /* -1 for source */
    double   dist;           /* total cost from source */
} DijkstraNode;

typedef struct {
    DijkstraNode *nodes;
    int           count;
    char          weight_key[64]; /* property used as edge weight */
} DijkstraResult;

/* weight_key: property name on relationships, e.g. "x_pu", "length_km"
   rel_type: NULL means any type
   Returns NULL if src not found. */
DijkstraResult *algo_dijkstra(Graph *g, int64_t src,
                               const char *rel_type,
                               const char *weight_key);
void            dijkstra_result_free(DijkstraResult *r);

/* ------------------------------------------------------------------ */
/*  Adjacency list (used internally and exported for matrix builder)   */
/* ------------------------------------------------------------------ */

typedef struct AdjEdge {
    int64_t        rel_id;
    int64_t        dst_node_id;
    double         weight;
    struct AdjEdge *next;
} AdjEdge;

typedef struct {
    int64_t  node_id;
    AdjEdge *head;
} AdjNode;

typedef struct {
    AdjNode *nodes;
    int      node_count;
    int      edge_count;
} AdjList;

AdjList *adjlist_build(Graph *g, const char *rel_type,
                       const char *weight_key, int undirected);
void     adjlist_free(AdjList *al);

/* find index of node_id in adjlist (-1 if not found) */
int adjlist_index(AdjList *al, int64_t node_id);

/* ------------------------------------------------------------------ */
/*  Degree sequence                                                     */
/* ------------------------------------------------------------------ */

typedef struct {
    int64_t node_id;
    int     degree;
} DegreeEntry;

typedef struct {
    DegreeEntry *entries;
    int          count;
} DegreeResult;

DegreeResult *algo_degree(Graph *g, const char *rel_type);
void          degree_result_free(DegreeResult *r);

#endif /* GRAPH_ALGORITHMS_H */
