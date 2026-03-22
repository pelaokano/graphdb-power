"""
graphdb_algorithms.py

Python analysis layer for graphdb. Builds matrices from the graph
and runs algorithms that require linear algebra or are already
implemented in NetworkX / SciPy.

Dependencies: numpy, scipy, networkx
Optional:     python-louvain (community detection)
"""

import json
import numpy as np
import scipy.sparse as sp
import scipy.sparse.linalg as spla
import networkx as nx


class GraphAnalyzer:
    """
    High-level analysis layer on top of a graphdb.Graph instance.

    All methods accept an optional rel_type filter so they work on
    specific edge types (e.g. 'LINE', 'TRANSFORMER') or on the full
    graph (rel_type=None).
    """

    def __init__(self, graph):
        self.g = graph

    # ----------------------------------------------------------------
    # Internal: extract topology from the graph into numpy/networkx
    # ----------------------------------------------------------------

    def _load_edges(self, rel_type=None, weight_key=None):
        """
        Returns list of (src_id, dst_id, rel_id, weight) tuples.
        Uses RETURN a, r, b where bare variables resolve to DB ids.
        """
        if rel_type:
            match_q = f"MATCH (a)-[r:{rel_type}]->(b) RETURN a, r, b"
        else:
            match_q = "MATCH (a)-[r]->(b) RETURN a, r, b"

        rows = self.g.query(match_q)
        edges = []
        for row in rows:
            parts = row.split(" | ")
            if len(parts) < 3:
                continue
            try:
                src = int(parts[0])
                rid = int(parts[1])
                dst = int(parts[2])
            except ValueError:
                continue
            w = 1.0
            if weight_key:
                rel = self.g.get_rel(rid)
                if rel:
                    try:
                        props = json.loads(rel["properties"])
                        raw = props.get(weight_key)
                        if raw is not None:
                            w = float(raw)
                            if w == 0.0:
                                w = 1.0
                    except (ValueError, KeyError):
                        pass
            edges.append((src, dst, rid, w))
        return edges

    def _load_nodes(self, label=None, rel_type=None):
        """
        Returns sorted list of node_ids.
        If rel_type is given, only returns nodes that appear in
        at least one edge of that type (avoids polluting matrices
        with unrelated node types like generators in a LINE query).
        """
        if rel_type:
            # collect nodes referenced by this edge type
            rows_src = self.g.query(f"MATCH (a)-[r:{rel_type}]->(b) RETURN a")
            rows_dst = self.g.query(f"MATCH (a)-[r:{rel_type}]->(b) RETURN b")
            ids = set()
            for r in rows_src + rows_dst:
                try:
                    ids.add(int(r))
                except ValueError:
                    pass
            return sorted(ids)

        if label:
            rows = self.g.query(f"MATCH (n:{label}) RETURN n")
        else:
            rows = self.g.query("MATCH (n) RETURN n")
        result = []
        for r in rows:
            try:
                result.append(int(r))
            except ValueError:
                pass
        return result

    def _build_networkx(self, rel_type=None, weight_key=None,
                        undirected=True):
        edges    = self._load_edges(rel_type, weight_key)
        node_ids = self._load_nodes(rel_type=rel_type)
        G = nx.Graph() if undirected else nx.DiGraph()
        G.add_nodes_from(node_ids)
        for src, dst, rid, w in edges:
            G.add_edge(src, dst, rel_id=rid, weight=w)
        return G

    def _node_index(self, node_ids):
        """Returns dict {node_id: row_index}."""
        return {nid: i for i, nid in enumerate(node_ids)}

    # ----------------------------------------------------------------
    # Matrices
    # ----------------------------------------------------------------

    def adjacency_matrix(self, rel_type=None, undirected=True):
        """
        Returns (A, node_ids) where A is a scipy sparse CSR matrix
        and node_ids[i] maps row i to the actual graph node id.
        """
        node_ids = self._load_nodes(rel_type=rel_type)
        idx      = self._node_index(node_ids)
        n        = len(node_ids)
        edges    = self._load_edges(rel_type)

        rows, cols, data = [], [], []
        for src, dst, _rid, _w in edges:
            if src not in idx or dst not in idx:
                continue
            i, j = idx[src], idx[dst]
            rows.append(i); cols.append(j); data.append(1)
            if undirected and i != j:
                rows.append(j); cols.append(i); data.append(1)

        A = sp.csr_matrix((data, (rows, cols)), shape=(n, n), dtype=float)
        return A, node_ids

    def incidence_matrix(self, rel_type=None):
        """
        Returns (C, node_ids, rel_ids) where C is a scipy sparse CSR
        matrix of shape (N_nodes x N_edges).
        C[i, l] = +1 if edge l leaves node i, -1 if enters.
        """
        node_ids = self._load_nodes(rel_type=rel_type)
        idx      = self._node_index(node_ids)
        edges    = self._load_edges(rel_type)
        n        = len(node_ids)
        m        = len(edges)
        rel_ids  = [e[2] for e in edges]

        rows, cols, data = [], [], []
        for l, (src, dst, _rid, _w) in enumerate(edges):
            if src in idx:
                rows.append(idx[src]); cols.append(l); data.append(1.0)
            if dst in idx:
                rows.append(idx[dst]); cols.append(l); data.append(-1.0)

        C = sp.csr_matrix((data, (rows, cols)), shape=(n, m), dtype=float)
        return C, node_ids, rel_ids

    def laplacian_matrix(self, rel_type=None):
        """
        Returns (L, node_ids) where L = D - A (unweighted Laplacian).
        """
        A, node_ids = self.adjacency_matrix(rel_type, undirected=True)
        degree      = np.asarray(A.sum(axis=1)).flatten()
        D           = sp.diags(degree)
        L           = D - A
        return L.tocsr(), node_ids

    def weighted_laplacian_matrix(self, rel_type=None, weight_key="x_pu"):
        """
        Returns (L_w, node_ids) where edge weights are admittances
        (1 / weight_key). Used to build Ybus for DC power flow.
        """
        node_ids = self._load_nodes(rel_type=rel_type)
        idx      = self._node_index(node_ids)
        n        = len(node_ids)
        edges    = self._load_edges(rel_type, weight_key)

        rows, cols, data = [], [], []
        degree = np.zeros(n)

        for src, dst, _rid, w in edges:
            if src not in idx or dst not in idx:
                continue
            i, j   = idx[src], idx[dst]
            admitt = 1.0 / w if w != 0 else 0.0
            rows.append(i); cols.append(j); data.append(-admitt)
            rows.append(j); cols.append(i); data.append(-admitt)
            degree[i] += admitt
            degree[j] += admitt

        rows += list(range(n)); cols += list(range(n))
        data += list(degree)

        L = sp.csr_matrix((data, (rows, cols)), shape=(n, n), dtype=float)
        return L, node_ids

    def ybus_matrix(self, rel_type="LINE", weight_key="x_pu"):
        """
        Alias for weighted_laplacian_matrix with electrical defaults.
        Returns (Ybus, node_ids).
        """
        return self.weighted_laplacian_matrix(rel_type, weight_key)

    # ----------------------------------------------------------------
    # Spectral analysis
    # ----------------------------------------------------------------

    def fiedler_value(self, rel_type=None):
        """
        Second smallest eigenvalue of the Laplacian (algebraic connectivity).
        Near 0 = fragile network. Returns float.
        """
        L, node_ids = self.laplacian_matrix(rel_type)
        # remove isolated nodes (zero rows) before eigendecomposition
        degree = np.asarray(L.diagonal()).flatten()
        mask   = degree > 0
        if mask.sum() < 2:
            return 0.0
        L_sub = L[mask][:, mask]
        n     = L_sub.shape[0]
        k     = min(3, n - 1)
        try:
            vals = spla.eigsh(L_sub.astype(float), k=k, which="SM",
                              return_eigenvectors=False, tol=1e-6)
            vals = np.sort(np.real(vals))
            return float(vals[1]) if len(vals) > 1 else 0.0
        except Exception:
            # fallback: dense eigendecomposition
            vals = np.linalg.eigvalsh(L_sub.toarray())
            vals = np.sort(np.real(vals))
            return float(vals[1]) if len(vals) > 1 else 0.0

    def fiedler_vector(self, rel_type=None):
        """
        Eigenvector of the second smallest Laplacian eigenvalue.
        Sign of each component indicates partition membership.
        Returns (vector, node_ids) for connected nodes only.
        """
        L, node_ids = self.laplacian_matrix(rel_type)
        degree = np.asarray(L.diagonal()).flatten()
        mask   = degree > 0
        active_ids = [nid for nid, m in zip(node_ids, mask) if m]
        n      = sum(mask)
        if n < 2:
            return np.zeros(n), active_ids
        L_sub  = L[mask][:, mask]
        k      = min(3, n - 1)
        try:
            vals, vecs = spla.eigsh(L_sub.astype(float), k=k,
                                    which="SM", tol=1e-6)
        except Exception:
            vals, vecs = np.linalg.eigh(L_sub.toarray())
        order   = np.argsort(np.real(vals))
        fiedler = np.real(vecs[:, order[1]]) if len(vals) > 1 else vecs[:, 0]
        return fiedler, active_ids

    def spectral_partition(self, n_clusters=2, rel_type=None):
        """
        Partition nodes into n_clusters groups using the Fiedler
        vector (n_clusters=2) or sklearn spectral clustering.

        Returns {node_id: cluster_index}.
        """
        if n_clusters == 2:
            vec, node_ids = self.fiedler_vector(rel_type)
            median        = np.median(vec)
            return {nid: int(vec[i] >= median)
                    for i, nid in enumerate(node_ids)}

        try:
            from sklearn.cluster import SpectralClustering
            A, node_ids = self.adjacency_matrix(rel_type)
            sc = SpectralClustering(n_clusters=n_clusters,
                                    affinity="precomputed",
                                    random_state=0)
            labels = sc.fit_predict(A.toarray())
            return {nid: int(labels[i]) for i, nid in enumerate(node_ids)}
        except ImportError:
            raise ImportError("spectral_partition with n_clusters>2 requires scikit-learn")

    # ----------------------------------------------------------------
    # Centrality
    # ----------------------------------------------------------------

    def degree_centrality(self, rel_type=None):
        """
        Returns {node_id: degree_centrality} normalized by (N-1).
        """
        G = self._build_networkx(rel_type)
        return dict(nx.degree_centrality(G))

    def betweenness_centrality(self, rel_type=None, weight_key=None,
                               normalized=True):
        """
        Fraction of shortest paths that pass through each node/edge.
        High betweenness = topological bottleneck.

        Returns {node_id: betweenness}.
        """
        G = self._build_networkx(rel_type, weight_key)
        w = "weight" if weight_key else None
        return dict(nx.betweenness_centrality(G, weight=w,
                                              normalized=normalized))

    def edge_betweenness_centrality(self, rel_type=None, weight_key=None):
        """
        Betweenness centrality for edges (lines).
        Returns {(src_id, dst_id): betweenness}.
        """
        G = self._build_networkx(rel_type, weight_key)
        w = "weight" if weight_key else None
        return dict(nx.edge_betweenness_centrality(G, weight=w))

    def closeness_centrality(self, rel_type=None, weight_key=None):
        """
        Inverse of average shortest path distance to all other nodes.
        High closeness = best candidate for BESS siting.

        Returns {node_id: closeness}.
        """
        G = self._build_networkx(rel_type, weight_key)
        w = "weight" if weight_key else None
        return dict(nx.closeness_centrality(G, distance=w))

    def pagerank(self, rel_type=None, weight_key=None, alpha=0.85):
        """
        PageRank weighted by edge weights (admittances).
        Returns {node_id: pagerank}.
        """
        G = self._build_networkx(rel_type, weight_key, undirected=False)
        w = "weight" if weight_key else None
        return dict(nx.pagerank(G, alpha=alpha, weight=w))

    # ----------------------------------------------------------------
    # Network flow
    # ----------------------------------------------------------------

    def max_flow(self, src_id, dst_id, rel_type=None,
                 capacity_key="rate_mva"):
        """
        Maximum flow from src_id to dst_id using edge capacities.
        Returns (flow_value, flow_dict).
        """
        G = self._build_networkx(rel_type, capacity_key, undirected=False)

        # rename weight -> capacity for networkx max_flow
        for u, v, d in G.edges(data=True):
            d["capacity"] = d.get("weight", 1.0)

        flow_val, flow_dict = nx.maximum_flow(G, src_id, dst_id,
                                              capacity="capacity")
        return flow_val, flow_dict

    def min_cut(self, src_id, dst_id, rel_type=None,
                capacity_key="rate_mva"):
        """
        Minimum cut between src_id and dst_id.
        Returns (cut_value, (set_S, set_T)).
        """
        G = self._build_networkx(rel_type, capacity_key, undirected=False)
        for u, v, d in G.edges(data=True):
            d["capacity"] = d.get("weight", 1.0)

        cut_val, partition = nx.minimum_cut(G, src_id, dst_id,
                                            capacity="capacity")
        return cut_val, partition

    # ----------------------------------------------------------------
    # Path algorithms
    # ----------------------------------------------------------------

    def k_shortest_paths(self, src_id, dst_id, k,
                         rel_type=None, weight_key=None):
        """
        K shortest simple paths using Yen's algorithm.
        Returns list of k paths, each a list of node_ids.
        """
        G = self._build_networkx(rel_type, weight_key)
        w = "weight" if weight_key else None
        paths = list(nx.shortest_simple_paths(G, src_id, dst_id, weight=w))
        return paths[:k]

    def all_pairs_shortest_path_length(self, rel_type=None,
                                        weight_key=None):
        """
        Returns dict {src_id: {dst_id: distance}} for all pairs.
        Uses Floyd-Warshall via NetworkX. Use with caution on large graphs.
        """
        G = self._build_networkx(rel_type, weight_key)
        w = "weight" if weight_key else None
        if w:
            return dict(nx.all_pairs_dijkstra_path_length(G, weight=w))
        return dict(nx.all_pairs_shortest_path_length(G))

    def graph_diameter(self, rel_type=None, weight_key=None):
        """
        Longest shortest path in the graph.
        Returns float (inf if disconnected).
        """
        G = self._build_networkx(rel_type, weight_key)
        if not nx.is_connected(G):
            return float("inf")
        w = "weight" if weight_key else None
        return nx.diameter(G) if not w else max(
            max(d.values())
            for d in nx.all_pairs_dijkstra_path_length(G, weight=w)
        )

    # ----------------------------------------------------------------
    # Community detection
    # ----------------------------------------------------------------

    def community_detection(self, rel_type=None, weight_key=None):
        """
        Louvain community detection.
        Requires: pip install python-louvain

        Returns {node_id: community_index}.
        """
        try:
            import community as louvain
        except ImportError:
            raise ImportError(
                "community_detection requires python-louvain: "
                "pip install python-louvain"
            )

        G = self._build_networkx(rel_type, weight_key)
        w = "weight" if weight_key else None
        partition = louvain.best_partition(G, weight=w)
        return partition

    def greedy_modularity_communities(self, rel_type=None):
        """
        Community detection without external dependencies.
        Uses NetworkX greedy modularity optimization.
        Returns list of sets of node_ids.
        """
        G = self._build_networkx(rel_type)
        communities = nx.community.greedy_modularity_communities(G)
        return [set(c) for c in communities]

    # ----------------------------------------------------------------
    # Power system specific
    # ----------------------------------------------------------------

    def bess_siting_ranking(self, rel_type="LINE", weight_key="x_pu",
                             n=10):
        """
        Rank buses as BESS candidates by combining closeness centrality
        (proximity to loads) and betweenness centrality (congestion
        relief potential).

        Returns list of (node_id, score) sorted by descending score.
        """
        close = self.closeness_centrality(rel_type, weight_key)
        betw  = self.betweenness_centrality(rel_type, weight_key)

        all_ids = set(close) | set(betw)
        scores  = {}
        for nid in all_ids:
            c = close.get(nid, 0.0)
            b = betw.get(nid, 0.0)
            scores[nid] = 0.5 * c + 0.5 * b

        ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return ranked[:n]

    def congestion_risk(self, rel_type="LINE"):
        """
        Identifies lines with high topological congestion risk using
        edge betweenness centrality. A high-betweenness line carries
        many shortest paths and is structurally prone to congestion.

        Returns list of (rel_id, src_id, dst_id, betweenness) sorted
        descending.
        """
        ebc   = self.edge_betweenness_centrality(rel_type)
        edges = self._load_edges(rel_type)

        edge_to_rid = {(src, dst): rid for src, dst, rid, _ in edges}

        result = []
        for (src, dst), btw in ebc.items():
            rid = edge_to_rid.get((src, dst)) or edge_to_rid.get((dst, src))
            result.append((rid, src, dst, btw))

        return sorted(result, key=lambda x: x[3], reverse=True)

    def network_robustness(self, rel_type=None):
        """
        Returns a summary dict with key robustness metrics:
            fiedler_value      : algebraic connectivity
            diameter           : longest shortest path
            n_bridges          : number of critical lines
            n_articulation_pts : number of critical buses
            is_connected       : bool
        """
        return {
            "fiedler_value":       self.fiedler_value(rel_type),
            "diameter":            self.graph_diameter(rel_type),
            "n_bridges":           len(self.g.find_bridges(rel_type=rel_type)),
            "n_articulation_pts":  len(self.g.find_articulation_points(rel_type=rel_type)),
            "is_connected":        bool(self.g.is_connected(rel_type=rel_type)),
        }
