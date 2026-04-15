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

    # ================================================================
    # NEW METHODS — appended, no existing code modified
    # ================================================================

    # ----------------------------------------------------------------
    # C-accelerated centralities (call C layer directly)
    # ----------------------------------------------------------------

    def betweenness_centrality_c(self, rel_type=None, normalized=True):
        """
        Betweenness centrality via C (Brandes algorithm).
        Significantly faster than the Python/NetworkX version for
        large graphs. Returns {node_id: float}.
        """
        try:
            return self.g.betweenness_centrality(rel_type=rel_type,
                                                  normalized=int(normalized))
        except AttributeError:
            # fallback to Python implementation if C not compiled yet
            return self.betweenness_centrality(rel_type, None)

    def pagerank_c(self, rel_type=None, damping=0.85, max_iter=100, tol=1e-6):
        """
        PageRank via C (power iteration).
        Returns {node_id: float}.
        """
        try:
            return self.g.pagerank(rel_type=rel_type, damping=damping,
                                    max_iter=max_iter, tol=tol)
        except AttributeError:
            return self.pagerank(rel_type)

    def max_flow_c(self, src, dst, rel_type=None, capacity_key="rate_A_MVA"):
        """
        Max flow between src and dst via C (Edmonds-Karp).
        Returns float (flow value).
        """
        try:
            return self.g.max_flow(src, dst, rel_type=rel_type,
                                    capacity_key=capacity_key)
        except AttributeError:
            fval, _ = self.max_flow(src, dst, rel_type, capacity_key)
            return fval

    def k_shortest_paths_c(self, src, dst, k=3, rel_type=None, weight_key=None):
        """
        K shortest paths via C (Yen's algorithm).
        Returns list of {nodes: [node_id,...], cost: float}.
        """
        try:
            return self.g.k_shortest_paths(src, dst, k=k, rel_type=rel_type,
                                            weight_key=weight_key)
        except AttributeError:
            return self.k_shortest_paths(src, dst, k=k, rel_type=rel_type,
                                          weight_key=weight_key)

    def strongly_connected_components(self, rel_type=None):
        """
        Strongly connected components via C (Kosaraju).
        Returns ({node_id: component_index}, n_components).
        """
        try:
            return self.g.strongly_connected_components(rel_type=rel_type)
        except AttributeError:
            G   = self._build_networkx(rel_type)
            sccs = list(nx.strongly_connected_components(G.to_directed()))
            comp_map = {}
            for idx, scc in enumerate(sccs):
                for nid in scc:
                    comp_map[nid] = idx
            return comp_map, len(sccs)

    def clustering_coefficient_c(self, rel_type=None):
        """
        Clustering coefficient via C (triangle counting).
        Returns {local: {node_id: float|None}, global: float}.
        """
        try:
            return self.g.clustering_coefficient(rel_type=rel_type)
        except AttributeError:
            G   = self._build_networkx(rel_type)
            loc = nx.clustering(G)
            glb = nx.transitivity(G)
            return {"local": loc, "global": glb}

    # ----------------------------------------------------------------
    # New Python-only metrics
    # ----------------------------------------------------------------

    def assortativity(self, rel_type=None):
        """
        Degree assortativity coefficient.
        Positive  -> high-degree nodes tend to connect to high-degree nodes.
        Negative  -> hubs connect to low-degree nodes (hub-and-spoke).
        Returns float in [-1, 1].
        """
        G = self._build_networkx(rel_type)
        try:
            return nx.degree_assortativity_coefficient(G)
        except Exception:
            return float("nan")

    def density(self, rel_type=None):
        """
        Graph density: ratio of actual edges to maximum possible edges.
        Returns float in [0, 1].
        """
        G = self._build_networkx(rel_type)
        return nx.density(G)

    def average_shortest_path_length(self, rel_type=None, weight_key=None):
        """
        Average shortest path length over all connected node pairs.
        Returns float, or None if graph is not connected.
        """
        G = self._build_networkx(rel_type, weight_key)
        w = "weight" if weight_key else None
        try:
            return nx.average_shortest_path_length(G, weight=w)
        except nx.NetworkXError:
            # not connected: compute on largest component
            largest = max(nx.connected_components(G), key=len)
            sub = G.subgraph(largest)
            return nx.average_shortest_path_length(sub, weight=w)

    def eccentricity(self, rel_type=None):
        """
        Eccentricity of each node: max shortest path to any other node.
        Returns {node_id: int}.
        """
        G = self._build_networkx(rel_type)
        try:
            return dict(nx.eccentricity(G))
        except Exception:
            largest = max(nx.connected_components(G), key=len)
            return dict(nx.eccentricity(G.subgraph(largest)))

    def radius(self, rel_type=None):
        """
        Graph radius: min eccentricity (the most central node).
        Returns int or None.
        """
        ecc = self.eccentricity(rel_type)
        return min(ecc.values()) if ecc else None

    def periphery(self, rel_type=None):
        """
        Peripheral nodes: those with eccentricity equal to the diameter.
        Returns list of node_ids.
        """
        ecc  = self.eccentricity(rel_type)
        diam = max(ecc.values()) if ecc else 0
        return [nid for nid, e in ecc.items() if e == diam]

    def center(self, rel_type=None):
        """
        Central nodes: those with eccentricity equal to the radius.
        Returns list of node_ids.
        """
        ecc = self.eccentricity(rel_type)
        rad = min(ecc.values()) if ecc else 0
        return [nid for nid, e in ecc.items() if e == rad]

    def label_propagation_communities(self, rel_type=None):
        """
        Community detection via label propagation.
        No external dependencies beyond NetworkX.
        Returns list of sets of node_ids.
        """
        G = self._build_networkx(rel_type)
        communities = nx.community.label_propagation_communities(
            G.to_undirected())
        return [set(c) for c in communities]

    def rich_club_coefficient(self, rel_type=None):
        """
        Rich-club coefficient: tendency of high-degree nodes to be
        interconnected. Returns {degree: coefficient}.
        """
        G = self._build_networkx(rel_type)
        try:
            return dict(nx.rich_club_coefficient(G, normalized=False))
        except Exception:
            return {}

    def node_connectivity(self, rel_type=None):
        """
        Minimum node connectivity of the graph.
        Minimum number of nodes to remove to disconnect the graph.
        Returns int.
        """
        G = self._build_networkx(rel_type)
        try:
            return nx.node_connectivity(G)
        except Exception:
            return 0

    def edge_connectivity(self, rel_type=None):
        """
        Minimum edge connectivity.
        Minimum number of edges to remove to disconnect the graph.
        Returns int.
        """
        G = self._build_networkx(rel_type)
        try:
            return nx.edge_connectivity(G)
        except Exception:
            return 0

    def wiener_index(self, rel_type=None, weight_key=None):
        """
        Wiener index: sum of shortest path distances over all pairs.
        Measure of topological compactness.
        Returns float.
        """
        G = self._build_networkx(rel_type, weight_key)
        w = "weight" if weight_key else None
        total = 0.0
        try:
            for src in G.nodes():
                lengths = nx.single_source_shortest_path_length(G, src) \
                          if not w else \
                          nx.single_source_dijkstra_path_length(G, src, weight=w)
                total += sum(lengths.values())
            return total / 2.0
        except Exception:
            return float("nan")

    # ----------------------------------------------------------------
    # Power systems specific (new)
    # ----------------------------------------------------------------

    def vulnerability_index(self, rel_type=None):
        """
        Vulnerability index per edge: ratio of betweenness centrality
        to thermal rating (rate_A_MVA). Higher = more critical from
        both topological and electrical perspectives.

        Returns list of (rel_id, src, dst, index) sorted descending.
        """
        ebc   = self.edge_betweenness_centrality(rel_type)
        edges = self._load_edges(rel_type)

        result = []
        for src, dst, rid, _w in edges:
            rel = self.g.get_rel(rid)
            try:
                props = json.loads(rel["properties"]) if rel else {}
            except Exception:
                props = {}
            rate = props.get("rate_A_MVA", props.get("rate_mva", 1.0))
            if rate is None or rate == 0:
                rate = 1.0
            btw = ebc.get((src, dst), ebc.get((dst, src), 0.0))
            idx = btw / float(rate)
            result.append((rid, src, dst, round(idx, 6)))

        return sorted(result, key=lambda x: x[3], reverse=True)

    def electrical_distance(self, rel_type=None, weight_key="X_pu"):
        """
        Electrical distance matrix based on shortest path impedances.
        Returns (matrix NxN as np.ndarray, node_ids list).
        """
        node_ids = self._load_nodes(rel_type=rel_type)
        node_map = self._node_index(node_ids)
        edges    = self._load_edges(rel_type, weight_key)
        n = len(node_ids)
        D = np.full((n, n), np.inf)
        np.fill_diagonal(D, 0.0)

        for src, dst, rid, w in edges:
            i = node_map.get(src, -1)
            j = node_map.get(dst, -1)
            if i < 0 or j < 0:
                continue
            if w == 0:
                w = 1.0
            D[i, j] = min(D[i, j], float(w))
            D[j, i] = min(D[j, i], float(w))

        # Floyd-Warshall
        for k in range(n):
            for i in range(n):
                for j in range(n):
                    if D[i, k] + D[k, j] < D[i, j]:
                        D[i, j] = D[i, k] + D[k, j]

        return D, node_ids

    def _load_graph_data(self, rel_type=None):
        """Helper: returns (node_ids, node_map, edges)."""
        nodes    = self._load_nodes(rel_type=rel_type)
        node_map = self._node_index(nodes)
        edges    = self._load_edges(rel_type)
        return nodes, node_map, edges

    def _load_all_nodes_indexed(self):
        """Returns (node_ids_list, {node_id: index}, labels_dict)."""
        rows = self.g.query("MATCH (n) RETURN n")
        node_ids = []
        for row in rows:
            try:
                node_ids.append(int(row))
            except ValueError:
                pass
        node_map = {nid: i for i, nid in enumerate(node_ids)}
        return node_ids, node_map, {}

    def summary(self, rel_type=None):
        """
        Returns a comprehensive summary dict of the graph.
        Combines topological and structural metrics.
        """
        bridges = self.g.find_bridges(rel_type=rel_type)
        aps     = self.g.find_articulation_points(rel_type=rel_type)
        deg     = self.g.degree(rel_type=rel_type)

        deg_vals = list(deg.values())
        return {
            "n_nodes":             len(deg),
            "n_edges":             sum(deg_vals) // 2,
            "is_connected":        bool(self.g.is_connected(rel_type=rel_type)),
            "n_bridges":           len(bridges),
            "n_articulation_pts":  len(aps),
            "density":             self.density(rel_type),
            "fiedler_value":       self.fiedler_value(rel_type),
            "avg_degree":          sum(deg_vals) / len(deg_vals) if deg_vals else 0,
            "max_degree":          max(deg_vals) if deg_vals else 0,
            "clustering_global":   self.clustering_coefficient_c(rel_type).get("global", 0),
            "assortativity":       self.assortativity(rel_type),
        }

    # ================================================================
    # PTDF / LODF integration
    # ================================================================

    def ptdf_from_dataframe(self, ptdf_df, rel_type="LINEA",
                             persist_topk=True):
        """
        Crea un GraphPTDF a partir de una PTDF DataFrame pre-calculada
        (producida por PTDFCalculator o equivalente).

        Parameters
        ----------
        ptdf_df : pd.DataFrame
            Matriz PTDF shape (n_ramas, n_buses).
            Index = etiquetas de rama, columns = numeros de bus PSS/E.
        rel_type : str
            Tipo de relacion en graphdb que representa las ramas.
        persist_topk : bool
            Si True, escribe las top sensibilidades en la BD.

        Returns
        -------
        GraphPTDF
        """
        from graphdb_ptdf import GraphPTDF
        return GraphPTDF(
            graph=self.g,
            ptdf_df=ptdf_df,
            rel_type=rel_type,
            persist_topk=persist_topk,
        )

    def lodf_from_ptdf(self, graph_ptdf, base_flows_mw,
                        ratings_mw=None, lodf_tol=1e-6,
                        max_natural_overloads=2):
        """
        Calcula la LODF completa y el analisis de contingencias N-1 a partir
        de un GraphPTDF y los flujos base.

        Parameters
        ----------
        graph_ptdf : GraphPTDF
            Instancia de GraphPTDF con PTDF base ya cargada.
        base_flows_mw : pd.Series
            Flujos base en MW, indexados por etiqueta de rama.
        ratings_mw : pd.Series | None
            Capacidades termicas en MW. Si None, no aplica filtro de sobrecarga.
        lodf_tol : float
            Tolerancia para descartar contingencias singulares.
        max_natural_overloads : int
            Maximo de sobrecargas naturales post-contingencia aceptadas.

        Returns
        -------
        GraphLODF
        """
        from graphdb_lodf import GraphLODF
        return GraphLODF(
            graph_ptdf=graph_ptdf,
            base_flows_mw=base_flows_mw,
            ratings_mw=ratings_mw,
            lodf_tol=lodf_tol,
            max_natural_overloads=max_natural_overloads,
        )

    def contingency_screening(self, graph_lodf, n_top=20):
        """
        Screening de contingencias N-1: retorna las n_top contingencias
        mas criticas combinando LODF con cargabilidades actuales del grafo.

        Parameters
        ----------
        graph_lodf : GraphLODF
            Instancia de GraphLODF con contingencias pre-calculadas.
        n_top : int
            Numero de contingencias a retornar.

        Returns
        -------
        pd.DataFrame con columnas:
            rank, label, max_loading_pct, n_overloads, verdict
        """
        from graphdb_lodf import GraphLODF

        ranking = graph_lodf.ranking_contingencias_criticas(n_top)
        if ranking.empty:
            return ranking

        verdicts = []
        for _, row in ranking.iterrows():
            result = graph_lodf.verificar_n1_electrico(row["label"])
            verdicts.append(result.get("verdict", "seguro"))

        ranking = ranking.copy()
        ranking["verdict"] = verdicts
        return ranking
