# graphdb-power

Embedded graph database with SQLite backend, Cypher query support, and graph algorithms for power systems analysis.

Built with a C core for high performance, backed by SQLite for full ACID transactions, and exposed to Python through a clean API.

## Features

- **Cypher queries**: MATCH, CREATE, MERGE, SET, DELETE, WHERE, ORDER BY, LIMIT, SKIP, UNION and more
- **Graph algorithms in C**: connected components, bridges, articulation points, N-1 check, Dijkstra, degree, betweenness centrality (Brandes), PageRank, max flow (Edmonds-Karp), k-shortest paths (Yen's), SCC (Kosaraju), clustering coefficient
- **Analysis layer** (`GraphAnalyzer`): adjacency matrix, Laplacian, Ybus, spectral partition, community detection (Louvain), vulnerability index, electrical distance, BESS placement ranking
- **Power systems** (`GraphPTDF`, `GraphLODF`): PTDF sensitivity matrix with top-K persistence, N-1 LODF contingency analysis with verdicts (critico/advertencia/seguro/puente/singular)
- **MCP server** (`graphdb_mcp.py`): 34 tools via FastMCP exposing the full analysis suite to Claude Desktop and Cursor
- **Interactive visualizer**: pywebview desktop app with Cytoscape.js, algorithm panel, PTDF/LODF interface
- **SQLite backend**: WAL mode, ACID transactions, JSON properties on nodes and relationships
- **No external dependencies for the core**: SQLite is bundled

## Requirements

- Windows 64-bit
- Python 3.13
- numpy, scipy, networkx (for `GraphAnalyzer`)
- fastmcp (optional, for MCP server)
- pywebview (optional, for the visualizer)

## Installation

```bash
pip install graphdb-power
```

## Quick start

```python
import graphdb
from graphdb_algorithms import GraphAnalyzer

with graphdb.Graph("my_network.db") as g:

    # create nodes
    b1 = g.create_node("Bus", '{"name": "B1", "vbase_kv": 138}')
    b2 = g.create_node("Bus", '{"name": "B2", "vbase_kv": 138}')
    b3 = g.create_node("Bus", '{"name": "B3", "vbase_kv": 138}')

    # create relationships
    g.create_rel(b1, b2, "LINE", '{"x_pu": 0.10, "rate_mva": 200}')
    g.create_rel(b2, b3, "LINE", '{"x_pu": 0.08, "rate_mva": 150}')
    g.create_rel(b1, b3, "LINE", '{"x_pu": 0.20, "rate_mva": 100}')

    # Cypher queries
    rows = g.query("MATCH (n:Bus) RETURN n.name, n.vbase_kv")
    rows = g.query("""
        MATCH (a:Bus)-[r:LINE]->(b:Bus)
        WHERE r.x_pu < 0.15
        RETURN a.name, b.name, r.x_pu
        ORDER BY r.x_pu
    """)

    # graph algorithms — C layer
    print(g.is_connected(rel_type="LINE"))
    print(g.find_bridges(rel_type="LINE"))
    print(g.find_articulation_points(rel_type="LINE"))
    print(g.dijkstra(b1, rel_type="LINE", weight_key="x_pu"))

    # new in v0.3.0 — C-accelerated algorithms
    bc  = g.betweenness_centrality(rel_type="LINE")   # Brandes O(VE)
    pr  = g.pagerank(rel_type="LINE", damping=0.85)
    mf  = g.max_flow(b1, b3, rel_type="LINE")          # Edmonds-Karp
    ksp = g.k_shortest_paths(b1, b3, k=3, rel_type="LINE")  # Yen's
    scc = g.strongly_connected_components(rel_type="LINE")   # Kosaraju
    cc  = g.clustering_coefficient(rel_type="LINE")

    # analysis layer (Python + NumPy/SciPy/NetworkX)
    ana = GraphAnalyzer(g)

    A, node_ids = ana.adjacency_matrix(rel_type="LINE")
    Y, node_ids = ana.ybus_matrix(rel_type="LINE", weight_key="x_pu")

    print(ana.fiedler_value(rel_type="LINE"))
    print(ana.community_detection(rel_type="LINE"))
    print(ana.vulnerability_index(rel_type="LINE"))
    print(ana.electrical_distance(rel_type="LINE"))
    print(ana.bess_placement_ranking(rel_type="LINE", weight_key="x_pu"))
    print(ana.network_robustness(rel_type="LINE"))
```

## Loading PSS/E networks

```python
from read_raw2 import RawParser
from base_grafo import GraphDbLoader

parser = RawParser()
parser.leer_archivo("IEEE 118 Bus v2.raw")
data = parser.obtener_dataframes()

with GraphDbLoader("ieee118.db") as loader:
    loader.limpiar_base_de_datos()
    loader.cargar_datos(data)
    loader.resumen()
```

## PTDF / LODF (power systems contingency analysis)

```python
import pandas as pd
from graphdb_ptdf import GraphPTDF
from graphdb_lodf import GraphLODF

# ptdf_df: DataFrame (n_branches x n_buses) pre-calculated by PTDFCalculator
gptdf = GraphPTDF(g, ptdf_df, rel_type="LINEA", persist_topk=True)
glodf = GraphLODF(gptdf)

# N-1 contingency analysis
results = glodf.contingency_n1(monitored_label="L 1-2")
for row in results.itertuples():
    print(row.outage, row.delta_flow_pct, row.verdict)

# Worst-case outage for a monitored line
print(glodf.worst_case_outage("L 1-2"))
```

## MCP server (Claude Desktop / Cursor integration)

```bash
pip install fastmcp
python graphdb_mcp.py
```

Exposes 34 tools covering: graph topology, centrality, shortest paths, max flow,
community detection, global metrics, power system analysis, and PTDF/LODF.

## Building from source

Requires Visual Studio Build Tools 2022 and the SQLite amalgamation (`sqlite3.h` + `sqlite3.c`
are bundled in the repository).

```bash
python setup.py build_ext --inplace
```

## Changelog

### v0.3.0
- C layer: added Brandes betweenness, PageRank, Edmonds-Karp max flow, Yen's k-shortest-paths, Kosaraju SCC, clustering coefficient
- New `GraphPTDF` module: integrates PTDF sensitivity matrix with the graph DB
- New `GraphLODF` module: N-1 contingency analysis with severity verdicts
- New `graphdb_mcp.py`: 34 MCP tools for AI assistant integration
- `GraphAnalyzer`: added `vulnerability_index`, `electrical_distance`, `bess_placement_ranking` and more
- Visualizer: algorithm panel with 15+ algorithm buttons, PTDF/LODF interface

### v0.2.0
- Initial public release with Cypher support, basic C algorithms, and PSS/E loader

## License

MIT License. Copyright (c) 2026 Adrian Alarcon.
