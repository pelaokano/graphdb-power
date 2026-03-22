# graphdb-power

Embedded graph database with SQLite backend, Cypher query support, and graph algorithms for power systems analysis.

Built with a C core for high performance, backed by SQLite for full ACID transactions, and exposed to Python through a clean API.

## Features

- **Cypher queries**: MATCH, CREATE, MERGE, SET, DELETE, WHERE, ORDER BY, LIMIT, SKIP, UNION and more
- **Graph algorithms in C**: connected components, bridges, articulation points, N-1 topological check, Dijkstra, degree
- **Analysis layer**: adjacency matrix, Laplacian, Ybus, betweenness/closeness/pagerank centrality, max-flow, min-cut, spectral partition, community detection
- **Power systems helpers**: BESS siting ranking, congestion risk, network robustness metrics
- **SQLite backend**: WAL mode, ACID transactions, JSON properties on nodes and relationships
- **No external dependencies for the core**: SQLite is bundled

## Requirements

- Windows 64-bit
- Python 3.13
- numpy, scipy, networkx (for GraphAnalyzer)

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

    # graph algorithms (C layer)
    print(g.is_connected(rel_type="LINE"))
    print(g.find_bridges(rel_type="LINE"))
    print(g.find_articulation_points(rel_type="LINE"))
    print(g.dijkstra(b1, rel_type="LINE", weight_key="x_pu"))

    # analysis layer (Python + NumPy/SciPy/NetworkX)
    ana = GraphAnalyzer(g)

    A, node_ids = ana.adjacency_matrix(rel_type="LINE")
    Y, node_ids = ana.ybus_matrix(rel_type="LINE", weight_key="x_pu")

    print(ana.fiedler_value(rel_type="LINE"))
    print(ana.betweenness_centrality(rel_type="LINE"))
    print(ana.bess_siting_ranking(rel_type="LINE", weight_key="x_pu", n=5))
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

## License

MIT License. Copyright (c) 2026 Adrian Alarcon.
