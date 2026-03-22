import graphdb
from graphdb_algorithms import GraphAnalyzer
import json

with graphdb.Graph("test.db") as g:

    # crear red de 4 barras
    b1 = g.create_node("Bus", '{"name":"B1","vbase_kv":138}')
    b2 = g.create_node("Bus", '{"name":"B2","vbase_kv":138}')
    b3 = g.create_node("Bus", '{"name":"B3","vbase_kv":138}')
    b4 = g.create_node("Bus", '{"name":"B4","vbase_kv":138}')

    g.create_rel(b1, b2, "LINE", '{"x_pu":0.10,"rate_mva":200}')
    g.create_rel(b2, b3, "LINE", '{"x_pu":0.08,"rate_mva":200}')
    g.create_rel(b3, b4, "LINE", '{"x_pu":0.05,"rate_mva":150}')
    g.create_rel(b1, b4, "LINE", '{"x_pu":0.20,"rate_mva":100}')

    # algoritmos C
    print("conectada:", g.is_connected(rel_type="LINE"))
    print("puentes:",   g.find_bridges(rel_type="LINE"))
    print("APs:",       g.find_articulation_points(rel_type="LINE"))
    print("grados:",    g.degree(rel_type="LINE"))

    dij = g.dijkstra(b1, rel_type="LINE", weight_key="x_pu")
    for r in dij:
        if r["dist"] >= 0:
            print(f"  B1->node {r['node_id']}: {r['dist']:.4f}")

    # algoritmos Python
    ana = GraphAnalyzer(g)
    print("robustez:", ana.network_robustness(rel_type="LINE"))
    print("BESS ranking:", ana.bess_siting_ranking(rel_type="LINE", n=4))

    A, nids = ana.adjacency_matrix(rel_type="LINE")
    print("matriz adyacencia:\n", A.toarray())