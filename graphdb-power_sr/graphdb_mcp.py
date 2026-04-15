"""
graphdb_mcp.py

Servidor MCP (Model Context Protocol) para graphdb.

Expone ~28 herramientas organizadas en bloques:
    - Base de datos / topologia
    - Algoritmos topologicos C
    - Centralidades
    - Caminos
    - Flujo de red
    - Comunidades
    - Metricas globales
    - Sistemas de potencia

Uso:
    python graphdb_mcp.py [ruta.db]

Configuracion Claude Desktop (~/.config/Claude/claude_desktop_config.json):
    {
      "mcpServers": {
        "graphdb": {
          "command": "python",
          "args": ["C:/ruta/a/graphdb_mcp.py", "C:/ruta/a/red.db"],
          "env": {}
        }
      }
    }

Dependencias:
    pip install mcp
    pip install graphdb-power   (o compilar localmente con setup.py)
"""

import os
import sys
import json
import math
import argparse

# --- graphdb ---
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
try:
    import graphdb
    import graphdb_algorithms as ga
    HAS_GRAPHDB = True
except ImportError:
    HAS_GRAPHDB = False

# --- MCP ---
try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    sys.exit("Instala el paquete MCP: pip install mcp")


# -----------------------------------------------------------------------
# Estado global
# -----------------------------------------------------------------------

mcp = FastMCP("graphdb")

_g:   "graphdb.Graph | None"    = None
_ana: "ga.GraphAnalyzer | None" = None
_db_path: str                   = ""


def _require():
    if _g is None:
        raise RuntimeError("No hay base de datos abierta. "
                           "Usa la herramienta abrir_base_datos primero.")
    return _g, _ana


def _fmt_list(rows, limit=50):
    """Format a list of rows for display."""
    if not rows:
        return "(sin resultados)"
    out = []
    for r in rows[:limit]:
        out.append(str(r))
    if len(rows) > limit:
        out.append(f"... ({len(rows) - limit} mas)")
    return "\n".join(out)


def _top(mapping: dict, n: int = 15, reverse: bool = True):
    """Return top-n entries from a {id: value} dict."""
    return dict(sorted(mapping.items(),
                        key=lambda x: x[1],
                        reverse=reverse)[:n])


# -----------------------------------------------------------------------
# BLOQUE 1 — Base de datos
# -----------------------------------------------------------------------

@mcp.tool()
def abrir_base_datos(ruta: str) -> str:
    """
    Abre una base de datos graphdb (.db).
    Debe llamarse antes de cualquier otra herramienta.
    """
    global _g, _ana, _db_path
    if not HAS_GRAPHDB:
        return "ERROR: graphdb no esta instalado. Ejecuta: pip install graphdb-power"
    if not os.path.exists(ruta):
        return f"ERROR: archivo no encontrado: {ruta}"
    if _g is not None:
        _g.close()
    _g       = graphdb.Graph(ruta)
    _ana     = ga.GraphAnalyzer(_g)
    _db_path = ruta

    rows_n = _g.query("MATCH (n) RETURN count(n)")
    rows_r = _g.query("MATCH (a)-[r]->(b) RETURN count(r)")
    n = rows_n[0] if rows_n else 0
    r = rows_r[0] if rows_r else 0
    return f"BD abierta: {ruta}\nNodos: {n}  Aristas: {r}"


@mcp.tool()
def info_base_datos() -> str:
    """Muestra estadisticas generales de la base de datos activa."""
    g, _ = _require()
    rows_n = g.query("MATCH (n) RETURN count(n)")
    rows_r = g.query("MATCH (a)-[r]->(b) RETURN count(r)")
    n = rows_n[0] if rows_n else 0
    r = rows_r[0] if rows_r else 0
    return (f"Base de datos: {_db_path}\n"
            f"Nodos   : {n}\n"
            f"Aristas : {r}")


@mcp.tool()
def consulta_cypher(cypher: str) -> str:
    """
    Ejecuta una consulta Cypher de solo lectura.
    Ejemplos:
        MATCH (n:Bus) RETURN n.numero, n.nombre LIMIT 10
        MATCH (a:Bus)-[r:LINEA]->(b:Bus) RETURN a.numero, b.numero, r.X_pu
    """
    g, _ = _require()
    palabras_prohibidas = {"CREATE", "MERGE", "DELETE", "SET", "REMOVE", "DETACH"}
    tokens = set(cypher.upper().split())
    malas  = tokens & palabras_prohibidas
    if malas:
        return f"ERROR: consulta rechazada — contiene: {sorted(malas)}"
    try:
        rows = g.query(cypher)
        return _fmt_list(rows)
    except Exception as e:
        return f"ERROR: {e}"


@mcp.tool()
def listar_labels() -> str:
    """Lista todos los labels de nodos presentes en la BD."""
    g, _ = _require()
    rows  = g.query("MATCH (n) RETURN n")
    label_counts: dict = {}
    for row in rows:
        try:
            nid  = int(row)
            node = g.get_node(nid)
            if node:
                for lbl in node["labels"].split():
                    label_counts[lbl] = label_counts.get(lbl, 0) + 1
        except (ValueError, TypeError):
            pass
    if not label_counts:
        return "(sin labels)"
    return "\n".join(f"  {lbl}: {cnt}" for lbl, cnt in
                     sorted(label_counts.items(), key=lambda x: -x[1]))


@mcp.tool()
def listar_tipos_relacion() -> str:
    """Lista todos los tipos de relacion presentes en la BD con sus conteos."""
    g, _ = _require()
    rows  = g.query("MATCH (a)-[r]->(b) RETURN r")
    tipo_counts: dict = {}
    for row in rows:
        try:
            rid = int(row)
            rel = g.get_rel(rid)
            if rel:
                t = rel["type"]
                tipo_counts[t] = tipo_counts.get(t, 0) + 1
        except (ValueError, TypeError):
            pass
    if not tipo_counts:
        return "(sin relaciones)"
    return "\n".join(f"  {t}: {cnt}" for t, cnt in
                     sorted(tipo_counts.items(), key=lambda x: -x[1]))


@mcp.tool()
def obtener_nodo(node_id: int) -> str:
    """Retorna las propiedades completas de un nodo dado su id interno."""
    g, _ = _require()
    node  = g.get_node(node_id)
    if not node:
        return f"Nodo {node_id} no encontrado."
    props = json.loads(node["properties"])
    lines = [f"id     : {node_id}",
             f"labels : {node['labels']}"]
    for k, v in props.items():
        lines.append(f"  {k}: {v}")
    return "\n".join(lines)


@mcp.tool()
def obtener_relacion(rel_id: int) -> str:
    """Retorna las propiedades completas de una relacion dado su id."""
    g, _ = _require()
    rel   = g.get_rel(rel_id)
    if not rel:
        return f"Relacion {rel_id} no encontrada."
    props = json.loads(rel["properties"])
    lines = [f"id     : {rel_id}",
             f"tipo   : {rel['type']}",
             f"origen : {rel['src_id']}  ->  destino: {rel['dst_id']}"]
    for k, v in props.items():
        lines.append(f"  {k}: {v}")
    return "\n".join(lines)


# -----------------------------------------------------------------------
# BLOQUE 2 — Topologia (C)
# -----------------------------------------------------------------------

@mcp.tool()
def es_conectado(tipo_relacion: str = None) -> str:
    """
    Verifica si el grafo es conectado.
    tipo_relacion: filtrar por tipo (ej. 'LINEA'). None = todos.
    """
    g, _ = _require()
    val  = g.is_connected(rel_type=tipo_relacion)
    return f"Conectado: {val}"


@mcp.tool()
def componentes_conectadas(tipo_relacion: str = None) -> str:
    """
    Componentes debilmente conectadas (WCC).
    Retorna cuantas islas tiene la red y la asignacion de cada nodo.
    """
    g, _ = _require()
    comp_map, n = g.connected_components(rel_type=tipo_relacion)
    sizes: dict = {}
    for c in comp_map.values():
        sizes[c] = sizes.get(c, 0) + 1
    lines = [f"Total componentes: {n}"]
    for c, sz in sorted(sizes.items(), key=lambda x: -x[1]):
        lines.append(f"  Componente {c}: {sz} nodos")
    return "\n".join(lines)


@mcp.tool()
def componentes_fuertemente_conexas(tipo_relacion: str = None) -> str:
    """
    Componentes fuertemente conexas (SCC) — Kosaraju.
    Util para detectar ciclos de dependencia en grafos dirigidos.
    """
    _, ana = _require()
    try:
        comp_map, n = ana.strongly_connected_components(tipo_relacion)
        sizes: dict = {}
        for c in comp_map.values():
            sizes[c] = sizes.get(c, 0) + 1
        lines = [f"Total SCC: {n}"]
        for c, sz in sorted(sizes.items(), key=lambda x: -x[1])[:10]:
            lines.append(f"  SCC {c}: {sz} nodos")
        return "\n".join(lines)
    except Exception as e:
        return f"ERROR: {e}"


@mcp.tool()
def encontrar_puentes(tipo_relacion: str = None) -> str:
    """
    Encuentra puentes (lineas/aristas criticas N-1).
    Un puente es una arista cuya eliminacion desconecta la red.
    """
    g, _ = _require()
    ids  = g.find_bridges(rel_type=tipo_relacion)
    if not ids:
        return "No se encontraron puentes. La red es N-1 robusta topologicamente."
    lines = [f"Puentes encontrados: {len(ids)}"]
    for rid in ids[:20]:
        rel = g.get_rel(rid)
        if rel:
            lines.append(f"  rel_id={rid}  {rel['type']}  "
                         f"{rel['src_id']} -> {rel['dst_id']}")
        else:
            lines.append(f"  rel_id={rid}")
    if len(ids) > 20:
        lines.append(f"  ... ({len(ids) - 20} mas)")
    return "\n".join(lines)


@mcp.tool()
def encontrar_puntos_articulacion(tipo_relacion: str = None) -> str:
    """
    Encuentra puntos de articulacion (nodos criticos).
    Un punto de articulacion es un nodo cuya eliminacion desconecta la red.
    """
    g, _ = _require()
    ids  = g.find_articulation_points(rel_type=tipo_relacion)
    if not ids:
        return "No se encontraron puntos de articulacion."
    lines = [f"Puntos de articulacion: {len(ids)}"]
    for nid in ids[:20]:
        node = g.get_node(nid)
        nombre = ""
        if node:
            try:
                p = json.loads(node["properties"])
                nombre = p.get("nombre", p.get("name", ""))
            except Exception:
                pass
        lines.append(f"  node_id={nid}  {nombre}")
    if len(ids) > 20:
        lines.append(f"  ... ({len(ids) - 20} mas)")
    return "\n".join(lines)


@mcp.tool()
def n1_es_critico(rel_id: int) -> str:
    """
    Verifica si la salida de una relacion especifica desconecta la red (N-1).
    """
    g, _ = _require()
    critico = g.n1_is_critical(rel_id)
    rel     = g.get_rel(rel_id)
    desc    = ""
    if rel:
        desc = f" ({rel['type']} {rel['src_id']}->{rel['dst_id']})"
    return (f"Relacion {rel_id}{desc}: "
            f"{'CRITICA (N-1 desconecta la red)' if critico else 'no critica'}")


# -----------------------------------------------------------------------
# BLOQUE 3 — Centralidades
# -----------------------------------------------------------------------

@mcp.tool()
def centralidad_grado(tipo_relacion: str = None, top: int = 15) -> str:
    """
    Grado de cada nodo (numero de conexiones).
    Retorna los top-N nodos de mayor grado.
    """
    g, _ = _require()
    deg   = g.degree(rel_type=tipo_relacion)
    top_d = _top(deg, top)
    lines = [f"Top {top} nodos por grado:"]
    for nid, d in top_d.items():
        node = g.get_node(nid)
        nombre = ""
        if node:
            try:
                p = json.loads(node["properties"])
                nombre = str(p.get("nombre", p.get("name", p.get("numero", ""))))
            except Exception:
                pass
        lines.append(f"  {nombre or nid}: {d}")
    return "\n".join(lines)


@mcp.tool()
def centralidad_betweenness(tipo_relacion: str = None,
                             normalizado: bool = True,
                             top: int = 15) -> str:
    """
    Betweenness centrality (C implementado en C, algoritmo de Brandes).
    Mide cuantos caminos minimos pasan por cada nodo.
    Nodos de alta betweenness son topologicamente criticos.
    """
    _, ana = _require()
    try:
        bc  = ana.betweenness_centrality_c(tipo_relacion, normalizado)
        top_bc = _top(bc, top)
        lines = [f"Top {top} nodos por betweenness centrality:"]
        for nid, val in top_bc.items():
            lines.append(f"  nodo {nid}: {val:.6f}")
        return "\n".join(lines)
    except Exception as e:
        return f"ERROR: {e}"


@mcp.tool()
def centralidad_pagerank(tipo_relacion: str = None,
                          damping: float = 0.85,
                          top: int = 15) -> str:
    """
    PageRank (implementado en C, iteracion de potencia).
    Mide la importancia de un nodo considerando la importancia de sus vecinos.
    """
    _, ana = _require()
    try:
        pr  = ana.pagerank_c(tipo_relacion, damping=damping)
        top_pr = _top(pr, top)
        lines = [f"Top {top} nodos por PageRank (damping={damping}):"]
        for nid, val in top_pr.items():
            lines.append(f"  nodo {nid}: {val:.6f}")
        return "\n".join(lines)
    except Exception as e:
        return f"ERROR: {e}"


@mcp.tool()
def centralidad_closeness(tipo_relacion: str = None,
                           clave_peso: str = None,
                           top: int = 15) -> str:
    """
    Closeness centrality.
    Mide que tan cerca esta un nodo de todos los demas.
    Util para ubicar equipos de compensacion o almacenamiento.
    """
    _, ana = _require()
    try:
        cc  = ana.closeness_centrality(tipo_relacion, clave_peso)
        top_cc = _top(cc, top)
        lines = [f"Top {top} nodos por closeness centrality:"]
        for nid, val in top_cc.items():
            lines.append(f"  nodo {nid}: {val:.6f}")
        return "\n".join(lines)
    except Exception as e:
        return f"ERROR: {e}"


@mcp.tool()
def coeficiente_clustering(tipo_relacion: str = None) -> str:
    """
    Coeficiente de clustering (implementado en C).
    Mide que tan interconectados estan los vecinos de cada nodo.
    Global: probabilidad de que dos vecinos aleatorios de un nodo esten conectados.
    """
    _, ana = _require()
    try:
        res    = ana.clustering_coefficient_c(tipo_relacion)
        global_cc = res.get("global", 0)
        local  = res.get("local", {})
        top_lc = _top(local, 10)
        lines  = [f"Clustering coefficient global: {global_cc:.4f}",
                  f"Top 10 nodos por clustering local:"]
        for nid, val in top_lc.items():
            if val is not None:
                lines.append(f"  nodo {nid}: {val:.4f}")
        return "\n".join(lines)
    except Exception as e:
        return f"ERROR: {e}"


# -----------------------------------------------------------------------
# BLOQUE 4 — Caminos
# -----------------------------------------------------------------------

@mcp.tool()
def camino_mas_corto(src: int, dst: int,
                      tipo_relacion: str = None) -> str:
    """
    Camino topologico mas corto entre dos nodos (sin pesos).
    Retorna la secuencia de node_ids.
    """
    g, _ = _require()
    path = g.shortest_path(src, dst, rel_type=tipo_relacion)
    if not path:
        return f"No existe camino entre {src} y {dst}."
    nodes = [str(p["node_id"]) for p in path]
    return (f"Longitud: {len(nodes) - 1} saltos\n"
            f"Camino: {' -> '.join(nodes)}")


@mcp.tool()
def dijkstra_desde(src: int,
                    tipo_relacion: str = None,
                    clave_peso: str = None,
                    top: int = 15) -> str:
    """
    Dijkstra desde un nodo origen.
    Retorna los top-N destinos mas cercanos con su distancia.
    clave_peso: propiedad JSON usada como peso (ej. 'X_pu', 'longitud_km').
    """
    g, _ = _require()
    res  = g.dijkstra(src, rel_type=tipo_relacion, weight_key=clave_peso)
    reachable = [(r["node_id"], r["dist"])
                 for r in res if r["dist"] >= 0 and r["node_id"] != src]
    reachable.sort(key=lambda x: x[1])
    lines = [f"Dijkstra desde nodo {src} "
             f"(peso: {clave_peso or 'saltos'}):"]
    for nid, dist in reachable[:top]:
        lines.append(f"  -> nodo {nid}: {dist:.4f}")
    if len(reachable) > top:
        lines.append(f"  ... ({len(reachable) - top} mas)")
    return "\n".join(lines)


@mcp.tool()
def k_caminos_mas_cortos(src: int, dst: int,
                          k: int = 3,
                          tipo_relacion: str = None,
                          clave_peso: str = None) -> str:
    """
    K caminos mas cortos entre dos nodos (algoritmo de Yen, implementado en C).
    """
    _, ana = _require()
    try:
        paths = ana.k_shortest_paths_c(src, dst, k=k,
                                        rel_type=tipo_relacion,
                                        weight_key=clave_peso)
        if not paths:
            return f"No se encontraron caminos entre {src} y {dst}."
        lines = [f"{len(paths)} caminos entre {src} y {dst}:"]
        for i, p in enumerate(paths, 1):
            nodes = " -> ".join(str(n) for n in p["nodes"])
            lines.append(f"  #{i} costo={p['cost']:.4f}: {nodes}")
        return "\n".join(lines)
    except Exception as e:
        return f"ERROR: {e}"


# -----------------------------------------------------------------------
# BLOQUE 5 — Flujo de red
# -----------------------------------------------------------------------

@mcp.tool()
def flujo_maximo(src: int, dst: int,
                  tipo_relacion: str = None,
                  clave_capacidad: str = "rate_A_MVA") -> str:
    """
    Flujo maximo entre dos nodos (Edmonds-Karp, implementado en C).
    clave_capacidad: propiedad JSON que representa la capacidad de la arista.
    """
    _, ana = _require()
    try:
        val = ana.max_flow_c(src, dst, tipo_relacion, clave_capacidad)
        return (f"Flujo maximo {src} -> {dst}: {val:.4f}\n"
                f"Capacidad clave: '{clave_capacidad}'")
    except Exception as e:
        return f"ERROR: {e}"


# -----------------------------------------------------------------------
# BLOQUE 6 — Comunidades
# -----------------------------------------------------------------------

@mcp.tool()
def comunidades_louvain(tipo_relacion: str = None) -> str:
    """
    Deteccion de comunidades por Louvain (requiere python-louvain).
    Identifica grupos de nodos densamente interconectados entre si.
    """
    _, ana = _require()
    try:
        partition = ana.community_detection(tipo_relacion)
        sizes: dict = {}
        for c in partition.values():
            sizes[c] = sizes.get(c, 0) + 1
        lines = [f"Comunidades Louvain: {len(sizes)}"]
        for c, sz in sorted(sizes.items(), key=lambda x: -x[1]):
            lines.append(f"  Comunidad {c}: {sz} nodos")
        return "\n".join(lines)
    except ImportError:
        return "ERROR: instala python-louvain: pip install python-louvain"
    except Exception as e:
        return f"ERROR: {e}"


@mcp.tool()
def comunidades_propagacion_etiquetas(tipo_relacion: str = None) -> str:
    """
    Deteccion de comunidades por propagacion de etiquetas (NetworkX).
    Mas rapido que Louvain, sin dependencias adicionales.
    """
    _, ana = _require()
    try:
        comms = ana.label_propagation_communities(tipo_relacion)
        lines = [f"Comunidades detectadas: {len(comms)}"]
        for i, c in enumerate(sorted(comms, key=len, reverse=True)):
            lines.append(f"  Comunidad {i}: {len(c)} nodos")
        return "\n".join(lines)
    except Exception as e:
        return f"ERROR: {e}"


@mcp.tool()
def particion_espectral(n_zonas: int = 2,
                         tipo_relacion: str = None) -> str:
    """
    Particion espectral del grafo en n_zonas zonas.
    Basada en el vector de Fiedler de la Laplaciana.
    Util para definir areas de control o zonas de precio.
    """
    _, ana = _require()
    try:
        zones = ana.spectral_partition(n_clusters=n_zonas,
                                        rel_type=tipo_relacion)
        sizes: dict = {}
        for c in zones.values():
            sizes[c] = sizes.get(c, 0) + 1
        lines = [f"Particion espectral en {n_zonas} zonas:"]
        for z, sz in sorted(sizes.items()):
            lines.append(f"  Zona {z}: {sz} nodos")
        return "\n".join(lines)
    except Exception as e:
        return f"ERROR: {e}"


# -----------------------------------------------------------------------
# BLOQUE 7 — Metricas globales
# -----------------------------------------------------------------------

@mcp.tool()
def resumen_grafo(tipo_relacion: str = None) -> str:
    """
    Resumen completo con las metricas mas importantes del grafo.
    Incluye conectividad, densidad, clustering, Fiedler y mas.
    """
    _, ana = _require()
    try:
        data = ana.summary(tipo_relacion)
        lines = ["Resumen del grafo:"]
        for k, v in data.items():
            if isinstance(v, float):
                lines.append(f"  {k}: {v:.4f}")
            else:
                lines.append(f"  {k}: {v}")
        return "\n".join(lines)
    except Exception as e:
        return f"ERROR: {e}"


@mcp.tool()
def valor_fiedler(tipo_relacion: str = None) -> str:
    """
    Valor de Fiedler (segundo valor propio menor de la Laplaciana).
    Mide la conectividad algebraica: cuanto mayor, mas robusta es la red.
    Valor = 0 indica red desconectada.
    """
    _, ana = _require()
    try:
        val = ana.fiedler_value(tipo_relacion)
        interpretacion = (
            "red desconectada" if val < 1e-10 else
            "conectividad muy baja (red fragil)" if val < 0.1 else
            "conectividad baja" if val < 0.5 else
            "conectividad moderada" if val < 2.0 else
            "conectividad alta (red robusta)"
        )
        return f"Valor de Fiedler: {val:.6f}\nInterpretacion: {interpretacion}"
    except Exception as e:
        return f"ERROR: {e}"


@mcp.tool()
def densidad_grafo(tipo_relacion: str = None) -> str:
    """
    Densidad del grafo: ratio de aristas actuales vs maximo posible.
    0 = sin aristas, 1 = grafo completo.
    Redes de transmision tipicamente tienen densidad entre 0.01 y 0.05.
    """
    _, ana = _require()
    try:
        d = ana.density(tipo_relacion)
        return f"Densidad: {d:.6f}  ({d*100:.2f}%)"
    except Exception as e:
        return f"ERROR: {e}"


@mcp.tool()
def asortatividad(tipo_relacion: str = None) -> str:
    """
    Coeficiente de asortatividad de grado.
    Positivo: nodos de alto grado se conectan entre si (resiliente).
    Negativo: hubs se conectan a nodos periferico (hub-and-spoke).
    """
    _, ana = _require()
    try:
        val = ana.assortativity(tipo_relacion)
        if math.isnan(val):
            return "No se pudo calcular asortatividad (grafo muy pequeno)."
        interpretacion = (
            "estructura hub-and-spoke (nodos importantes conectan a la periferia)"
            if val < -0.1 else
            "estructura neutra" if abs(val) < 0.1 else
            "estructura homofila (similares se conectan entre si)"
        )
        return f"Asortatividad: {val:.4f}\nInterpretacion: {interpretacion}"
    except Exception as e:
        return f"ERROR: {e}"


@mcp.tool()
def robustez_red(tipo_relacion: str = None) -> str:
    """
    Informe completo de robustez de la red combinando multiples metricas.
    """
    _, ana = _require()
    try:
        r = ana.network_robustness(tipo_relacion)
        lines = ["Robustez de la red:"]
        for k, v in r.items():
            if isinstance(v, float):
                lines.append(f"  {k}: {v:.4f}")
            else:
                lines.append(f"  {k}: {v}")
        return "\n".join(lines)
    except Exception as e:
        return f"ERROR: {e}"


# -----------------------------------------------------------------------
# BLOQUE 8 — Sistemas electricos de potencia
# -----------------------------------------------------------------------

@mcp.tool()
def ranking_ubicacion_bess(tipo_relacion: str = "LINEA",
                            clave_peso: str = "X_pu",
                            top: int = 10) -> str:
    """
    Ranking de barras candidatas para ubicacion de baterias BESS.
    Combina betweenness y closeness centrality para identificar las
    barras de mayor impacto topologico en la red.
    """
    _, ana = _require()
    try:
        ranked = ana.bess_siting_ranking(rel_type=tipo_relacion,
                                          weight_key=clave_peso, n=top)
        lines = [f"Top {top} barras candidatas para BESS:"]
        for i, (nid, score) in enumerate(ranked, 1):
            lines.append(f"  #{i} nodo {nid}: score={score:.4f}")
        return "\n".join(lines)
    except Exception as e:
        return f"ERROR: {e}"


@mcp.tool()
def riesgo_congestion(tipo_relacion: str = "LINEA") -> str:
    """
    Ranking de lineas por riesgo de congestion topologica.
    Basado en edge betweenness centrality.
    Lineas de alta betweenness son las mas propensas a congestion.
    """
    _, ana = _require()
    try:
        res   = ana.congestion_risk(tipo_relacion)
        lines = ["Top 15 lineas por riesgo de congestion:"]
        for rid, src, dst, btw in res[:15]:
            lines.append(f"  rel={rid} ({src}->{dst}): {btw:.6f}")
        return "\n".join(lines)
    except Exception as e:
        return f"ERROR: {e}"


@mcp.tool()
def indice_vulnerabilidad_aristas(tipo_relacion: str = "LINEA",
                                   top: int = 15) -> str:
    """
    Indice de vulnerabilidad por arista: betweenness / capacidad termica.
    Aristas con alto indice son tanto topologicamente criticas como
    electricamente limitadas.
    """
    _, ana = _require()
    try:
        res   = ana.vulnerability_index(tipo_relacion)
        lines = [f"Top {top} aristas por vulnerabilidad:"]
        for rid, src, dst, idx in res[:top]:
            lines.append(f"  rel={rid} ({src}->{dst}): indice={idx:.6f}")
        return "\n".join(lines)
    except Exception as e:
        return f"ERROR: {e}"


@mcp.tool()
def analisis_n1_completo(tipo_relacion: str = "LINEA") -> str:
    """
    Analisis N-1 topologico completo.
    Lista todos los puentes (salidas que desconectan la red) y
    puntos de articulacion. Incluye recomendacion de prioridad.
    """
    g, _ = _require()
    bridges = g.find_bridges(rel_type=tipo_relacion)
    aps     = g.find_articulation_points(rel_type=tipo_relacion)
    connected = g.is_connected(rel_type=tipo_relacion)

    lines = [
        f"=== Analisis N-1 Topologico ===",
        f"Red conectada        : {connected}",
        f"Puentes (lineas)     : {len(bridges)}",
        f"Puntos articulacion  : {len(aps)}",
        "",
    ]

    if bridges:
        lines.append("Lineas criticas (puentes):")
        for rid in bridges[:10]:
            rel = g.get_rel(rid)
            if rel:
                lines.append(f"  rel_id={rid}  {rel['src_id']} -> {rel['dst_id']}")

    if aps:
        lines.append("\nNodos criticos (puntos de articulacion):")
        for nid in aps[:10]:
            node = g.get_node(nid)
            nombre = ""
            if node:
                try:
                    p = json.loads(node["properties"])
                    nombre = str(p.get("nombre", p.get("name", "")))
                except Exception:
                    pass
            lines.append(f"  node_id={nid}  {nombre}")

    nivel = ("CRITICO" if len(bridges) > 5 else
             "ALTO"    if len(bridges) > 2 else
             "MODERADO" if len(bridges) > 0 else "BAJO")
    lines.append(f"\nNivel de riesgo topologico: {nivel}")
    return "\n".join(lines)


# -----------------------------------------------------------------------
# BLOQUE 9 — PTDF / LODF
# -----------------------------------------------------------------------

# Estado global para PTDF/LODF (se cargan via herramienta cargar_ptdf)
_gptdf: "GraphPTDF | None" = None
_glodf: "GraphLODF | None" = None

try:
    from graphdb_ptdf import GraphPTDF
    from graphdb_lodf import GraphLODF
    HAS_PTDF = True
except ImportError:
    HAS_PTDF = False


def _require_ptdf():
    if not HAS_PTDF:
        raise RuntimeError(
            "graphdb_ptdf/graphdb_lodf no disponibles. "
            "Verifica que los archivos esten en el mismo directorio.")
    if _gptdf is None:
        raise RuntimeError(
            "PTDF no cargada. Usa cargar_ptdf() primero.")
    return _gptdf


def _require_lodf():
    _require_ptdf()
    if _glodf is None:
        raise RuntimeError(
            "LODF no calculada. Usa cargar_ptdf() con "
            "base_flows_csv para calcularla automaticamente.")
    return _glodf


@mcp.tool()
def cargar_ptdf(ptdf_csv: str,
                base_flows_csv: str = "",
                ratings_csv: str = "",
                rel_type: str = "LINEA") -> str:
    """
    Carga una matriz PTDF desde un archivo CSV y opcionalmente
    calcula la LODF si se provee el CSV de flujos base.

    Parameters
    ----------
    ptdf_csv : str
        Ruta al CSV con la PTDF.
        Formato: primera columna = etiquetas de rama (index),
                 resto de columnas = numeros de bus PSS/E.
    base_flows_csv : str
        Ruta al CSV con flujos base en MW.
        Formato: dos columnas 'label' y 'flow_mw'.
        Si vacio, no se calcula la LODF.
    ratings_csv : str
        Ruta al CSV con capacidades termicas en MW.
        Formato: dos columnas 'label' y 'rating_mw'. Opcional.
    rel_type : str
        Tipo de relacion en graphdb (default LINEA).
    """
    global _gptdf, _glodf
    if not HAS_PTDF:
        return "ERROR: graphdb_ptdf.py no encontrado en el directorio."

    g, _ = _require()

    import os
    import pandas as pd
    from graphdb_ptdf import GraphPTDF
    from graphdb_lodf import GraphLODF

    if not os.path.exists(ptdf_csv):
        return f"ERROR: archivo no encontrado: {ptdf_csv}"

    try:
        ptdf_df = pd.read_csv(ptdf_csv, index_col=0)
        ptdf_df.columns = [int(c) for c in ptdf_df.columns]
    except Exception as e:
        return f"ERROR al leer PTDF CSV: {e}"

    _gptdf = GraphPTDF(g, ptdf_df, rel_type=rel_type, persist_topk=True)
    res = _gptdf.resumen()

    lines = [
        f"PTDF cargada: {res['n_branches']} ramas x {res['n_buses']} buses",
        f"  Puentes topologicos  : {res['n_bridges']}",
        f"  Ramas mapeadas en BD : {res['n_mapped_to_graph']}",
        f"  PTDF max             : {res['max_ptdf']:.4f}",
        f"  Esparsidad           : {res['sparsity_pct']:.1f}%",
        f"  Persistidas en BD    : si (top-{20} buses por rama)",
    ]

    _glodf = None
    if base_flows_csv and os.path.exists(base_flows_csv):
        try:
            flows_df = pd.read_csv(base_flows_csv)
            flows_s  = pd.Series(
                flows_df.iloc[:, 1].values,
                index=flows_df.iloc[:, 0].astype(str).values,
            )
        except Exception as e:
            lines.append(f"  Advertencia flujos: {e}")
            return "\n".join(lines)

        ratings_s = None
        if ratings_csv and os.path.exists(ratings_csv):
            try:
                rat_df   = pd.read_csv(ratings_csv)
                ratings_s = pd.Series(
                    rat_df.iloc[:, 1].values,
                    index=rat_df.iloc[:, 0].astype(str).values,
                )
            except Exception:
                pass

        _glodf = GraphLODF(_gptdf, flows_s, ratings_s)
        lodf_res = _glodf.resumen()
        lines += [
            "",
            f"LODF calculada:",
            f"  Columnas validas     : {lodf_res['n_valid_lodf_columns']}",
            f"  Contingencias viables: {lodf_res['n_contingencias']}",
            f"  Singulares descartadas: {lodf_res['n_singular']}",
        ]
        if "max_loading_pct_worst" in lodf_res:
            lines.append(
                f"  Peor contingencia    : {lodf_res['max_loading_pct_worst']:.1f}%")

    return "\n".join(lines)


@mcp.tool()
def calcular_ptdf_rama(nombre_rama: str) -> str:
    """
    Retorna la sensibilidad de una rama especifica a inyecciones en cada
    bus (fila de la PTDF), ordenada por magnitud absoluta descendente.

    nombre_rama : etiqueta exacta de la rama, ej. "L 1-2" o "T2D 30-38".
    """
    gptdf = _require_ptdf()

    s = gptdf.sensibilidad_rama(nombre_rama)
    if s is None:
        return (f"Rama '{nombre_rama}' no encontrada en la PTDF.\n"
                f"Ejemplo de etiquetas: {gptdf.labels[:5]}")

    abs_s   = s.abs().sort_values(ascending=False)
    top20   = abs_s.head(20)
    lines   = [f"PTDF fila '{nombre_rama}' (top 20 buses por |PTDF|):"]
    for bus, val in zip(top20.index, s.loc[top20.index]):
        lines.append(f"  bus {bus:6d}: {val:+.6f}")
    return "\n".join(lines)


@mcp.tool()
def impacto_inyeccion_bus(bus_num: int, top_n: int = 20) -> str:
    """
    Retorna las ramas mas impactadas por una inyeccion unitaria en un bus
    (columna de la PTDF), ordenadas por |PTDF| descendente.

    bus_num : numero PSS/E del bus de inyeccion.
    top_n   : numero de ramas a mostrar (default 20).
    """
    gptdf = _require_ptdf()

    s = gptdf.impacto_inyeccion_bus(bus_num, top_n)
    if s.empty:
        return f"Bus {bus_num} no encontrado en la PTDF."

    lines = [f"Impacto de inyeccion unitaria en bus {bus_num}:"]
    for label, val in s.items():
        lines.append(f"  {label:<35} {val:+.6f}")
    return "\n".join(lines)


@mcp.tool()
def flujos_post_contingencia(nombre_rama_outage: str,
                              top_n: int = 15) -> str:
    """
    Calcula los flujos en todas las ramas tras la salida de una linea.
    Usa la LODF pre-calculada (O(n_ramas) por contingencia).

    nombre_rama_outage : etiqueta de la rama que sale de servicio.
    top_n              : ramas mas cargadas a mostrar.
    """
    glodf = _require_lodf()

    flows = glodf.flujos_post_contingencia(nombre_rama_outage)
    if flows is None:
        return (f"Rama '{nombre_rama_outage}' no tiene contingencia "
                f"valida (puede ser puente o singular).")

    ratings = glodf._ratings
    labels  = glodf._gptdf.labels
    safe_r  = [r if r > 1e-3 else 1.0 for r in ratings]
    loadings_pct = [abs(float(flows.iloc[i])) / safe_r[i] * 100.0
                    for i in range(len(flows))]

    order = sorted(range(len(loadings_pct)),
                   key=lambda x: loadings_pct[x], reverse=True)

    lines = [f"Flujos post-contingencia '{nombre_rama_outage}' "
             f"(top {top_n} ramas):"]
    lines.append(f"  {'Rama':<35} {'F_post_MW':>10} {'Loading%':>10}")
    lines.append("  " + "-" * 58)
    for i in order[:top_n]:
        f_mw  = float(flows.iloc[i])
        l_pct = loadings_pct[i]
        flag  = " <-- SOBRECARGA" if l_pct > 100 else ""
        lines.append(f"  {labels[i]:<35} {f_mw:>10.2f} {l_pct:>9.1f}%{flag}")

    return "\n".join(lines)


@mcp.tool()
def ranking_contingencias_criticas(top_n: int = 20) -> str:
    """
    Ranking de las contingencias N-1 mas criticas por maxima carga
    post-contingencia. Requiere que la LODF haya sido calculada.

    top_n : numero de contingencias a mostrar (default 20).
    """
    glodf = _require_lodf()

    df = glodf.ranking_contingencias_criticas(top_n)
    if df.empty:
        return "No hay contingencias viables calculadas."

    lines = [f"Top {top_n} contingencias criticas (LODF):"]
    lines.append(
        f"  {'Rank':>4}  {'Rama outage':<35} "
        f"{'MaxLoad%':>9}  {'Overloads':>9}")
    lines.append("  " + "-" * 65)
    for _, row in df.iterrows():
        lines.append(
            f"  {int(row['rank']):>4}  {row['label']:<35} "
            f"{row['max_loading_pct']:>9.1f}  {int(row['n_overloads']):>9}")
    return "\n".join(lines)


@mcp.tool()
def ptdf_cruzado(rama_afectada: str, bus_num: int) -> str:
    """
    Retorna el factor PTDF[rama_afectada, bus_num]:
    cambio en flujo de la rama ante inyeccion unitaria en el bus.

    rama_afectada : etiqueta de la rama afectada.
    bus_num       : numero PSS/E del bus de inyeccion.
    """
    gptdf = _require_ptdf()

    val = gptdf.ptdf_cruzado(rama_afectada, bus_num)
    return (f"PTDF['{rama_afectada}', bus={bus_num}] = {val:+.6f}\n"
            f"Interpretacion: una inyeccion de 1 MW en bus {bus_num} "
            f"cambia el flujo en '{rama_afectada}' en {val:+.4f} MW.")


@mcp.tool()
def verificar_n1_electrico(nombre_rama: str) -> str:
    """
    Dictamen N-1 electrico completo para una rama.
    Combina el analisis topologico del motor C con la LODF para
    dar un veredicto: 'critico', 'advertencia', 'seguro', 'puente'.

    nombre_rama : etiqueta de la rama a evaluar.
    """
    glodf = _require_lodf()

    r = glodf.verificar_n1_electrico(nombre_rama)

    lines = [
        f"=== Analisis N-1 electrico: '{nombre_rama}' ===",
        f"  Es puente topologico : {r['is_bridge']}",
        f"  LODF singular        : {r['is_singular']}",
        f"  Max carga post-N1    : {r['max_loading_pct']:.1f}%",
        f"  Sobrecargas          : {r['n_overloads']}",
        f"  VEREDICTO            : {r['verdict'].upper()}",
    ]

    if r["top_overloaded"]:
        lines.append("  Ramas sobrecargadas:")
        for lbl, pct in r["top_overloaded"].items():
            lines.append(f"    {lbl:<35} {pct:.1f}%")

    return "\n".join(lines)


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Servidor MCP para graphdb"
    )
    parser.add_argument("db", nargs="?", default="",
                        help="Ruta a la base de datos .db (opcional)")
    args = parser.parse_args()

    if args.db and os.path.exists(args.db):
        # pre-open the database so tools are ready immediately
        result = abrir_base_datos(args.db)
        print(f"[graphdb-mcp] {result}", file=sys.stderr)
    elif args.db:
        print(f"[graphdb-mcp] Advertencia: {args.db} no encontrado.",
              file=sys.stderr)

    print("[graphdb-mcp] Servidor iniciado. Esperando conexiones MCP...",
          file=sys.stderr)
    mcp.run()


if __name__ == "__main__":
    main()
