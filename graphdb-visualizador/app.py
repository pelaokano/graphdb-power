import sys
import os
import json
import threading
import webview

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import graphdb

MAX_NODES = 500
_window   = None


class GraphAPI:

    def __init__(self):
        self.g       = None
        self.db_path = None

    # ----------------------------------------------------------------
    # Internal
    # ----------------------------------------------------------------

    def _log(self, level, msg):
        """Push log line to the JS console panel."""
        if _window:
            safe = (msg.replace("\\", "\\\\")
                       .replace("`", "'")
                       .replace("\n", " ")
                       .replace("\r", ""))
            try:
                _window.evaluate_js(f'window.appendLog("{level}", `{safe}`)')
            except Exception:
                pass

    def _require_db(self):
        if self.g is None:
            raise RuntimeError("No hay base de datos abierta.")

    # ----------------------------------------------------------------
    # File dialog via tkinter (works correctly on Windows from any thread)
    # ----------------------------------------------------------------

    def _pick_file(self):
        """Open a native file dialog and return the chosen path or ''."""
        path_result = [""]
        done = threading.Event()

        def show():
            try:
                import tkinter as tk
                from tkinter import filedialog
                root = tk.Tk()
                root.withdraw()
                root.wm_attributes("-topmost", True)
                p = filedialog.askopenfilename(
                    parent=root,
                    title="Seleccionar base de datos graphdb",
                    filetypes=[
                        ("SQLite Database", "*.db"),
                        ("All files", "*.*"),
                    ]
                )
                root.destroy()
                path_result[0] = p or ""
            except Exception as ex:
                self._log("ERROR", f"dialogo: {ex}")
                path_result[0] = ""
            finally:
                done.set()

        t = threading.Thread(target=show, daemon=True)
        t.start()
        done.wait(timeout=60)
        return path_result[0]

    # ----------------------------------------------------------------
    # Database management
    # ----------------------------------------------------------------

    def open_database(self):
        """Show file dialog and open chosen .db file."""
        try:
            path = self._pick_file()
            if not path:
                self._log("INFO", "Seleccion cancelada.")
                return {"ok": False, "cancelled": True}

            if not os.path.exists(path):
                self._log("ERROR", f"Archivo no encontrado: {path}")
                return {"ok": False, "error": f"Archivo no encontrado: {path}"}

            # close previous db
            if self.g:
                try:
                    self.g.close()
                except Exception:
                    pass
                self.g = None

            self.g       = graphdb.Graph(path)
            self.db_path = path
            self._log("INFO", f"Abierto: {path}")

            node_rows   = self.g.query("MATCH (n) RETURN count(n)")
            rel_rows    = self.g.query("MATCH (a)-[r]->(b) RETURN count(r)")
            total_nodes = int(node_rows[0]) if node_rows else 0
            total_rels  = int(rel_rows[0])  if rel_rows  else 0

            self._log("INFO",
                f"BD cargada: {total_nodes} nodos, {total_rels} aristas.")

            return {
                "ok":          True,
                "db_path":     path,
                "db_name":     os.path.basename(path),
                "total_nodes": total_nodes,
                "total_rels":  total_rels,
            }

        except Exception as e:
            self._log("ERROR", f"open_database: {e}")
            return {"ok": False, "error": str(e)}

    def close_database(self):
        """Close current db without opening a new one."""
        if self.g:
            try:
                self.g.close()
            except Exception:
                pass
            self.g       = None
            self.db_path = None
            self._log("INFO", "Base de datos cerrada.")
        return {"ok": True}

    def get_db_info(self):
        if self.g is None:
            return {"ok": True, "no_db": True}
        try:
            node_rows   = self.g.query("MATCH (n) RETURN count(n)")
            rel_rows    = self.g.query("MATCH (a)-[r]->(b) RETURN count(r)")
            total_nodes = int(node_rows[0]) if node_rows else 0
            total_rels  = int(rel_rows[0])  if rel_rows  else 0
            return {
                "ok":          True,
                "db_path":     self.db_path,
                "db_name":     os.path.basename(self.db_path),
                "total_nodes": total_nodes,
                "total_rels":  total_rels,
            }
        except Exception as e:
            self._log("ERROR", f"get_db_info: {e}")
            return {"ok": False, "error": str(e)}

    # ----------------------------------------------------------------
    # Load graph
    # ----------------------------------------------------------------

    def load_graph(self):
        try:
            self._require_db()

            count_rows  = self.g.query("MATCH (n) RETURN count(n)")
            total_nodes = int(count_rows[0]) if count_rows else 0
            self._log("INFO", f"Cargando grafo ({total_nodes} nodos en BD)...")

            degree_map   = self.g.degree()
            sorted_ids   = sorted(degree_map.keys(),
                                  key=lambda x: degree_map[x], reverse=True)
            selected_ids = set(sorted_ids[:MAX_NODES])

            nodes = []
            for row in self.g.query("MATCH (n) RETURN n"):
                try:
                    nid = int(row)
                except ValueError:
                    continue
                if nid not in selected_ids:
                    continue
                node = self.g.get_node(nid)
                if node:
                    props = {}
                    try:
                        props = json.loads(node["properties"])
                    except Exception:
                        pass
                    nodes.append({
                        "id":     str(nid),
                        "labels": node["labels"],
                        "props":  props,
                    })

            edges = []
            for row in self.g.query("MATCH (a)-[r]->(b) RETURN a, r, b"):
                parts = row.split(" | ")
                if len(parts) < 3:
                    continue
                try:
                    src = int(parts[0])
                    rid = int(parts[1])
                    dst = int(parts[2])
                except ValueError:
                    continue
                if src not in selected_ids or dst not in selected_ids:
                    continue
                rel = self.g.get_rel(rid)
                if rel:
                    props = {}
                    try:
                        props = json.loads(rel["properties"])
                    except Exception:
                        pass
                    edges.append({
                        "id":     f"e{rid}",
                        "source": str(src),
                        "target": str(dst),
                        "type":   rel["type"],
                        "props":  props,
                    })

            self._log("INFO",
                f"Grafo listo: {len(nodes)} nodos, {len(edges)} aristas.")
            if total_nodes > MAX_NODES:
                self._log("WARN",
                    f"Limite {MAX_NODES}: mostrando {len(nodes)} de "
                    f"{total_nodes} nodos (ordenados por grado).")

            return {
                "ok":          True,
                "nodes":       nodes,
                "edges":       edges,
                "total_nodes": total_nodes,
                "shown_nodes": len(nodes),
                "truncated":   total_nodes > MAX_NODES,
            }

        except Exception as e:
            self._log("ERROR", f"load_graph: {e}")
            return {"ok": False, "error": str(e)}

    # ----------------------------------------------------------------
    # Cypher query
    # ----------------------------------------------------------------

    def run_query(self, cypher):
        """
        Execute a Cypher query and return the matched nodes and edges
        so the frontend can REPLACE the graph view with only the results.

        - Collects all node ids found in the result rows.
        - For edge queries, also collects the endpoint nodes.
        - If result exceeds MAX_NODES, truncates to MAX_NODES keeping
          nodes with the highest degree.
        - Returns all edges between the selected nodes.
        """
        try:
            self._require_db()
            preview = cypher[:80] + ("..." if len(cypher) > 80 else "")
            self._log("INFO", f"Query: {preview}")

            rows = self.g.query(cypher)
            self._log("INFO", f"Resultado: {len(rows)} filas.")

            found_node_ids = set()
            found_rel_ids  = set()

            for row in rows:
                for part in row.split(" | "):
                    part = part.strip()
                    if not part or part == "null":
                        continue
                    try:
                        val  = int(part)
                        node = self.g.get_node(val)
                        if node:
                            found_node_ids.add(val)
                        else:
                            rel = self.g.get_rel(val)
                            if rel:
                                found_rel_ids.add(val)
                                # include endpoint nodes of matched edges
                                found_node_ids.add(rel["src_id"])
                                found_node_ids.add(rel["dst_id"])
                    except ValueError:
                        pass

            total_found = len(found_node_ids)

            # apply node limit: keep highest-degree nodes
            if total_found > MAX_NODES:
                degree_map  = self.g.degree()
                sorted_ids  = sorted(found_node_ids,
                                     key=lambda x: degree_map.get(x, 0),
                                     reverse=True)
                found_node_ids = set(sorted_ids[:MAX_NODES])
                self._log("WARN",
                    f"Limite {MAX_NODES}: mostrando {MAX_NODES} de "
                    f"{total_found} nodos (por grado).")

            # build node list
            nodes = []
            for nid in found_node_ids:
                node = self.g.get_node(nid)
                if node:
                    props = {}
                    try:
                        props = json.loads(node["properties"])
                    except Exception:
                        pass
                    nodes.append({
                        "id":     str(nid),
                        "labels": node["labels"],
                        "props":  props,
                    })

            # build edge list: all edges between selected nodes
            # plus explicitly matched edges whose endpoints are selected
            edges = []
            seen_eids = set()

            for row in self.g.query("MATCH (a)-[r]->(b) RETURN a, r, b"):
                parts = row.split(" | ")
                if len(parts) < 3:
                    continue
                try:
                    src = int(parts[0])
                    rid = int(parts[1])
                    dst = int(parts[2])
                except ValueError:
                    continue

                if src not in found_node_ids or dst not in found_node_ids:
                    continue
                if rid in seen_eids:
                    continue
                seen_eids.add(rid)

                rel = self.g.get_rel(rid)
                if rel:
                    props = {}
                    try:
                        props = json.loads(rel["properties"])
                    except Exception:
                        pass
                    edges.append({
                        "id":     f"e{rid}",
                        "source": str(src),
                        "target": str(dst),
                        "type":   rel["type"],
                        "props":  props,
                    })

            self._log("INFO",
                f"Vista: {len(nodes)} nodos, {len(edges)} aristas.")

            return {
                "ok":          True,
                "rows":        rows,
                "nodes":       nodes,
                "edges":       edges,
                "total_found": total_found,
                "truncated":   total_found > MAX_NODES,
            }

        except Exception as e:
            self._log("ERROR", f"run_query: {e}")
            return {"ok": False, "error": str(e)}


    # ----------------------------------------------------------------
    # Graph algorithms panel
    # ----------------------------------------------------------------

    def run_algorithm(self, algo, params):
        """
        Execute a named graph algorithm and return a serializable result.

        algo   : str  — algorithm name
        params : dict — optional parameters (rel_type, weight_key, src, dst, k, ...)
        """
        try:
            self._require_db()
            import graphdb_algorithms as ga

            ana      = ga.GraphAnalyzer(self.g)
            rel_type = params.get("rel_type") or None
            w_key    = params.get("weight_key") or None
            self._log("INFO", f"Algoritmo: {algo} rel_type={rel_type}")

            # --- topology ---
            if algo == "is_connected":
                val = self.g.is_connected(rel_type=rel_type)
                return {"ok": True, "type": "scalar",
                        "label": "Conectado", "value": str(val)}

            elif algo == "connected_components":
                comp_map, n = self.g.connected_components(rel_type=rel_type)
                return {"ok": True, "type": "scalar",
                        "label": "Componentes conectadas", "value": str(n)}

            elif algo == "strongly_connected_components":
                comp_map, n = ana.strongly_connected_components(rel_type)
                return {"ok": True, "type": "node_map",
                        "label": "CFC (componente fuertemente conexa)",
                        "data": {str(k): v for k, v in comp_map.items()},
                        "scalar": f"{n} componentes"}

            elif algo == "bridges":
                ids = self.g.find_bridges(rel_type=rel_type)
                return {"ok": True, "type": "id_list",
                        "label": "Puentes (aristas criticas N-1)",
                        "ids": [str(i) for i in ids],
                        "scalar": f"{len(ids)} puentes"}

            elif algo == "articulation_points":
                ids = self.g.find_articulation_points(rel_type=rel_type)
                return {"ok": True, "type": "id_list",
                        "label": "Puntos de articulacion (nodos criticos)",
                        "ids": [str(i) for i in ids],
                        "scalar": f"{len(ids)} puntos"}

            # --- centralities ---
            elif algo == "betweenness":
                data = ana.betweenness_centrality_c(rel_type)
                return {"ok": True, "type": "node_map",
                        "label": "Betweenness Centrality",
                        "data": {str(k): round(v, 6) for k, v in data.items()}}

            elif algo == "pagerank":
                data = ana.pagerank_c(rel_type)
                return {"ok": True, "type": "node_map",
                        "label": "PageRank",
                        "data": {str(k): round(v, 6) for k, v in data.items()}}

            elif algo == "closeness":
                data = ana.closeness_centrality(rel_type, w_key)
                return {"ok": True, "type": "node_map",
                        "label": "Closeness Centrality",
                        "data": {str(k): round(v, 6) for k, v in data.items()}}

            elif algo == "degree":
                data = self.g.degree(rel_type=rel_type)
                return {"ok": True, "type": "node_map",
                        "label": "Grado de nodos",
                        "data": {str(k): v for k, v in data.items()}}

            elif algo == "clustering":
                res = ana.clustering_coefficient_c(rel_type)
                local = {str(k): (round(v, 4) if v is not None else None)
                         for k, v in res.get("local", {}).items()}
                return {"ok": True, "type": "node_map",
                        "label": "Clustering Coefficient",
                        "data": local,
                        "scalar": f"Global: {round(res.get('global', 0), 4)}"}

            # --- paths ---
            elif algo == "dijkstra":
                src = int(params.get("src", 0))
                res = self.g.dijkstra(src, rel_type=rel_type, weight_key=w_key)
                data = {str(r["node_id"]): round(r["dist"], 4)
                        for r in res if r["dist"] >= 0}
                return {"ok": True, "type": "node_map",
                        "label": f"Dijkstra desde nodo {src}",
                        "data": data}

            elif algo == "k_shortest":
                src = int(params.get("src", 0))
                dst = int(params.get("dst", 0))
                k   = int(params.get("k", 3))
                res = ana.k_shortest_paths_c(src, dst, k=k,
                                              rel_type=rel_type, weight_key=w_key)
                paths = [{"nodes": p["nodes"], "cost": round(p["cost"], 4)}
                         for p in res]
                return {"ok": True, "type": "paths",
                        "label": f"{k} caminos mas cortos {src}->{dst}",
                        "data": paths}

            elif algo == "max_flow":
                src = int(params.get("src", 0))
                dst = int(params.get("dst", 0))
                cap = params.get("capacity_key", "rate_A_MVA")
                val = ana.max_flow_c(src, dst, rel_type, cap)
                return {"ok": True, "type": "scalar",
                        "label": f"Flujo maximo {src} -> {dst}",
                        "value": round(val, 4)}

            # --- global metrics ---
            elif algo == "density":
                val = ana.density(rel_type)
                return {"ok": True, "type": "scalar",
                        "label": "Densidad del grafo",
                        "value": round(val, 6)}

            elif algo == "fiedler":
                val = ana.fiedler_value(rel_type)
                return {"ok": True, "type": "scalar",
                        "label": "Valor de Fiedler (conectividad algebraica)",
                        "value": round(val, 6)}

            elif algo == "assortativity":
                val = ana.assortativity(rel_type)
                return {"ok": True, "type": "scalar",
                        "label": "Asortatividad",
                        "value": round(val, 4)}

            elif algo == "summary":
                data = ana.summary(rel_type)
                return {"ok": True, "type": "summary",
                        "label": "Resumen del grafo", "data": data}

            elif algo == "bess_ranking":
                n   = int(params.get("n", 10))
                res = ana.bess_siting_ranking(rel_type=rel_type or "LINEA",
                                               weight_key=w_key or "X_pu", n=n)
                return {"ok": True, "type": "ranking",
                        "label": f"Top {n} barras candidatas BESS",
                        "data": [[str(nid), round(score, 6)]
                                  for nid, score in res]}

            elif algo == "vulnerability":
                res = ana.vulnerability_index(rel_type)
                return {"ok": True, "type": "edge_list",
                        "label": "Indice de vulnerabilidad de aristas",
                        "data": [[str(rid), src, dst, idx]
                                  for rid, src, dst, idx in res[:20]]}

            else:
                return {"ok": False, "error": f"Algoritmo desconocido: {algo}"}

        except Exception as e:
            self._log("ERROR", f"run_algorithm({algo}): {e}")
            return {"ok": False, "error": str(e)}

    # ----------------------------------------------------------------
    # PTDF / LODF analysis
    # ----------------------------------------------------------------

    def run_ptdf_analysis(self, action, params):
        """
        Interfaz unificada para analisis PTDF/LODF desde el visualizador.

        action  : str — nombre de la operacion
        params  : dict — parametros de la operacion

        Operaciones soportadas:
            cargar_ptdf        — carga PTDF desde archivo CSV
            sensibilidad_rama  — fila de la PTDF para una rama
            impacto_bus        — columna de la PTDF para un bus
            ptdf_cruzado       — valor puntual PTDF[rama, bus]
            n1_electrico       — dictamen N-1 electrico completo
            ranking_n1         — ranking de contingencias criticas
            flujos_post        — flujos post-contingencia
            resumen_ptdf       — metricas de resumen de la PTDF cargada
        """
        try:
            self._require_db()
            import graphdb_algorithms as ga
            from graphdb_ptdf import GraphPTDF
            from graphdb_lodf import GraphLODF

            # Estado PTDF/LODF persistido en el API object
            if not hasattr(self, "_gptdf"):
                self._gptdf = None
                self._glodf = None

            ana = ga.GraphAnalyzer(self.g)

            # ----------------------------------------------------------
            if action == "cargar_ptdf":
                import pandas as pd
                ptdf_path   = params.get("ptdf_csv", "")
                flows_path  = params.get("flows_csv", "")
                ratings_path = params.get("ratings_csv", "")

                if not os.path.exists(ptdf_path):
                    return {"ok": False,
                            "error": f"Archivo no encontrado: {ptdf_path}"}

                ptdf_df = pd.read_csv(ptdf_path, index_col=0)
                ptdf_df.columns = [int(c) for c in ptdf_df.columns]

                self._gptdf = ana.ptdf_from_dataframe(
                    ptdf_df,
                    rel_type=params.get("rel_type", "LINEA"),
                    persist_topk=True,
                )

                result = {"ok": True, "type": "summary",
                          "label": "PTDF cargada",
                          "data": self._gptdf.resumen()}

                if flows_path and os.path.exists(flows_path):
                    flows_df = pd.read_csv(flows_path)
                    flows_s  = pd.Series(
                        flows_df.iloc[:, 1].values,
                        index=flows_df.iloc[:, 0].astype(str).values,
                    )
                    ratings_s = None
                    if ratings_path and os.path.exists(ratings_path):
                        rat_df    = pd.read_csv(ratings_path)
                        ratings_s = pd.Series(
                            rat_df.iloc[:, 1].values,
                            index=rat_df.iloc[:, 0].astype(str).values,
                        )
                    self._glodf = ana.lodf_from_ptdf(
                        self._gptdf, flows_s, ratings_s)
                    result["lodf"] = self._glodf.resumen()

                return result

            # ----------------------------------------------------------
            elif action == "sensibilidad_rama":
                if self._gptdf is None:
                    return {"ok": False,
                            "error": "PTDF no cargada. Usa cargar_ptdf primero."}
                label = params.get("label", "")
                s = self._gptdf.sensibilidad_rama(label)
                if s is None:
                    return {"ok": False,
                            "error": f"Rama '{label}' no encontrada"}
                top20 = s.abs().sort_values(ascending=False).head(20)
                return {
                    "ok": True, "type": "node_map",
                    "label": f"PTDF rama '{label}'",
                    "data": {str(b): round(float(s.loc[b]), 6)
                             for b in top20.index},
                }

            # ----------------------------------------------------------
            elif action == "impacto_bus":
                if self._gptdf is None:
                    return {"ok": False,
                            "error": "PTDF no cargada."}
                bus_num = int(params.get("bus_num", 0))
                s = self._gptdf.impacto_inyeccion_bus(bus_num, top_n=20)
                if s.empty:
                    return {"ok": False,
                            "error": f"Bus {bus_num} no en PTDF"}
                return {
                    "ok": True, "type": "edge_list",
                    "label": f"Impacto inyeccion bus {bus_num}",
                    "data": [[lbl, 0, 0, round(float(v), 6)]
                             for lbl, v in s.items()],
                }

            # ----------------------------------------------------------
            elif action == "ptdf_cruzado":
                if self._gptdf is None:
                    return {"ok": False, "error": "PTDF no cargada."}
                val = self._gptdf.ptdf_cruzado(
                    params.get("rama", ""),
                    int(params.get("bus_num", 0)),
                )
                return {
                    "ok": True, "type": "scalar",
                    "label": (f"PTDF['{params.get('rama')}', "
                              f"bus={params.get('bus_num')}]"),
                    "value": round(val, 6),
                }

            # ----------------------------------------------------------
            elif action == "n1_electrico":
                if self._glodf is None:
                    return {"ok": False,
                            "error": "LODF no calculada."}
                result = self._glodf.verificar_n1_electrico(
                    params.get("label", ""))
                return {
                    "ok": True, "type": "summary",
                    "label": f"N-1 electrico '{params.get('label')}'",
                    "data": result,
                }

            # ----------------------------------------------------------
            elif action == "ranking_n1":
                if self._glodf is None:
                    return {"ok": False, "error": "LODF no calculada."}
                n = int(params.get("n", 20))
                df = self._glodf.ranking_contingencias_criticas(n)
                return {
                    "ok": True, "type": "ranking",
                    "label": f"Top {n} contingencias N-1",
                    "data": [[r["label"],
                               r["max_loading_pct"],
                               r["n_overloads"]]
                              for _, r in df.iterrows()],
                }

            # ----------------------------------------------------------
            elif action == "flujos_post":
                if self._glodf is None:
                    return {"ok": False, "error": "LODF no calculada."}
                label_outage = params.get("label", "")
                flows = self._glodf.flujos_post_contingencia(label_outage)
                if flows is None:
                    return {"ok": False,
                            "error": f"Contingencia '{label_outage}' "
                                     f"no valida (puente o singular)"}
                top15 = flows.abs().sort_values(ascending=False).head(15)
                return {
                    "ok": True, "type": "edge_list",
                    "label": f"Flujos post '{label_outage}'",
                    "data": [[lbl, 0, 0, round(float(flows.loc[lbl]), 2)]
                             for lbl in top15.index],
                }

            # ----------------------------------------------------------
            elif action == "resumen_ptdf":
                if self._gptdf is None:
                    return {"ok": False, "error": "PTDF no cargada."}
                data = self._gptdf.resumen()
                if self._glodf is not None:
                    data.update({"lodf_" + k: v
                                 for k, v in self._glodf.resumen().items()})
                return {"ok": True, "type": "summary",
                        "label": "Resumen PTDF/LODF", "data": data}

            else:
                return {"ok": False,
                        "error": f"Accion desconocida: {action}"}

        except Exception as e:
            self._log("ERROR", f"run_ptdf_analysis({action}): {e}")
            return {"ok": False, "error": str(e)}

    # ----------------------------------------------------------------
    # Export PNG
    # ----------------------------------------------------------------

    def save_png(self, png_base64, filename):
        import base64
        try:
            export_dir = (os.path.dirname(os.path.abspath(self.db_path))
                          if self.db_path else os.getcwd())
            filepath = os.path.join(export_dir, filename)
            data = png_base64.split(",")[1] if "," in png_base64 else png_base64
            with open(filepath, "wb") as f:
                f.write(base64.b64decode(data))
            self._log("INFO", f"PNG guardado: {filepath}")
            return {"ok": True, "path": filepath}
        except Exception as e:
            self._log("ERROR", f"save_png: {e}")
            return {"ok": False, "error": str(e)}

    def close(self):
        if self.g:
            try:
                self.g.close()
            except Exception:
                pass


# ----------------------------------------------------------------
# Main — opens directly, NO file dialog before webview starts
# ----------------------------------------------------------------

def main():
    global _window

    api       = GraphAPI()
    ui_dir    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ui")
    html_path = os.path.join(ui_dir, "index.html")
    html_url  = "file:///" + html_path.replace("\\", "/")

    print(f"[graphdb] UI : {html_url}")
    print(f"[graphdb] Existe: {os.path.exists(html_path)}")

    _window = webview.create_window(
        title="graphdb Visualizer",
        url=html_url,
        js_api=api,
        width=1440,
        height=920,
        resizable=True,
        min_size=(900, 600),
    )
    _window.events.closed += api.close
    webview.start(debug=False)


if __name__ == "__main__":
    main()
