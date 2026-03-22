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
