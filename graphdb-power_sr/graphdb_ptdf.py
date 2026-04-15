"""
graphdb_ptdf.py

GraphPTDF — integra una matriz PTDF pre-calculada (pd.DataFrame producido
por PTDFCalculator) con una instancia de graphdb.Graph.

Responsabilidades:
    1. Persistir la PTDF en graphdb: escribe la sensibilidad de cada rama
       como propiedad JSON en la relacion correspondiente (solo top-K buses
       por magnitud absoluta, para no inflar la BD).
    2. Proveer consultas rapidas sin pasar por el motor C: sensibilidad de
       una rama, ranking de ramas mas impactadas, columna de un bus.
    3. Detectar puentes topologicos usando el motor C (find_bridges) para
       excluir contingencias invalidas en GraphLODF.
    4. Mantener el mapa label -> rel_id para escritura en la BD.

Diseno deliberado:
    - NO recalcula la PTDF. La recibe como pd.DataFrame ya construida por
      PTDFCalculator (que usa CasADi/KLU internamente).
    - La PTDF se almacena en RAM como ndarray para operaciones vectorizadas;
      la BD recibe solo las columnas top-K para consultas Cypher.
    - La clase es stateless respecto al flujo de potencia: solo sabe de
      sensibilidades lineales y topologia.

Formato esperado de ptdf_df:
    - Index  : etiquetas de rama (str), ej. "L 1-2", "T2D 30-38"
    - Columns: numeros de bus PSS/E (int o str convertible a int)
    - Valores: float, PTDF[l, n] = cambio en flujo de rama l ante inyeccion
               unitaria en bus n con referencia en slack
"""

import json
import numpy as np
import pandas as pd


# -----------------------------------------------------------------------
# Constantes
# -----------------------------------------------------------------------

PTDF_TOP_K_BUSES   = 20    # max buses a persistir por rama en la BD
PTDF_ZERO_THRESH   = 1e-6  # umbral para considerar sensibilidad nula


class GraphPTDF:
    """
    Envuelve una PTDF DataFrame y la integra con graphdb.

    Parameters
    ----------
    graph : graphdb.Graph
        Instancia abierta de graphdb.
    ptdf_df : pd.DataFrame
        Matriz PTDF con shape (n_ramas, n_buses).
        Index = etiquetas de rama, columns = numeros de bus PSS/E.
    rel_type : str
        Tipo de relacion en graphdb que representa las ramas
        (default "LINEA"; tambien se usara TRANSFORMADOR_2W si
        las etiquetas del PTDF incluyen transformadores).
    persist_topk : bool
        Si True, escribe las top-K sensibilidades en la BD al construir.
    """

    def __init__(self, graph, ptdf_df: pd.DataFrame,
                 rel_type: str = "LINEA",
                 persist_topk: bool = True):

        self.g        = graph
        self.rel_type = rel_type

        # Normalizar index y columns a str/int respectivamente
        self._ptdf_df = ptdf_df.copy()
        self._ptdf_df.index   = [str(i) for i in self._ptdf_df.index]
        self._ptdf_df.columns = [int(c) for c in self._ptdf_df.columns]

        # ndarray para operaciones vectorizadas
        self._ptdf_arr  = self._ptdf_df.values.astype(np.float64)
        self._labels    = list(self._ptdf_df.index)       # lista de etiquetas
        self._bus_nums  = list(self._ptdf_df.columns)     # lista de numeros de bus

        # Mapas de acceso rapido
        self._label_to_row = {lbl: i for i, lbl in enumerate(self._labels)}
        self._bus_to_col   = {b: j for j, b in enumerate(self._bus_nums)}

        # Mapa label -> rel_id en graphdb (se construye en _build_label_map)
        self._label_to_relid: dict[str, int] = {}
        self._build_label_map()

        # Cache de puentes (etiquetas de ramas criticas N-1 topologico)
        self._bridges: set[str] = set()
        self._detect_bridges()

        if persist_topk:
            self.persist_to_graph(top_k=PTDF_TOP_K_BUSES)

    # -------------------------------------------------------------------
    # Construccion interna
    # -------------------------------------------------------------------

    def _build_label_map(self) -> None:
        """
        Construye el mapa etiqueta_rama -> rel_id consultando graphdb.
        Usa la propiedad label almacenada en cada relacion durante la carga
        con base_grafo.py (campo 'label' o 'etiqueta' en properties JSON).
        Fallback: busca por src_bus y dst_bus si no hay label exacta.
        """
        try:
            rows = self.g.query(
                "MATCH (a)-[r]->(b) RETURN r"
            )
        except Exception:
            return

        for row in rows:
            try:
                rid = int(row)
            except (ValueError, TypeError):
                continue
            rel = self.g.get_rel(rid)
            if not rel:
                continue
            try:
                props = json.loads(rel.get("properties", "{}"))
            except Exception:
                props = {}

            # Intentar match por campo label en propiedades
            lbl = props.get("label", props.get("etiqueta", ""))
            if lbl and str(lbl) in self._label_to_row:
                self._label_to_relid[str(lbl)] = rid
                continue

            # Fallback: construir etiqueta desde src/dst bus
            src_id = rel.get("src_id")
            dst_id = rel.get("dst_id")
            if src_id is None or dst_id is None:
                continue
            src_node = self.g.get_node(src_id)
            dst_node = self.g.get_node(dst_id)
            if not src_node or not dst_node:
                continue
            try:
                src_props = json.loads(src_node.get("properties", "{}"))
                dst_props = json.loads(dst_node.get("properties", "{}"))
                src_num   = int(src_props.get("numero", src_id))
                dst_num   = int(dst_props.get("numero", dst_id))
            except Exception:
                continue

            # Probar formatos de etiqueta: "L src-dst" y "T2D src-dst"
            for prefix in ("L", "T2D", "T3D-arm"):
                candidate = f"{prefix} {src_num}-{dst_num}"
                if candidate in self._label_to_row:
                    self._label_to_relid[candidate] = rid
                    break

    def _detect_bridges(self) -> None:
        """
        Detecta puentes usando el motor C de graphdb (Tarjan O(V+E)).
        Solo se marcan como puentes las ramas que ademas estan en la PTDF.
        """
        try:
            bridge_rids = self.g.find_bridges(rel_type=self.rel_type)
        except Exception:
            bridge_rids = []

        # Invertir mapa relid -> label
        relid_to_label = {v: k for k, v in self._label_to_relid.items()}
        for rid in bridge_rids:
            lbl = relid_to_label.get(int(rid))
            if lbl and lbl in self._label_to_row:
                self._bridges.add(lbl)

    # -------------------------------------------------------------------
    # Persistencia en graphdb
    # -------------------------------------------------------------------

    def persist_to_graph(self, top_k: int = PTDF_TOP_K_BUSES) -> int:
        """
        Escribe las top-K sensibilidades de cada rama en graphdb como
        propiedad 'ptdf_topk' de la relacion.

        Formato almacenado (JSON):
            {"buses": [bus1, bus2, ...], "vals": [v1, v2, ...]}
        Los buses estan ordenados por |ptdf| descendente.

        Returns
        -------
        int : numero de relaciones actualizadas.
        """
        updated = 0
        for lbl, rid in self._label_to_relid.items():
            row_idx = self._label_to_row.get(lbl)
            if row_idx is None:
                continue

            row = self._ptdf_arr[row_idx]
            abs_row = np.abs(row)
            # Indices de los top-K valores absolutos
            if len(abs_row) > top_k:
                top_idx = np.argpartition(abs_row, -top_k)[-top_k:]
                top_idx = top_idx[np.argsort(abs_row[top_idx])[::-1]]
            else:
                top_idx = np.argsort(abs_row)[::-1]

            top_buses = [self._bus_nums[i] for i in top_idx
                         if abs_row[i] > PTDF_ZERO_THRESH]
            top_vals  = [round(float(row[i]), 6) for i in top_idx
                         if abs(float(row[i])) > PTDF_ZERO_THRESH]

            if not top_buses:
                continue

            payload = json.dumps({"buses": top_buses, "vals": top_vals})
            try:
                self.g.set_rel_property(rid, "ptdf_topk", payload)
                updated += 1
            except Exception:
                pass

        return updated

    # -------------------------------------------------------------------
    # Consultas sobre la PTDF
    # -------------------------------------------------------------------

    def sensibilidad_rama(self, label: str) -> pd.Series | None:
        """
        Retorna la fila completa de la PTDF para la rama indicada.

        Returns pd.Series indexada por numero de bus, o None si la
        etiqueta no existe.
        """
        row_idx = self._label_to_row.get(str(label))
        if row_idx is None:
            return None
        return pd.Series(self._ptdf_arr[row_idx], index=self._bus_nums,
                         name=label)

    def impacto_inyeccion_bus(self, bus_num: int,
                               top_n: int = 20) -> pd.Series:
        """
        Retorna las top_n ramas mas impactadas por una inyeccion unitaria
        en bus_num (columna de la PTDF), ordenadas por |PTDF| descendente.
        """
        col_idx = self._bus_to_col.get(int(bus_num))
        if col_idx is None:
            return pd.Series(dtype=float)

        col = self._ptdf_arr[:, col_idx]
        abs_col = np.abs(col)
        top_idx = np.argsort(abs_col)[::-1][:top_n]

        return pd.Series(
            [col[i] for i in top_idx],
            index=[self._labels[i] for i in top_idx],
            name=f"PTDF_bus_{bus_num}",
        )

    def ptdf_cruzado(self, rama_afectada: str, bus_num: int) -> float:
        """
        Retorna PTDF[rama_afectada, bus_num]: cambio en flujo de
        rama_afectada ante inyeccion unitaria en bus_num.
        """
        row_idx = self._label_to_row.get(str(rama_afectada))
        col_idx = self._bus_to_col.get(int(bus_num))
        if row_idx is None or col_idx is None:
            return 0.0
        return float(self._ptdf_arr[row_idx, col_idx])

    def ramas_mas_sensibles(self, top_n: int = 10) -> pd.DataFrame:
        """
        Retorna las top_n ramas con mayor sensibilidad maxima absoluta
        (max sobre todos los buses).

        Columns: ['label', 'max_abs_ptdf', 'bus_max']
        """
        max_abs = np.max(np.abs(self._ptdf_arr), axis=1)
        bus_max_idx = np.argmax(np.abs(self._ptdf_arr), axis=1)
        top_idx = np.argsort(max_abs)[::-1][:top_n]

        return pd.DataFrame({
            "label":       [self._labels[i]        for i in top_idx],
            "max_abs_ptdf":[round(float(max_abs[i]), 6) for i in top_idx],
            "bus_max":     [self._bus_nums[bus_max_idx[i]] for i in top_idx],
        })

    # -------------------------------------------------------------------
    # Acceso a estructuras internas
    # -------------------------------------------------------------------

    @property
    def ptdf_df(self) -> pd.DataFrame:
        """DataFrame PTDF completo (n_ramas x n_buses)."""
        return self._ptdf_df

    @property
    def ptdf_array(self) -> np.ndarray:
        """ndarray PTDF (n_ramas x n_buses) para operaciones vectorizadas."""
        return self._ptdf_arr

    @property
    def labels(self) -> list[str]:
        """Lista de etiquetas de ramas en el mismo orden que las filas."""
        return self._labels

    @property
    def bus_nums(self) -> list[int]:
        """Lista de numeros de bus en el mismo orden que las columnas."""
        return self._bus_nums

    @property
    def bridges(self) -> set[str]:
        """Conjunto de etiquetas de ramas que son puentes topologicos."""
        return self._bridges

    @property
    def n_branches(self) -> int:
        return len(self._labels)

    @property
    def n_buses(self) -> int:
        return len(self._bus_nums)

    def label_to_row(self, label: str) -> int | None:
        return self._label_to_row.get(str(label))

    def bus_to_col(self, bus_num: int) -> int | None:
        return self._bus_to_col.get(int(bus_num))

    # -------------------------------------------------------------------
    # Diagnostico
    # -------------------------------------------------------------------

    def resumen(self) -> dict:
        """Diccionario con metricas de diagnostico de la PTDF."""
        return {
            "n_branches":        self.n_branches,
            "n_buses":           self.n_buses,
            "n_bridges":         len(self._bridges),
            "n_mapped_to_graph": len(self._label_to_relid),
            "max_ptdf":          float(np.max(np.abs(self._ptdf_arr))),
            "mean_abs_ptdf":     float(np.mean(np.abs(self._ptdf_arr))),
            "sparsity_pct":      round(
                100.0 * np.sum(np.abs(self._ptdf_arr) < PTDF_ZERO_THRESH)
                / self._ptdf_arr.size, 2),
        }
