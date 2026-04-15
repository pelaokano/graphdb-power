"""
graphdb_lodf.py

GraphLODF — calcula y almacena la matriz LODF (Line Outage Distribution
Factors) a partir de un GraphPTDF, y provee analisis de contingencias N-1.

Matematica (identica a ScenarioGeneratorUnificado):
    Para la contingencia de la rama k (buses from_k, to_k):

        BSF[l]     = PTDF[l, from_k] - PTDF[l, to_k]    (Branch Sensitivity Factor)
        denom      = 1 - BSF[k]
        LODF[l, k] = BSF[l] / denom                      (singular si |denom| < tol)

    Flujos post-contingencia:
        F_post[l]       = F_base[l] + LODF[l, k] * F_base[k]
        F_post[k]       = 0                               (rama k desconectada)

    PTDF post-contingencia:
        PTDF_post[l, n] = PTDF_base[l, n] + LODF[l, k] * PTDF_base[k, n]
        PTDF_post[k, :] = 0

Diseno:
    - La LODF completa (n_ramas x n_ramas) se calcula en RAM una sola vez.
    - Las contingencias singulares (|denom| < LODF_TOL) se descartan.
    - Los puentes (detectados por el motor C en GraphPTDF) se excluyen.
    - El dict de contingencias tiene el mismo formato que usa
      ScenarioGeneratorUnificado para compatibilidad directa.
    - Los resultados de contingencias criticas se persisten en graphdb
      como propiedades de la relacion outaged.

Dependencias: numpy, pandas, graphdb_ptdf.GraphPTDF
"""

import json
import numpy as np
import pandas as pd

from graphdb_ptdf import GraphPTDF


# -----------------------------------------------------------------------
# Constantes
# -----------------------------------------------------------------------

LODF_TOL          = 1e-6   # denominador minimo para contingencia valida
OVERLOAD_THRESH   = 1.0    # fraccion de rating (1.0 = 100%)
MAX_NATURAL_OVER  = 2      # max ramas sobrecargadas naturalmente aceptadas
PERSIST_TOP_N     = 20     # top contingencias a persistir en la BD


class GraphLODF:
    """
    Calcula la matriz LODF y el analisis de contingencias N-1.

    Parameters
    ----------
    graph_ptdf : GraphPTDF
        Instancia de GraphPTDF con PTDF base ya cargada.
    base_flows_mw : pd.Series
        Flujos base en MW indexados por etiqueta de rama.
        El orden debe coincidir con ptdf_df.index.
    ratings_mw : pd.Series | None
        Capacidades termicas en MW indexadas por etiqueta de rama.
        Si None, no se aplica filtro de sobrecarga.
    lodf_tol : float
        Tolerancia para descartar contingencias singulares.
    max_natural_overloads : int
        Maximo de sobrecargas naturales post-contingencia aceptadas.
    """

    def __init__(
        self,
        graph_ptdf: GraphPTDF,
        base_flows_mw: pd.Series,
        ratings_mw: pd.Series | None = None,
        lodf_tol: float = LODF_TOL,
        max_natural_overloads: int = MAX_NATURAL_OVER,
    ):
        self._gptdf   = graph_ptdf
        self._lodf_tol = lodf_tol
        self._max_nat  = max_natural_overloads

        # Alinear flujos base al orden de la PTDF
        self._flows_base = self._align_series(
            base_flows_mw, graph_ptdf.labels, "base_flows_mw")

        # Alinear ratings (si existen)
        if ratings_mw is not None:
            self._ratings = self._align_series(
                ratings_mw, graph_ptdf.labels, "ratings_mw")
        else:
            self._ratings = np.ones(graph_ptdf.n_branches)

        # Matrices principales
        self._lodf_matrix: np.ndarray | None = None  # (n_br, n_br)
        self._contingencias: dict = {}                # idx -> info

        # Calcular
        self._calcular_lodf()
        self._precomputar_contingencias()

    # -------------------------------------------------------------------
    # Alineacion de series
    # -------------------------------------------------------------------

    @staticmethod
    def _align_series(series: pd.Series,
                      labels: list[str],
                      name: str) -> np.ndarray:
        """
        Reindexea una pd.Series al orden de labels.
        Rellena con 0.0 si faltan etiquetas.
        """
        aligned = series.reindex([str(l) for l in labels], fill_value=0.0)
        n_missing = aligned.isna().sum()
        if n_missing > 0:
            print(f"[GraphLODF] Advertencia: {n_missing} etiquetas faltantes "
                  f"en {name}, rellenadas con 0.0")
        return aligned.fillna(0.0).values.astype(np.float64)

    # -------------------------------------------------------------------
    # Calculo de la LODF completa
    # -------------------------------------------------------------------

    def _calcular_lodf(self) -> None:
        """
        Calcula la matriz LODF completa (n_ramas x n_ramas).

        LODF[:, k] = BSF[:] / (1 - BSF[k])
        donde BSF[l] = PTDF[l, from_k] - PTDF[l, to_k]

        Las columnas correspondientes a puentes o denominadores singulares
        se dejan en nan para identificarlas facilmente.
        """
        ptdf = self._gptdf.ptdf_array        # (n_br, n_bus)
        labels  = self._gptdf.labels
        bus_map = self._gptdf.bus_to_col
        edges   = self._get_branch_endpoints()

        n_br = self._gptdf.n_branches
        lodf = np.full((n_br, n_br), np.nan)

        for k, label_k in enumerate(labels):
            if label_k in self._gptdf.bridges:
                continue  # puente: LODF indefinido

            ep = edges.get(label_k)
            if ep is None:
                continue

            from_bus, to_bus = ep
            col_from = bus_map(from_bus)
            col_to   = bus_map(to_bus)

            # BSF para todos los brazos
            bsf_from = ptdf[:, col_from] if col_from is not None \
                       else np.zeros(n_br)
            bsf_to   = ptdf[:, col_to]   if col_to   is not None \
                       else np.zeros(n_br)
            bsf = bsf_from - bsf_to

            denom = 1.0 - float(bsf[k])
            if abs(denom) < self._lodf_tol:
                continue  # singular

            lodf[:, k] = bsf / denom

        self._lodf_matrix = lodf

    def _get_branch_endpoints(self) -> dict[str, tuple[int, int]]:
        """
        Extrae los pares (from_bus, to_bus) de cada rama en la PTDF
        consultando graphdb.
        """
        g = self._gptdf.g
        edges: dict[str, tuple[int, int]] = {}

        # Intentar desde el mapa label_to_relid
        for label, rid in self._gptdf._label_to_relid.items():
            rel = g.get_rel(rid)
            if not rel:
                continue
            src_node = g.get_node(rel["src_id"])
            dst_node = g.get_node(rel["dst_id"])
            if not src_node or not dst_node:
                continue
            try:
                sp = json.loads(src_node.get("properties", "{}"))
                dp = json.loads(dst_node.get("properties", "{}"))
                src_num = int(sp.get("numero", rel["src_id"]))
                dst_num = int(dp.get("numero", rel["dst_id"]))
                edges[label] = (src_num, dst_num)
            except Exception:
                pass

        # Fallback: parsear desde la propia etiqueta "L from-to" o "T2D from-to"
        for label in self._gptdf.labels:
            if label in edges:
                continue
            try:
                parts = label.split()
                if len(parts) >= 2 and '-' in parts[1]:
                    nums = parts[1].split('-')
                    edges[label] = (int(nums[0]), int(nums[1]))
            except Exception:
                pass

        return edges

    # -------------------------------------------------------------------
    # Pre-computo de contingencias viables
    # -------------------------------------------------------------------

    def _precomputar_contingencias(self) -> None:
        """
        Para cada rama no-puente y no-singular, calcula el estado
        post-contingencia y lo almacena en self._contingencias.

        Formato de cada entrada (compatible con ScenarioGeneratorUnificado):
            {
              'label'       : str,
              'idx_base'    : int,
              'ptdf_df'     : pd.DataFrame,
              'flows_arr'   : np.ndarray,
              'flows_mw'    : pd.Series,
              'lodf_col'    : np.ndarray,  # columna LODF de esta contingencia
              'n_natural'   : int,
              'max_natural' : float,
              'is_bridge'   : bool,
            }
        """
        if self._lodf_matrix is None:
            return

        ptdf_base = self._gptdf.ptdf_array    # (n_br, n_bus)
        labels    = self._gptdf.labels
        bus_nums  = self._gptdf.bus_nums
        flows     = self._flows_base
        ratings   = self._ratings

        for k, label_k in enumerate(labels):
            # Solo contingencias con LODF calculada (columna sin nan)
            lodf_col = self._lodf_matrix[:, k]
            if np.any(np.isnan(lodf_col)):
                continue

            # Flujos post-contingencia
            flows_post = flows + lodf_col * float(flows[k])
            flows_post[k] = 0.0

            # PTDF post-contingencia
            ptdf_post = ptdf_base + np.outer(lodf_col, ptdf_base[k, :])
            ptdf_post[k, :] = 0.0

            # Verificacion de sobrecargas naturales
            safe_ratings = np.where(ratings > 1e-3, ratings, 1.0)
            loadings     = np.abs(flows_post) / safe_ratings
            n_natural    = int(np.sum(loadings > OVERLOAD_THRESH))
            max_natural  = float(np.max(loadings))

            if n_natural > self._max_nat:
                continue

            ptdf_post_df = pd.DataFrame(
                ptdf_post,
                index=labels,
                columns=bus_nums,
            )

            self._contingencias[k] = {
                "label":        label_k,
                "idx_base":     k,
                "ptdf_df":      ptdf_post_df,
                "flows_arr":    flows_post,
                "flows_mw":     pd.Series(flows_post, index=labels),
                "lodf_col":     lodf_col,
                "n_natural":    n_natural,
                "max_natural":  round(max_natural * 100.0, 2),
                "is_bridge":    label_k in self._gptdf.bridges,
            }

        # Persistir las contingencias mas criticas en graphdb
        self._persistir_contingencias_criticas(top_n=PERSIST_TOP_N)

    def _persistir_contingencias_criticas(self, top_n: int) -> None:
        """
        Escribe las top_n contingencias mas criticas (por max_natural
        loading) en graphdb como propiedades de la relacion outaged.

        Propiedades escritas:
            lodf_max_loading_pct : float  — maxima carga post-contingencia
            lodf_n_overloads     : int    — numero de sobrecargas
            lodf_top_impacted    : str    — JSON con las 5 ramas mas afectadas
        """
        if not self._contingencias:
            return

        sorted_conts = sorted(
            self._contingencias.items(),
            key=lambda x: x[1]["max_natural"],
            reverse=True,
        )[:top_n]

        g           = self._gptdf.g
        rid_map     = self._gptdf._label_to_relid

        for k, info in sorted_conts:
            label = info["label"]
            rid   = rid_map.get(label)
            if rid is None:
                continue

            # Top 5 ramas mas impactadas por esta contingencia
            lodf_col = info["lodf_col"]
            top5_idx = np.argsort(np.abs(lodf_col))[::-1][:5]
            top5 = {
                self._gptdf.labels[i]: round(float(lodf_col[i]), 6)
                for i in top5_idx
            }

            try:
                g.set_rel_property(
                    rid, "lodf_max_loading_pct",
                    str(info["max_natural"]))
                g.set_rel_property(
                    rid, "lodf_n_overloads",
                    str(info["n_natural"]))
                g.set_rel_property(
                    rid, "lodf_top_impacted",
                    json.dumps(top5))
            except Exception:
                pass

    # -------------------------------------------------------------------
    # Consultas sobre LODF y contingencias
    # -------------------------------------------------------------------

    def flujos_post_contingencia(
            self, label_outage: str) -> pd.Series | None:
        """
        Retorna los flujos en todas las ramas tras la salida de label_outage.
        """
        k = self._gptdf.label_to_row(label_outage)
        if k is None or k not in self._contingencias:
            return None
        info = self._contingencias[k]
        return info["flows_mw"]

    def ptdf_post_contingencia(
            self, label_outage: str) -> pd.DataFrame | None:
        """
        Retorna la PTDF post-contingencia tras la salida de label_outage.
        """
        k = self._gptdf.label_to_row(label_outage)
        if k is None or k not in self._contingencias:
            return None
        return self._contingencias[k]["ptdf_df"]

    def lodf_columna(self, label_outage: str) -> pd.Series | None:
        """
        Retorna la columna k de la LODF: impacto de la contingencia
        label_outage sobre todas las ramas.
        """
        k = self._gptdf.label_to_row(label_outage)
        if k is None:
            return None
        col = self._lodf_matrix[:, k]
        if np.any(np.isnan(col)):
            return None
        return pd.Series(col, index=self._gptdf.labels,
                         name=f"LODF_outage_{label_outage}")

    def ranking_contingencias_criticas(
            self, top_n: int = 20) -> pd.DataFrame:
        """
        Ranking de contingencias por maxima carga post-contingencia.

        Returns pd.DataFrame con columnas:
            rank, label, max_loading_pct, n_overloads
        """
        if not self._contingencias:
            return pd.DataFrame(
                columns=["rank", "label",
                         "max_loading_pct", "n_overloads"])

        sorted_c = sorted(
            self._contingencias.values(),
            key=lambda x: x["max_natural"],
            reverse=True,
        )[:top_n]

        return pd.DataFrame([{
            "rank":             i + 1,
            "label":            c["label"],
            "max_loading_pct":  c["max_natural"],
            "n_overloads":      c["n_natural"],
        } for i, c in enumerate(sorted_c)])

    def verificar_n1_electrico(self, label_outage: str) -> dict:
        """
        Dictamen N-1 electrico completo para una rama:
            - is_bridge     : fallo topologico (motor C)
            - is_singular   : LODF indefinida (columna nan)
            - max_loading_pct: maxima carga post-contingencia (%)
            - n_overloads   : ramas que superan rating
            - top_overloaded: dict {label: loading_pct} de ramas sobrecargadas
            - verdict       : 'critico' | 'advertencia' | 'seguro' | 'puente'
        """
        result = {
            "label":          label_outage,
            "is_bridge":      label_outage in self._gptdf.bridges,
            "is_singular":    False,
            "max_loading_pct": 0.0,
            "n_overloads":    0,
            "top_overloaded": {},
            "verdict":        "seguro",
        }

        if result["is_bridge"]:
            result["verdict"] = "puente"
            return result

        k = self._gptdf.label_to_row(label_outage)
        if k is None:
            result["verdict"] = "no_encontrado"
            return result

        col = self._lodf_matrix[:, k] if self._lodf_matrix is not None \
              else np.full(self._gptdf.n_branches, np.nan)

        if np.any(np.isnan(col)):
            result["is_singular"] = True
            result["verdict"] = "singular"
            return result

        flows_post = self._flows_base + col * float(self._flows_base[k])
        flows_post[k] = 0.0

        safe_ratings = np.where(self._ratings > 1e-3, self._ratings, 1.0)
        loadings_pct = np.abs(flows_post) / safe_ratings * 100.0

        result["max_loading_pct"] = round(float(np.max(loadings_pct)), 2)
        overloaded_idx = np.where(loadings_pct > 100.0)[0]
        result["n_overloads"] = len(overloaded_idx)

        if result["n_overloads"] > 0:
            result["top_overloaded"] = {
                self._gptdf.labels[i]: round(float(loadings_pct[i]), 2)
                for i in sorted(overloaded_idx,
                                key=lambda x: loadings_pct[x],
                                reverse=True)[:5]
            }

        if result["n_overloads"] > MAX_NATURAL_OVER:
            result["verdict"] = "critico"
        elif result["n_overloads"] > 0:
            result["verdict"] = "advertencia"
        elif result["max_loading_pct"] > 90.0:
            result["verdict"] = "advertencia"
        else:
            result["verdict"] = "seguro"

        return result

    # -------------------------------------------------------------------
    # Acceso a estructuras internas
    # -------------------------------------------------------------------

    @property
    def lodf_matrix(self) -> np.ndarray | None:
        """Matriz LODF completa (n_ramas x n_ramas). nan = indefinido."""
        return self._lodf_matrix

    @property
    def contingencias(self) -> dict:
        """
        Dict de contingencias viables en formato compatible con
        ScenarioGeneratorUnificado: {idx_base: info_dict}.
        """
        return self._contingencias

    @property
    def graph_ptdf(self) -> GraphPTDF:
        return self._gptdf

    @property
    def n_contingencias(self) -> int:
        return len(self._contingencias)

    # -------------------------------------------------------------------
    # Diagnostico
    # -------------------------------------------------------------------

    def resumen(self) -> dict:
        """Metricas de resumen del analisis LODF."""
        n_br      = self._gptdf.n_branches
        n_bridges = len(self._gptdf.bridges)

        if self._lodf_matrix is not None:
            n_valid_cols = int(np.sum(
                ~np.any(np.isnan(self._lodf_matrix), axis=0)))
        else:
            n_valid_cols = 0

        r = {
            "n_branches":           n_br,
            "n_bridges":            n_bridges,
            "n_valid_lodf_columns": n_valid_cols,
            "n_contingencias":      self.n_contingencias,
            "n_singular":           n_br - n_bridges - n_valid_cols,
        }

        if self._contingencias:
            max_loads = [c["max_natural"]
                         for c in self._contingencias.values()]
            r["max_loading_pct_worst"] = round(max(max_loads), 2)
            r["mean_loading_pct"]      = round(
                float(np.mean(max_loads)), 2)
            r["n_criticas"] = sum(
                1 for c in self._contingencias.values()
                if c["n_natural"] > MAX_NATURAL_OVER)

        return r
