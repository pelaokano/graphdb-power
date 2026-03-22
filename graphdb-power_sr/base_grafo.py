import json
import graphdb
from read_raw2 import RawParser


class GraphDbLoader:
    """
    Carga una red electrica parseada desde un .raw de PSS/E
    hacia la base de datos de grafos propia (graphdb).

    Schema resultante:
        (Bus)-[:LINEA]->(Bus)
        (Bus)-[:TRANSFORMADOR_2W]->(Bus)
        (Bus)-[:CONEXION_XF]->(VirtualNode)   <- transformadores 3 devanados
        (Bus)-[:GENERACION]->(Generator)
        (Bus)-[:CONSUMO]->(Load)
        (Bus)-[:SHUNT]->(Shunt)
    """

    def __init__(self, db_path):
        self.db_path = db_path
        self.g = graphdb.Graph(db_path)
        # mapa bus_numero -> node_id en graphdb
        self._bus_map = {}

    def close(self):
        self.g.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def limpiar_base_de_datos(self):
        rows = self.g.query("MATCH (n) RETURN n")
        for row in rows:
            try:
                nid = int(row)
                self.g.delete_node(nid, detach=True)
            except ValueError:
                pass
        self._bus_map = {}
        print("Base de datos limpiada.")

    def cargar_datos(self, data):
        if data['buses'] is not None:
            print(f"Cargando {len(data['buses'])} barras...")
            self._cargar_buses(data['buses'])

        if data['lineas'] is not None:
            print(f"Cargando {len(data['lineas'])} lineas...")
            self._cargar_lineas(data['lineas'])

        if data['transformadores'] is not None:
            print(f"Cargando {len(data['transformadores'])} transformadores...")
            self._cargar_transformadores(data['transformadores'])

        if data['generadores'] is not None:
            print(f"Cargando {len(data['generadores'])} generadores...")
            self._cargar_hojas(data['generadores'], "Generator", "GENERACION")

        if data['cargas'] is not None:
            print(f"Cargando {len(data['cargas'])} cargas...")
            self._cargar_hojas(data['cargas'], "Load", "CONSUMO")

        if data['shunts'] is not None:
            print(f"Cargando {len(data['shunts'])} shunts...")
            self._cargar_hojas(data['shunts'], "Shunt", "SHUNT")

    # ----------------------------------------------------------------
    # Buses
    # ----------------------------------------------------------------

    def _cargar_buses(self, df):
        for _, row in df.iterrows():
            props = {
                'numero':    int(row['numero']),
                'nombre':    str(row.get('nombre', '')),
                'V_base_kV': float(row.get('V_base_kV', 138.0)),
                'tipo':      int(row.get('tipo', 1)),
                'area':      int(row.get('area', 1)),
                'zona':      int(row.get('zona', 1)),
                'zoname':    str(row.get('zoname', '')),
                'V_mag_pu':  float(row.get('V_mag_pu', 1.0)),
                'V_ang_deg': float(row.get('V_ang_deg', 0.0)),
                'V_max_pu':  float(row.get('V_max_pu', 1.1)),
                'V_min_pu':  float(row.get('V_min_pu', 0.9)),
            }
            nid = self.g.create_node("Bus", json.dumps(props))
            self._bus_map[int(row['numero'])] = nid

    # ----------------------------------------------------------------
    # Lineas
    # ----------------------------------------------------------------

    def _cargar_lineas(self, df):
        errores = 0
        for _, row in df.iterrows():
            src_num = int(row['from_bus_numero'])
            dst_num = int(row['to_bus_numero'])

            src_id = self._bus_map.get(src_num)
            dst_id = self._bus_map.get(dst_num)

            if src_id is None or dst_id is None:
                errores += 1
                continue

            props = {
                'circuito':   str(row.get('_CKT', '1')),
                'R_pu':       float(row.get('R_pu', 0.0)),
                'X_pu':       float(row.get('X_pu', 0.0)),
                'B_pu':       float(row.get('B_pu', 0.0)),
                'rate_A_MVA': float(row.get('rate_A_MVA', 0.0)),
                'rate_B_MVA': float(row.get('rate_B_MVA', 0.0)),
                'rate_C_MVA': float(row.get('rate_C_MVA', 0.0)),
                'status':     int(row.get('status', 1)),
                'longitud_km':float(row.get('longitud_km', 0.0)),
            }
            self.g.create_rel(src_id, dst_id, "LINEA", json.dumps(props))

        if errores:
            print(f"  advertencia: {errores} lineas con barras no encontradas")

    # ----------------------------------------------------------------
    # Transformadores
    # ----------------------------------------------------------------

    def _cargar_transformadores(self, df):
        errores = 0
        for _, row in df.iterrows():
            from_num = int(row['from_bus_index'])
            to_num   = int(row['to_bus_index'])
            tert_num = int(row.get('tertiary_bus_index', 0))

            from_id = self._bus_map.get(from_num)
            to_id   = self._bus_map.get(to_num)

            if from_id is None or to_id is None:
                errores += 1
                continue

            is_3w = tert_num != 0

            if not is_3w:
                props = {
                    'ckt':      str(row.get('ckt_id', '1')),
                    'R1_2_pu':  float(row.get('R1_2_pu', 0.0)),
                    'X1_2_pu':  float(row.get('X1_2_pu', 0.0)),
                    'WINDV1':   float(row.get('WINDV1', 1.0)),
                    'ANG1':     float(row.get('ANG1', 0.0)),
                    'STAT':     int(row.get('STAT', 1)),
                }
                self.g.create_rel(from_id, to_id,
                                  "TRANSFORMADOR_2W", json.dumps(props))
            else:
                tert_id = self._bus_map.get(tert_num)
                if tert_id is None:
                    errores += 1
                    continue

                # nodo virtual que representa el devanado central
                v_id_str = f"XF3W_{from_num}_{to_num}_{tert_num}"
                vn_props = {
                    'id':       v_id_str,
                    'ckt':      str(row.get('ckt_id', '1')),
                    'R1_2_pu':  float(row.get('R1_2_pu', 0.0)),
                    'X1_2_pu':  float(row.get('X1_2_pu', 0.0)),
                    'R2_3_pu':  float(row.get('R2_3_pu', 0.0)),
                    'X2_3_pu':  float(row.get('X2_3_pu', 0.0)),
                    'R3_1_pu':  float(row.get('R3_1_pu', 0.0)),
                    'X3_1_pu':  float(row.get('X3_1_pu', 0.0)),
                    'STAT':     int(row.get('STAT', 1)),
                }
                vn_id = self.g.create_node("VirtualNode", json.dumps(vn_props))
                self.g.create_rel(from_id, vn_id, "CONEXION_XF", '{}')
                self.g.create_rel(to_id,   vn_id, "CONEXION_XF", '{}')
                self.g.create_rel(tert_id, vn_id, "CONEXION_XF", '{}')

        if errores:
            print(f"  advertencia: {errores} transformadores con barras no encontradas")

    # ----------------------------------------------------------------
    # Generadores, cargas y shunts (hojas conectadas a una barra)
    # ----------------------------------------------------------------

    def _cargar_hojas(self, df, label, rel_type):
        errores = 0
        for _, row in df.iterrows():
            bus_num = int(row['bus_numero'])
            bus_id  = self._bus_map.get(bus_num)

            if bus_id is None:
                errores += 1
                continue

            # convertir toda la fila a tipos serializables en JSON
            props = {}
            for k, v in row.items():
                if v is None or (isinstance(v, float) and v != v):
                    props[k] = None
                elif hasattr(v, 'item'):
                    props[k] = v.item()
                else:
                    props[k] = v

            leaf_id = self.g.create_node(label, json.dumps(props))
            self.g.create_rel(bus_id, leaf_id, rel_type, '{}')

        if errores:
            print(f"  advertencia: {errores} {label} con barras no encontradas")

    # ----------------------------------------------------------------
    # Consultas utiles post-carga
    # ----------------------------------------------------------------

    def resumen(self):
        """Imprime un resumen de los elementos cargados."""
        tipos = [
            ("Bus",         "barras"),
            ("Generator",   "generadores"),
            ("Load",        "cargas"),
            ("Shunt",       "shunts"),
            ("VirtualNode", "nodos virtuales XF3W"),
        ]
        print("\nResumen de la red cargada:")
        for label, nombre in tipos:
            rows = self.g.query(f"MATCH (n:{label}) RETURN count(n)")
            count = rows[0] if rows else "0"
            print(f"  {nombre}: {count}")

        rels = [
            ("LINEA",           "lineas"),
            ("TRANSFORMADOR_2W","transformadores 2D"),
            ("CONEXION_XF",     "conexiones XF3W"),
            ("GENERACION",      "conexiones generador-barra"),
            ("CONSUMO",         "conexiones carga-barra"),
            ("SHUNT",           "conexiones shunt-barra"),
        ]
        for rtype, nombre in rels:
            rows = self.g.query(
                f"MATCH (a)-[r:{rtype}]->(b) RETURN count(r)"
            )
            count = rows[0] if rows else "0"
            print(f"  {nombre}: {count}")

    def bus_por_numero(self, numero):
        """Devuelve el dict de propiedades de una barra por su numero PSS/E."""
        nid = self._bus_map.get(numero)
        if nid is None:
            return None
        node = self.g.get_node(nid)
        if node:
            return json.loads(node['properties'])
        return None

    def vecinos(self, bus_numero, rel_type=None):
        """
        Devuelve lista de barras adyacentes a bus_numero.
        rel_type: 'LINEA', 'TRANSFORMADOR_2W' o None para ambas.
        """
        nid = self._bus_map.get(bus_numero)
        if nid is None:
            return []

        if rel_type:
            q = (f"MATCH (a)-[r:{rel_type}]->(b) "
                 f"WHERE a = {nid} RETURN b, r.X_pu")
        else:
            q = (f"MATCH (a)-[r]->(b) "
                 f"WHERE a = {nid} RETURN b, r.X_pu")

        rows = self.g.query(q)
        result = []
        for row in rows:
            parts = row.split(" | ")
            if len(parts) >= 1:
                try:
                    nb_id = int(parts[0])
                    nb    = self.g.get_node(nb_id)
                    if nb:
                        p = json.loads(nb['properties'])
                        result.append(p)
                except ValueError:
                    pass
        return result


if __name__ == "__main__":
    parser = RawParser()
    # parser.leer_archivo("IEEE 14 bus.raw")
    # parser.leer_archivo("IEEE 118 Bus v2.raw")
    parser.leer_archivo("IEEE 118 Bus v2.raw")
    data = parser.obtener_dataframes()

    with GraphDbLoader("IEEE118bus.db") as loader:
        loader.limpiar_base_de_datos()
        loader.cargar_datos(data)
        loader.resumen()
        print("\nGrafo cargado correctamente.")
