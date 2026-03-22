"""
PARSER DE ARCHIVOS .RAW DE PSS/E
Soporta versiones 33
"""

import pandas as pd
import numpy as np
import re


class RawParser:
    """
    Parser completo para archivos .raw de PSS/E
    """
    
    def __init__(self):
        self.version = None
        self.base_mva = 100.0
        self.frecuencia = 60.0
        self.in_transformer = False
        self.seccion_actual = None
        
        # Datos extraídos
        self.buses = []
        self.cargas = []
        self.generadores = []
        self.lineas = []
        self.transformadores = []
        self.shunts = []
        self.zonas = []
        
    def leer_archivo(self, archivo_raw):
        """
        Lee y parsea un archivo .raw de PSS/E
        """
       
        print(f"Leyendo archivo: {archivo_raw}")
        print("=" * 80)
        
        with open(archivo_raw, 'r', encoding='utf-8', errors='ignore') as f:
            lineas = f.readlines()
        
        # Parsear encabezado
        self._parsear_encabezado(lineas[0:3])

        line_iter = iter(enumerate(lineas))

        for i, linea in line_iter:
            linea = linea.rstrip('\n').strip()

            if not linea or linea.startswith('@'):
                continue

            if i==3:
                self.seccion_actual = 'BUS DATA'
            
            if linea.startswith('0 /'):
                seccion = linea.split(',')
                if len(seccion) > 1:
                    seccion = seccion[1].replace('BEGIN', '').replace('begin', '').replace('Begin', '').strip()
                    self.seccion_actual = seccion
                else:
                    print('end')
                continue

            if i>=3:

                # Parsear según la sección actual
                if self.seccion_actual == 'BUS DATA' or self.seccion_actual.lower() == 'bus data':
                    self._parsear_bus(linea)
                elif self.seccion_actual == 'LOAD DATA' or self.seccion_actual.lower() == 'load data':
                    self._parsear_carga(linea)
                elif self.seccion_actual == 'FIXED SHUNT DATA' or self.seccion_actual.lower() == 'fixed shunt data':
                    self._parsear_shunt(linea)
                elif self.seccion_actual == 'GENERATOR DATA' or self.seccion_actual.lower() == 'generator data':
                    self._parsear_generador(linea)
                elif self.seccion_actual == 'BRANCH DATA' or self.seccion_actual.lower() == 'branch data':
                    self._parsear_linea(linea)
                elif self.seccion_actual == 'TRANSFORMER DATA' or self.seccion_actual.lower() == 'transformer data':
                    i = self.parse_transformer_block(lineas, i)
                    next(line_iter, None)
                    next(line_iter, None)
                    next(line_iter, None)
                elif self.seccion_actual == 'ZONE DATA' or self.seccion_actual.lower() == 'zone data':
                    self._parsear_zona(linea)
    
    def _parsear_encabezado(self, lineas_encabezado):
        """Parsea las primeras líneas del encabezado"""
        # Eliminar comentarios (todo después de '/')
        primera_linea_limpia = lineas_encabezado[0].split('/')[0]
        primera_linea = primera_linea_limpia.split(',')
        
        # IC, SBASE, REV, XFRRAT, NXFRAT, BASFRQ
        if len(primera_linea) >= 2:
            try:
                self.base_mva = float(primera_linea[1].strip())
            except ValueError:
                self.base_mva = 100.0
        
        if len(primera_linea) >= 3:
            try:
                self.version = int(float(primera_linea[2].strip()))
            except ValueError:
                self.version = 33
        
        if len(primera_linea) >= 6:
            try:
                self.frecuencia = float(primera_linea[5].strip())
            except ValueError:
                self.frecuencia = 60.0
    
    def _parsear_linea_csv(self, linea):
        """
        Parsea una línea CSV del archivo raw, manejando strings entre comillas
        """
        # Eliminar comentarios (todo después de '/')
        if '/' in linea:
            linea = linea.split('/')[0]
        
        elementos = []
        actual = ''
        dentro_comillas = False
        
        for char in linea:
            if char == "'" or char == '"':
                dentro_comillas = not dentro_comillas
            elif char == ',' and not dentro_comillas:
                elementos.append(actual.strip())
                actual = ''
            else:
                actual += char
        
        # Agregar último elemento
        if actual.strip():
            elementos.append(actual.strip())
        
        return elementos
    
    def _convertir_tipo(self, valor, tipo_esperado='float'):
        """Convierte un valor a su tipo apropiado"""
        if not valor or valor in ['', ' ']:
            return 0.0 if tipo_esperado == 'float' else 0 if tipo_esperado == 'int' else ''
        
        valor = valor.strip().strip("'").strip('"')
        
        try:
            if tipo_esperado == 'int':
                return int(float(valor))
            elif tipo_esperado == 'float':
                return float(valor)
            else:
                return valor
        except (ValueError, TypeError):
            return 0.0 if tipo_esperado == 'float' else 0 if tipo_esperado == 'int' else valor
    
    def _parsear_bus(self, linea):
        """
        Parsea datos de bus
        Formato PSS/E: I,NAME,BASKV,IDE,AREA,ZONE,OWNER,VM,VA,NVHI,NVLO,EVHI,EVLO
        """
        partes = self._parsear_linea_csv(linea)
        
        if len(partes) < 4:
            return
        
        bus_num = self._convertir_tipo(partes[0], 'int')
        
        # tipos PQ: 1, PV: 2, slack: 3
        
        bus_data = {
            'numero': bus_num,
            'nombre': self._convertir_tipo(partes[1], 'str') if len(partes) > 1 else f'Bus_{bus_num}',
            'V_base_kV': self._convertir_tipo(partes[2], 'float') if len(partes) > 2 else 138.0,
            'tipo': self._convertir_tipo(partes[3], 'int') if len(partes) > 3 else 1,
            'area': self._convertir_tipo(partes[4], 'int') if len(partes) > 4 else 1,
            'zona': self._convertir_tipo(partes[5], 'int') if len(partes) > 5 else 1,
            'ower': self._convertir_tipo(partes[6], 'int') if len(partes) > 6 else 1,
            'V_mag_pu': self._convertir_tipo(partes[7], 'float') if len(partes) > 7 else 1.0,
            'V_ang_deg': self._convertir_tipo(partes[8], 'float') if len(partes) > 8 else 0.0,
            'V_max_pu': self._convertir_tipo(partes[9], 'float') if len(partes) > 9 else 1.1,
            'V_min_pu': self._convertir_tipo(partes[10], 'float') if len(partes) > 10 else 0.9,
            'V_EVHI_pu': self._convertir_tipo(partes[11], 'float') if len(partes) > 11 else 1.1,
            'V_EVLO_pu': self._convertir_tipo(partes[12], 'float') if len(partes) > 12 else 0.9,
        }
        
        self.buses.append(bus_data)
    
    def _parsear_carga(self, linea):
        """
        Parsea datos de carga
        Formato: I,ID,STATUS,AREA,ZONE,PL,QL,IP,IQ,YP,YQ,OWNER,SCALE,INTRPT
        """
        # print(f'seccion: {self.seccion_actual}-> {linea}')
        partes = self._parsear_linea_csv(linea)
        
        if len(partes) < 6:
            return
                
        carga_data = {
            'bus_numero': self._convertir_tipo(partes[0], 'int'),
            'id': self._convertir_tipo(partes[1], 'str') if len(partes) > 1 else '1',
            'status': self._convertir_tipo(partes[2], 'int') if len(partes) > 2 else '1',
            'area': self._convertir_tipo(partes[3], 'int') if len(partes) > 3 else 1,
            'zona': self._convertir_tipo(partes[4], 'int') if len(partes) > 4 else 1,
            'PL_MW': self._convertir_tipo(partes[5], 'float') if len(partes) > 5 else 0.0,
            'QL_MVAR': self._convertir_tipo(partes[6], 'float') if len(partes) > 6 else 0.0,
            'IP_MW': self._convertir_tipo(partes[7], 'float') if len(partes) > 7 else 0.0,
            'IQ_MVAR': self._convertir_tipo(partes[8], 'float') if len(partes) > 8 else 0.0,
            'YP_MW': self._convertir_tipo(partes[9], 'float') if len(partes) > 9 else 0.0,
            'YQ_MVAR': self._convertir_tipo(partes[10], 'float') if len(partes) > 10 else 0.0,
            'OWNER': self._convertir_tipo(partes[11], 'str') if len(partes) > 11 else 0.0,
            'SCALE': self._convertir_tipo(partes[12], 'int') if len(partes) > 12 else 0.0,
            'INTRPT': self._convertir_tipo(partes[13], 'int') if len(partes) > 13 else 0.0,
            'PL_pu': 0.0,
            'QL_pu': 0.0,
        }
        
        # Convertir a pu
        carga_data['PL_pu'] = carga_data['PL_MW'] / self.base_mva
        carga_data['QL_pu'] = carga_data['QL_MVAR'] / self.base_mva
        
        self.cargas.append(carga_data)
    
    def _parsear_generador(self, linea):
        """
        Parsea datos de generador
        Formato: I,ID,PG,QG,QT,QB,VS,IREG,MBASE,ZR,ZX,RT,XT,GTAP,STAT,RMPCT,PT,PB,O1,F1,...
        """
        partes = self._parsear_linea_csv(linea)
        
        if len(partes) < 4:
            return
            
        gen_data = {
            'bus_numero': self._convertir_tipo(partes[0], 'int'),
            'id': self._convertir_tipo(partes[1], 'str') if len(partes) > 1 else '1',
            'P_MW': self._convertir_tipo(partes[2], 'float') if len(partes) > 2 else 0.0,
            'Q_MVAR': self._convertir_tipo(partes[3], 'float') if len(partes) > 3 else 0.0,
            'Q_max_MVAR': self._convertir_tipo(partes[4], 'float') if len(partes) > 4 else 100.0,
            'Q_min_MVAR': self._convertir_tipo(partes[5], 'float') if len(partes) > 5 else -100.0,
            'V_set_pu': self._convertir_tipo(partes[6], 'float') if len(partes) > 6 else 1.0,
            'IREG': self._convertir_tipo(partes[7], 'float') if len(partes) > 7 else 1.0,
            # 'NREG': self._convertir_tipo(partes[8], 'float') if len(partes) > 8 else 1.0,
            'MVA_base': self._convertir_tipo(partes[8], 'float') if len(partes) > 8 else self.base_mva,
            'ZR':self._convertir_tipo(partes[9], 'float') if len(partes) > 9 else 0.0,
            'ZX':self._convertir_tipo(partes[10], 'float') if len(partes) > 10 else 0.0,
            'RT':self._convertir_tipo(partes[11], 'float') if len(partes) > 11 else 0.0,
            'XT':self._convertir_tipo(partes[12], 'float') if len(partes) > 12 else 0.0,
            'GTAP':self._convertir_tipo(partes[13], 'float') if len(partes) > 13 else 0.0,
            'status':self._convertir_tipo(partes[14], 'int') if len(partes) > 14 else 1,
            'RMPCT':self._convertir_tipo(partes[15], 'float') if len(partes) > 15 else 0.0,
            'P_max_MW': self._convertir_tipo(partes[16], 'float') if len(partes) > 16 else 100.0,
            'P_min_MW': self._convertir_tipo(partes[17], 'float') if len(partes) > 17 else 0.0,
            'BASLOD': self._convertir_tipo(partes[18], 'float') if len(partes) > 18 else 0.0,
            'P_pu': 0.0,
            'Q_pu': 0.0
        }
        
        # Convertir a pu
        gen_data['P_pu'] = gen_data['P_MW'] / self.base_mva
        gen_data['Q_pu'] = gen_data['Q_MVAR'] / self.base_mva
        
        self.generadores.append(gen_data)
    
    def _parsear_linea(self, linea):
        """
        Parsea datos de línea (branch)
        Formato: I,J,CKT,R,X,B,RATEA,RATEB,RATEC,GI,BI,GJ,BJ,ST,MET,LEN,O1,F1,...
        """
        partes = self._parsear_linea_csv(linea)
        
        if len(partes) < 6:
            return
        
        linea_data = {
            'from_bus_numero': self._convertir_tipo(partes[0], 'int'),
            'to_bus_numero': self._convertir_tipo(partes[1], 'int'),
            '_CKT': self._convertir_tipo(partes[2], 'str') if len(partes) > 2 else '1',

            'R_pu': self._convertir_tipo(partes[3], 'float') if len(partes) > 3 else 0.0,
            'X_pu': self._convertir_tipo(partes[4], 'float') if len(partes) > 4 else 0.0,
            'B_pu': self._convertir_tipo(partes[5], 'float') if len(partes) > 5 else 0.0,

            'rate_A_MVA': self._convertir_tipo(partes[6], 'float') if len(partes) > 6 else 0.0,
            'rate_B_MVA': self._convertir_tipo(partes[7], 'float') if len(partes) > 7 else 0.0,
            'rate_C_MVA': self._convertir_tipo(partes[8], 'float') if len(partes) > 8 else 0.0,

            'GI_pu': self._convertir_tipo(partes[9], 'float') if len(partes) > 9 else 0.0,
            'BI_pu': self._convertir_tipo(partes[10], 'float') if len(partes) > 10 else 0.0,
            'GJ_pu': self._convertir_tipo(partes[11], 'float') if len(partes) > 11 else 0.0,
            'BJ_pu': self._convertir_tipo(partes[12], 'float') if len(partes) > 12 else 0.0,

            'status': self._convertir_tipo(partes[13], 'int') if len(partes) > 13 else 1,
            'metodo': self._convertir_tipo(partes[14], 'int') if len(partes) > 14 else 1,
            'longitud_km': self._convertir_tipo(partes[15], 'float') if len(partes) > 15 else 0.0,

            'owner1': self._convertir_tipo(partes[16], 'int') if len(partes) > 16 else 1,
            'frac1': self._convertir_tipo(partes[17], 'float') if len(partes) > 17 else 1.0,
            'owner2': self._convertir_tipo(partes[18], 'int') if len(partes) > 18 else 0,
            'frac2': self._convertir_tipo(partes[19], 'float') if len(partes) > 19 else 0.0,
            'owner3': self._convertir_tipo(partes[20], 'int') if len(partes) > 20 else 0,
            'frac3': self._convertir_tipo(partes[21], 'float') if len(partes) > 21 else 0.0,
            'owner4': self._convertir_tipo(partes[22], 'int') if len(partes) > 22 else 0,
            'frac4': self._convertir_tipo(partes[23], 'float') if len(partes) > 23 else 0.0,
        }
        
        self.lineas.append(linea_data)

    def parse_transformer_block(self, lineas, i):
        """
        Parsea un bloque de 4 líneas correspondientes a un transformador en formato PSS®E (.raw).
        Recibe la lista completa de líneas y el índice de la primera línea (i).
        Devuelve un diccionario con los parámetros del transformador.
        """
        self.in_transformer = True
        # --------------------
        # LÍNEA 1: Identificación y configuración básica
        # --------------------
        partes1 = [p.strip() for p in lineas[i].split(',')]
        n1 = len(partes1)

        from_bus = self._convertir_tipo(partes1[0], 'int') if n1 > 0 else -1
        to_bus = self._convertir_tipo(partes1[1], 'int') if n1 > 1 else -1
        tertiary_bus = self._convertir_tipo(partes1[2], 'int') if n1 > 2 else 0

        transformer = {
            'from_bus_index': self._convertir_tipo(partes1[0], 'int') if n1 > 0 else -1,
            'to_bus_index': self._convertir_tipo(partes1[1], 'int') if n1 > 1 else -1,
            'tertiary_bus_index': self._convertir_tipo(partes1[2], 'int') if n1 > 2 else 0,
            'ckt_id': self._convertir_tipo(partes1[3], 'str') if n1 > 3 else '1',
            'CW': self._convertir_tipo(partes1[4], 'int') if n1 > 4 else 1,
            'CZ': self._convertir_tipo(partes1[5], 'int') if n1 > 5 else 1,
            'CM': self._convertir_tipo(partes1[6], 'int') if n1 > 6 else 1,
            'MAG1': self._convertir_tipo(partes1[7], 'float') if n1 > 7 else 0.0,
            'MAG2': self._convertir_tipo(partes1[8], 'float') if n1 > 8 else 0.0,
            'NMETR': self._convertir_tipo(partes1[9], 'int') if n1 > 9 else 2,
            'NAME': self._convertir_tipo(partes1[10], 'str') if n1 > 10 else '',
            'STAT': self._convertir_tipo(partes1[11], 'int') if n1 > 11 else 1,
            'VECGRP': self._convertir_tipo(partes1[12], 'str') if n1 > 12 else '',
        }

        # --------------------
        # LÍNEA 2: Impedancias y base MVA
        # --------------------
        partes2 = [p.strip() for p in lineas[i+1].split(',')]
        n2 = len(partes2)

        transformer.update({
            'R1_2_pu': self._convertir_tipo(partes2[0], 'float') if n2 > 0 else 0.0,
            'X1_2_pu': self._convertir_tipo(partes2[1], 'float') if n2 > 1 else 0.0,
            'SBASE1_2_MVA': self._convertir_tipo(partes2[2], 'float') if n2 > 2 else 100.0,
            'R2_3_pu': self._convertir_tipo(partes2[3], 'float') if n2 > 3 else 0.0,
            'X2_3_pu': self._convertir_tipo(partes2[4], 'float') if n2 > 4 else 0.0,
            'SBASE2_3_MVA': self._convertir_tipo(partes2[5], 'float') if n2 > 5 else 100.0,
            'R3_1_pu': self._convertir_tipo(partes2[6], 'float') if n2 > 6 else 0.0,
            'X3_1_pu': self._convertir_tipo(partes2[7], 'float') if n2 > 7 else 0.0,
            'SBASE3_1_MVA': self._convertir_tipo(partes2[8], 'float') if n2 > 8 else 100.0,
        })

        # --------------------
        # LÍNEA 3: Datos de devanados (tap, control, límites, etc.)
        # --------------------
        partes3 = [p.strip() for p in lineas[i+2].split(',')]
        n3 = len(partes3)

        transformer.update({
            'WINDV1': self._convertir_tipo(partes3[0], 'float') if n3 > 0 else 1.0,
            'ANG1': self._convertir_tipo(partes3[1], 'float') if n3 > 1 else 0.0,
            'WINDV2': self._convertir_tipo(partes3[2], 'float') if n3 > 2 else 1.0,
            'ANG2': self._convertir_tipo(partes3[3], 'float') if n3 > 3 else 0.0,
            'WINDV3': self._convertir_tipo(partes3[4], 'float') if n3 > 4 else 1.0,
            'ANG3': self._convertir_tipo(partes3[5], 'float') if n3 > 5 else 0.0,
            'COD1': self._convertir_tipo(partes3[6], 'int') if n3 > 6 else 0,
            'CONT1': self._convertir_tipo(partes3[7], 'int') if n3 > 7 else 0,
            'RMA1': self._convertir_tipo(partes3[8], 'float') if n3 > 8 else 1.1,
            'RMI1': self._convertir_tipo(partes3[9], 'float') if n3 > 9 else 0.9,
        })

        # --------------------
        # LÍNEA 4: Referencias de magnitud y ángulo
        # --------------------
        partes4 = [p.strip() for p in lineas[i+3].split(',')]
        n4 = len(partes4)

        transformer.update({
            'VMSTAR': self._convertir_tipo(partes4[0], 'float') if n4 > 0 else 1.0,
            'ANSTAR': self._convertir_tipo(partes4[1], 'float') if n4 > 1 else 0.0,
        })

        self.transformadores.append(transformer)

    def _parsear_zona(self, linea):
        """
        Formato PSS/E v33: I, ZONAME
        I      : numero de zona (int)
        ZONAME : nombre de zona (str, hasta 12 caracteres)
        """
        partes = self._parsear_linea_csv(linea)
        if len(partes) < 1:
            return
        self.zonas.append({
            'zona': self._convertir_tipo(partes[0], 'int'),
            'zoname': self._convertir_tipo(partes[1], 'str') if len(partes) > 1 else '',
        })

    def _parsear_shunt(self, linea):
        """
        Si BL > 0 ⇒ Condensador, inyecta potencia reactiva al sistema (eleva el voltaje).
        Si BL < 0 ⇒ Reactor, absorbe potencia reactiva (reduce el voltaje).
        """
        partes = self._parsear_linea_csv(linea)
        
        if len(partes) < 5:
            return
                
        shunt_data = {
            'bus_numero': self._convertir_tipo(partes[0], 'int'),
            'id': self._convertir_tipo(partes[1], 'str') if len(partes) > 1 else '1',
            'status': self._convertir_tipo(partes[2], 'int') if len(partes) > 2 else 1,  # STATUS agregado
            'GL_MW': self._convertir_tipo(partes[3], 'float') if len(partes) > 3 else 0.0,
            'BL_MVAR': self._convertir_tipo(partes[4], 'float') if len(partes) > 4 else 0.0,
            'GL_pu': 0.0,
            'BL_pu': 0.0,
        }
        
        # Convertir a pu
        shunt_data['GL_pu'] = shunt_data['GL_pu'] / self.base_mva
        shunt_data['BL_pu'] = shunt_data['BL_MVAR'] / self.base_mva
        
        self.shunts.append(shunt_data)
    
    def obtener_dataframes(self):
        """
        Convierte los datos parseados a DataFrames de pandas
        """
        df_buses = pd.DataFrame(self.buses) if self.buses else None
        df_zonas = pd.DataFrame(self.zonas) if self.zonas else None

        if df_buses is not None and df_zonas is not None:
            df_buses = df_buses.merge(df_zonas, on='zona', how='left')

        return {
            'base_MVA': self.base_mva,
            'frecuencia_Hz': self.frecuencia,
            'version': self.version,
            'buses': df_buses,
            'cargas': pd.DataFrame(self.cargas) if self.cargas else None,
            'generadores': pd.DataFrame(self.generadores) if self.generadores else None,
            'lineas': pd.DataFrame(self.lineas) if self.lineas else None,
            'transformadores': pd.DataFrame(self.transformadores) if self.transformadores else None,
            'shunts': pd.DataFrame(self.shunts) if self.shunts else None,
            'zonas': df_zonas,
        }



if __name__ == "__main__":
    
    # archivo_raw = r"IEEE 118 Bus v2.raw"
    archivo_raw = r"IEEE 14 bus.raw"
    
    try:
        # Leer archivo
        parser = RawParser()
        parser.leer_archivo(archivo_raw)
        data = parser.obtener_dataframes()

        print(data)
        
    except FileNotFoundError:
        print(f"\nERROR: No se encontró el archivo '{archivo_raw}'")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()