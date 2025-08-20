# client_polars.py
"""
Cliente optimizado para usar Polars en lugar de Pandas
Optimizado para archivos CSV grandes y máximo rendimiento
"""

import requests
import json
import os
import polars as pl
import time
from typing import Optional, Dict, Any
from config import DATABASE_CONFIG, CSV_CONFIG, POLARS_CONFIG

class CSVFirebirdClientPolars:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.session = requests.Session()
        
        # Configurar Polars
        pl.Config.set_tbl_rows(20)
        pl.Config.set_tbl_cols(10)
        if POLARS_CONFIG.get("n_threads"):
            pl.Config.set_global_string_cache(True)
    
    def configure_database(self, host: str, database_path: str, user: str, password: str, charset: str = "UTF8"):
        """Configura la conexión a la base de datos"""
        config = {
            "host": host,
            "database_path": database_path,
            "user": user,
            "password": password,
            "charset": charset
        }
        
        response = self.session.post(f"{self.base_url}/configure-database/", json=config)
        
        if response.status_code == 200:
            print("[CHECK] Base de datos configurada correctamente")
            return True
        else:
            print(f"[X] Error configurando base de datos: {response.text}")
            return False
    
    def process_csv_file(self, file_path: str, target_column: str = None, 
                        data_start_row: int = None, propuesta_column: str = None,
                        use_streaming: bool = None):
        """Procesa un archivo CSV usando Polars para máximo rendimiento"""
        
        # Usar valores por defecto de la configuración
        target_column = target_column or CSV_CONFIG["target_column"]
        data_start_row = data_start_row or CSV_CONFIG["data_start_row"]
        propuesta_column = propuesta_column or CSV_CONFIG["propuesta_column"]
        use_streaming = use_streaming if use_streaming is not None else CSV_CONFIG["use_streaming"]
        
        if not os.path.exists(file_path):
            print(f"[X] Archivo no encontrado: {file_path}")
            return None
        
        # Verificar tamaño del archivo
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        print(f"[CARPETA] Archivo: {os.path.basename(file_path)} ({file_size_mb:.1f} MB)")
        
        # Mostrar preview del archivo con Polars
        self._show_csv_preview(file_path, data_start_row)
        
        with open(file_path, 'rb') as f:
            files = {'file': (os.path.basename(file_path), f, 'text/csv')}
            data = {
                'target_column': target_column,
                'data_start_row': data_start_row,
                'propuesta_column': propuesta_column,
                'use_streaming': str(use_streaming).lower()
            }
            
            print(f"[COHETE] Enviando archivo para procesamiento...")
            start_time = time.time()
            
            response = self.session.post(f"{self.base_url}/process-csv/", files=files, data=data)
            
            upload_time = time.time() - start_time
        
        if response.status_code == 200:
            result = response.json()
            print("[CHECK] Archivo procesado correctamente con Polars")
            print(f"   [GRAFICO] Registros procesados: {result['processed_count']:,}")
            print(f"   [CHECK] Matches encontrados: {result['matched_count']:,}")
            print(f"   [TIEMPO]  Tiempo total: {result['execution_time']:.2f} segundos")
            print(f"   [EMOJI] Tiempo upload: {upload_time:.2f} segundos")
            print(f"   [COHETE] Velocidad: {result['processed_count']/result['execution_time']:,.0f} registros/seg")
            
            if result.get('chunks_processed'):
                print(f"   [CHUNKS] Chunks procesados: {result['chunks_processed']}")
            
            if result['errors']:
                print(f"   [WARNING]  Errores encontrados: {len(result['errors'])}")
                for error in result['errors'][:3]:  # Mostrar solo los primeros 3 errores
                    print(f"      - {error}")
                if len(result['errors']) > 3:
                    print(f"      ... y {len(result['errors']) - 3} errores más")
            
            return result
        else:
            print(f"[X] Error procesando archivo: {response.text}")
            return None
    
    def _show_csv_preview(self, file_path: str, data_start_row: int):
        """Muestra un preview del CSV usando Polars"""
        try:
            print(f"\n[LISTA] Preview del CSV (primeras {data_start_row + 5} filas):")
            
            # Leer solo las primeras filas para preview
            preview_df = pl.read_csv(
                file_path,
                has_header=False,
                n_rows=data_start_row + 5,
                dtypes=str,
                truncate_ragged_lines=True
            )
            
            print(f"   [EMOJI] Dimensiones: {preview_df.height} filas × {preview_df.width} columnas")
            
            # Mostrar algunas filas clave
            if preview_df.height >= data_start_row:
                print(f"\n   [NOTA] Fila {data_start_row} (primer dato):")
                row_data = preview_df.row(data_start_row - 1)
                for i, value in enumerate(row_data[:5]):  # Primeras 5 columnas
                    col_letter = chr(65 + i)  # A, B, C, D, E
                    print(f"      {col_letter}: {value}")
                
                # Mostrar columna B específicamente (PROPUESTA JKM)
                if preview_df.width > 1:
                    prop_value = preview_df.row(data_start_row - 1)[1]
                    print(f"   [TARGET] PROPUESTA JKM (col B): '{prop_value}'")
            
        except Exception as e:
            print(f"   [WARNING] No se pudo mostrar preview: {str(e)}")
    
    def get_propuesta_info(self, legajo: str):
        """Obtiene información de una propuesta específica"""
        response = self.session.get(f"{self.base_url}/propuesta/{legajo}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"[CHECK] Propuesta {legajo} encontrada:")
            print(f"   [EMOJI] Cliente: {data['nombre_cliente']}")
            print(f"   [EMOJI] Sucursal: {data['sucursal']}")
            print(f"   [DINERO] Monto: ${data['monto']:,.2f}")
            print(f"   [EMOJI] Fecha: {data['fecha_contrato']}")
            print(f"   [GRAFICO] Estado: {data['estado']}")
            return data
        else:
            print(f"[X] Propuesta {legajo} no encontrada")
            return None
    
    def health_check(self):
        """Verifica el estado del sistema"""
        response = self.session.get(f"{self.base_url}/health")
        
        if response.status_code == 200:
            status = response.json()
            print("[EMOJI] Estado del sistema:")
            print(f"   Status: {status['status']}")
            print(f"   Base de datos configurada: {status['database_configured']}")
            if 'database_connection' in status:
                print(f"   Conexión BD: {status['database_connection']}")
            return status
        else:
            print(f"[X] Error verificando estado: {response.text}")
            return None
    
    def benchmark_polars(self, test_sizes: list = None):
        """Benchmark de rendimiento con Polars"""
        test_sizes = test_sizes or [1000, 5000, 10000]
        
        print("\n[TEST] Benchmark de rendimiento con Polars")
        print("=" * 50)
        
        for size in test_sizes:
            print(f"\n[GRAFICO] Probando con {size:,} registros...")
            
            # Generar archivo de prueba
            test_file = self._generate_test_csv(size)
            
            try:
                # Probar procesamiento
                start_time = time.time()
                result = self.process_csv_file(test_file)
                total_time = time.time() - start_time
                
                if result:
                    print(f"   [TIEMPO] Tiempo total: {total_time:.2f}s")
                    print(f"   [COHETE] Velocidad: {size/total_time:,.0f} registros/s")
                    
            finally:
                # Limpiar archivo de prueba
                if os.path.exists(test_file):
                    os.remove(test_file)
    
    def _generate_test_csv(self, num_rows: int) -> str:
        """Genera un CSV de prueba con formato correcto"""
        test_file = f"test_polars_{num_rows}.csv"
        
        with open(test_file, 'w', encoding='utf-8') as f:
            # Escribir headers (10 filas)
            for i in range(10):
                f.write(f"HEADER_ROW_{i},PROPUESTA JKM,OTROS,CAMPOS\n")
            
            # Escribir datos de prueba
            for i in range(num_rows):
                legajo = f"{642799 + i}"  # Usar base del ejemplo real
                f.write(f"DATA_{i},{legajo},OTROS,DATOS\n")
        
        return test_file