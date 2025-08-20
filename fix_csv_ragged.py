#!/usr/bin/env python3
"""
Script para arreglar el problema de CSV "ragged lines" 
y procesar el archivo real ORDEN DE VENTA CUA.csv
"""

import polars as pl
import os
from config import DATABASE_CONFIG
import fdb

def analyze_real_csv():
    """Analiza el archivo CSV real del usuario"""
    print("[ANALISIS] Analizando ORDEN DE VENTA CUA.csv...")
    
    csv_file = "ORDEN DE VENTA CUA.csv"
    
    if not os.path.exists(csv_file):
        print("[ERROR] No se encontró ORDEN DE VENTA CUA.csv")
        return False
    
    try:
        # Leer primeras líneas para análisis
        with open(csv_file, 'r', encoding='utf-8', errors='ignore') as f:
            lines = []
            for i, line in enumerate(f):
                lines.append(line.strip())
                if i >= 15:  # Primeras 15 líneas
                    break
        
        print(f"[INFO] Primeras líneas del CSV:")
        for i, line in enumerate(lines, 1):
            fields_count = len(line.split('\t')) if '\t' in line else len(line.split(','))
            print(f"  Línea {i:2d}: {fields_count} campos - {line[:80]}...")
        
        # Detectar separador
        separator = '\t' if '\t' in lines[0] else ','
        separator_name = "TAB" if separator == '\t' else "COMMA"
        print(f"[INFO] Separador detectado: {separator_name}")
        
        return separator
        
    except Exception as e:
        print(f"[ERROR] Analizando CSV: {e}")
        return False

def read_csv_robust(csv_file, separator='\t'):
    """Lee CSV de manera robusta manejando líneas irregulares"""
    print("[LECTURA] Leyendo CSV con manejo robusto...")
    
    try:
        # Estrategia 1: Con truncate_ragged_lines
        df = pl.read_csv(
            csv_file,
            separator=separator,
            has_header=False,
            truncate_ragged_lines=True,  # Esto arregla el problema
            ignore_errors=True,
            infer_schema_length=0,  # No inferir tipos, todo como string
            encoding='utf-8'
        )
        
        print(f"[OK] CSV leído: {df.height} filas, {df.width} columnas")
        return df
        
    except Exception as e1:
        print(f"[ERROR] Estrategia 1 falló: {e1}")
        
        try:
            # Estrategia 2: Sin schema, máxima tolerancia
            df = pl.read_csv(
                csv_file,
                separator=separator,
                has_header=False,
                truncate_ragged_lines=True,
                ignore_errors=True,
                null_values=["", "NULL", "null", "#N/D"],
                encoding='utf-8-sig'  # Por si tiene BOM
            )
            
            print(f"[OK] CSV leído (estrategia 2): {df.height} filas, {df.width} columnas")
            return df
            
        except Exception as e2:
            print(f"[ERROR] Estrategia 2 falló: {e2}")
            return None

def find_propuestas_in_real_csv(df):
    """Encuentra propuestas en el CSV real"""
    print("[BUSQUEDA] Buscando propuestas en columna B...")
    
    if df.width < 2:
        print("[ERROR] CSV no tiene suficientes columnas")
        return []
    
    # Buscar en columna B (índice 1)
    col_b = df.get_column("column_1")
    
    propuestas_found = []
    
    # Revisar desde fila 11 (índice 10) como especifica el usuario
    start_row = 10
    
    print(f"[INFO] Revisando desde fila {start_row + 1}...")
    
    for i in range(start_row, min(df.height, start_row + 20)):  # Revisar 20 filas
        if i < len(col_b):
            val = col_b[i]
            if val is not None:
                val_str = str(val).strip()
                if val_str and val_str not in ['', 'PROPUESTA JKM', 'NUMERO']:
                    print(f"  Fila {i+1}: '{val_str}'")
                    
                    # Si parece un número (LEGAJO), agregarlo
                    if val_str.isdigit() or val_str.replace('.', '').isdigit():
                        propuestas_found.append(val_str)
    
    print(f"[RESULTADO] {len(propuestas_found)} propuestas encontradas")
    return propuestas_found[:5]  # Primeras 5 para prueba

def test_propuestas_in_database(propuestas):
    """Prueba si las propuestas existen en la base de datos"""
    print("[TEST] Probando propuestas en base de datos...")
    
    if not propuestas:
        print("[INFO] No hay propuestas para probar")
        return []
    
    try:
        con = fdb.connect(
            host=DATABASE_CONFIG["host"],
            database=DATABASE_CONFIG["database_path"],
            user=DATABASE_CONFIG["user"],
            password=DATABASE_CONFIG["password"],
            charset=DATABASE_CONFIG["charset"]
        )
        
        query = """
        SELECT DISTINCT 
            p.LEGAJO AS COD_PROPUESTA,
            c.NOMBRE as Nombre_Cliente,
            s.NOMBRE AS SUCURSAL
        FROM CONTRATOS_CLIENTES cc
        INNER JOIN clientes c ON cc.COD_CLIENTE=c.COD_CLIENTE
        INNER JOIN contratos ct ON cc.COD_CONTRATO=ct.COD_CONTRATO
        INNER JOIN propuesta p ON ct.COD_PROPUESTA=p.COD_PROPUESTA
        INNER JOIN creditos cr ON cr.COD_PROPUESTA = p.COD_PROPUESTA
        INNER JOIN sucursales s ON cr.COD_SUCURSAL=s.COD_SUCURSAL
        WHERE p.LEGAJO = ?
        """
        
        cur = con.cursor()
        matches_found = []
        
        for propuesta in propuestas:
            try:
                cur.execute(query, (propuesta,))
                result = cur.fetchone()
                
                if result:
                    matches_found.append({
                        'legajo': propuesta,
                        'cliente': result[1],
                        'sucursal': result[2]
                    })
                    print(f"  [MATCH] {propuesta} -> {result[1]}")
                else:
                    print(f"  [NO MATCH] {propuesta}")
                    
            except Exception as e:
                print(f"  [ERROR] {propuesta}: {e}")
        
        con.close()
        
        print(f"[RESULTADO] {len(matches_found)} matches encontrados")
        return matches_found
        
    except Exception as e:
        print(f"[ERROR] Consultando base de datos: {e}")
        return []

def process_real_csv_sample():
    """Procesa una muestra del CSV real"""
    print("[PROCESO] Procesando muestra del CSV real...")
    
    csv_file = "ORDEN DE VENTA CUA.csv"
    
    # 1. Analizar archivo
    separator = analyze_real_csv()
    if not separator:
        return False
    
    # 2. Leer CSV robusto
    df = read_csv_robust(csv_file, separator)
    if df is None:
        return False
    
    # 3. Encontrar propuestas
    propuestas = find_propuestas_in_real_csv(df)
    
    # 4. Probar en base de datos
    matches = test_propuestas_in_database(propuestas)
    
    if matches:
        print("\n[EXITO] ¡Proceso funcionando!")
        print("Propuestas que se pueden llenar:")
        for match in matches:
            print(f"  - LEGAJO {match['legajo']}: {match['cliente']}")
        
        return True
    else:
        print("\n[INFO] No se encontraron matches con las propuestas del CSV")
        print("Esto puede ser normal si:")
        print("  1. Los LEGAJOs en el CSV no están en la BD")
        print("  2. El formato de los LEGAJOs es diferente")
        print("  3. Los datos están en filas diferentes")
        
        return False

def update_polars_code():
    """Actualiza el código de Polars para manejar CSV irregulares"""
    print("\n[UPDATE] Actualizando código para CSV irregulares...")
    
    files_to_update = [
        "main.py",
        "client_polars.py", 
        "debug_tools.py"
    ]
    
    for filepath in files_to_update:
        if not os.path.exists(filepath):
            continue
            
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Buscar y reemplazar llamadas a pl.read_csv problemáticas
            old_patterns = [
                'pl.read_csv(\n                file_path,\n                has_header=False,\n                dtypes=str,\n                ignore_errors=True,\n                truncate_ragged_lines=True',
                'pl.read_csv(\n            file_path,\n            has_header=False,\n            dtypes=str,\n            ignore_errors=True',
                'pl.read_csv(file_path, has_header=False, dtypes=str, ignore_errors=True)',
                'pl.read_csv(csv_file, has_header=False, n_rows=15, dtypes=str, ignore_errors=True)'
            ]
            
            new_pattern = '''pl.read_csv(
                file_path,
                has_header=False,
                truncate_ragged_lines=True,
                ignore_errors=True,
                infer_schema_length=0'''
            
            modified = False
            for old_pattern in old_patterns:
                if old_pattern in content:
                    content = content.replace(old_pattern, new_pattern)
                    modified = True
                    print(f"  [UPDATE] Patrón actualizado en {filepath}")
            
            # También buscar patrones más simples
            if 'pl.read_csv(' in content and 'truncate_ragged_lines=True' not in content:
                # Agregar truncate_ragged_lines a todas las llamadas
                import re
                
                pattern = r'pl\.read_csv\(\s*([^,]+),\s*([^)]+)\)'
                
                def add_truncate(match):
                    file_param = match.group(1)
                    other_params = match.group(2)
                    
                    if 'truncate_ragged_lines' not in other_params:
                        return f'pl.read_csv({file_param}, {other_params}, truncate_ragged_lines=True)'
                    else:
                        return match.group(0)
                
                new_content = re.sub(pattern, add_truncate, content)
                
                if new_content != content:
                    content = new_content
                    modified = True
                    print(f"  [UPDATE] Agregado truncate_ragged_lines en {filepath}")
            
            if modified:
                # Hacer backup
                with open(filepath + '.backup', 'w', encoding='utf-8') as f_backup:
                    with open(filepath, 'r', encoding='utf-8') as f_orig:
                        f_backup.write(f_orig.read())
                
                # Escribir versión actualizada
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(content)
                
                print(f"  [OK] {filepath} actualizado")
            else:
                print(f"  [OK] {filepath} ya estaba correcto")
                
        except Exception as e:
            print(f"  [ERROR] {filepath}: {e}")

def main():
    print("ARREGLO DE CSV RAGGED LINES")
    print("=" * 50)
    
    # 1. Actualizar código primero
    update_polars_code()
    
    # 2. Procesar CSV real
    success = process_real_csv_sample()
    
    if success:
        print("\n" + "=" * 50)
        print("[COMPLETADO] ¡CSV procesado exitosamente!")
        print("\nAhora puedes ejecutar:")
        print("  python main_polars.py")
        print("  start_server.bat")
        print("\nEl sistema llenará automáticamente la columna CLIENTE")
        print("con los nombres correspondientes a cada LEGAJO.")
    else:
        print("\n" + "=" * 50)
        print("[INFO] Proceso completado con observaciones")
        print("El sistema está configurado, solo necesita ajustar:")
        print("  1. Verificar LEGAJOs en el CSV")
        print("  2. Verificar que correspondan con la BD")

if __name__ == "__main__":
    main()