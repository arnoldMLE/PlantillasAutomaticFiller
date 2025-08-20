#!/usr/bin/env python3
"""
Script para hacer match entre PROPUESTA del CSV y LEGAJO de la base de datos
y llenar automáticamente la columna CLIENTE
"""

import polars as pl
import fdb
import os
from config import DATABASE_CONFIG

def read_csv_robust():
    """Lee el CSV real de manera robusta"""
    print("[LECTURA] Leyendo ORDEN DE VENTA CUA.csv...")
    
    csv_file = "ORDEN DE VENTA CUA.csv"
    
    if not os.path.exists(csv_file):
        print("[ERROR] No se encontró ORDEN DE VENTA CUA.csv")
        return None
    
    try:
        # Leer con configuración robusta para CSV irregular
        df = pl.read_csv(
            csv_file,
            separator=',',  # CSV separado por COMAS
            has_header=False,
            truncate_ragged_lines=True,  # Arregla líneas irregulares
            ignore_errors=True,
            infer_schema_length=0,  # Todo como string
            encoding='utf-8'
        )
        
        print(f"[OK] CSV leído: {df.height} filas x {df.width} columnas")
        return df
        
    except Exception as e:
        print(f"[ERROR] Leyendo CSV: {e}")
        return None

def extract_propuestas_from_csv(df):
    """Extrae las propuestas de la columna B desde la fila 11"""
    print("[EXTRACCION] Extrayendo propuestas de columna B...")
    
    if df.width < 2:
        print("[ERROR] CSV no tiene columna B")
        return []
    
    # Usar el nombre real de la columna B (segunda columna)
    column_b_name = df.columns[1]
    col_b = df.get_column(column_b_name)
    
    print(f"[INFO] Extrayendo de columna '{column_b_name}'")
    
    # Revisar desde fila 11 (índice 10) - DESPUÉS de "PROPUESTA JKM"
    # Las filas 5-9 son solo ejemplos/plantillas, los datos reales empiezan en fila 11
    start_row = 10
    
    propuestas = []
    
    print(f"[INFO] Extrayendo de columna '{column_b_name}'")
    print(f"[INFO] Revisando desde fila {start_row + 1}...")
    
    for i in range(start_row, min(df.height, start_row + 100)):  # Revisar 100 filas
        if i < len(col_b):
            val = col_b[i]
            if val is not None:
                val_str = str(val).strip()
                
                # Filtrar solo valores que parecen números (LEGAJO/PROPUESTA)
                # Aceptar números puros de 4+ dígitos
                if val_str and val_str.isdigit() and len(val_str) >= 4:
                    propuestas.append(val_str)
                    print(f"  Fila {i+1}: '{val_str}'")
    
    print(f"[RESULTADO] {len(propuestas)} propuestas encontradas")
    return propuestas

def search_legajos_in_database(propuestas):
    """Busca las propuestas como LEGAJOs en la base de datos"""
    print("[BUSQUEDA] Buscando LEGAJOs en base de datos...")
    
    if not propuestas:
        print("[ERROR] No hay propuestas para buscar")
        return {}
    
    try:
        con = fdb.connect(
            host=DATABASE_CONFIG["host"],
            database=DATABASE_CONFIG["database_path"],
            user=DATABASE_CONFIG["user"],
            password=DATABASE_CONFIG["password"],
            charset=DATABASE_CONFIG["charset"]
        )
        
        # Tu consulta SQL real (simplificada para testing)
        query = """
        SELECT DISTINCT 
            p.LEGAJO AS COD_PROPUESTA,
            c.NOMBRE as Nombre_Cliente,
            s.NOMBRE AS SUCURSAL,
            CR.MONTO,
            ct.FECHA as FECHA_CONTRATO
        FROM CONTRATOS_CLIENTES cc
        INNER JOIN clientes c ON cc.COD_CLIENTE=c.COD_CLIENTE
        INNER JOIN contratos ct ON cc.COD_CONTRATO=ct.COD_CONTRATO
        INNER JOIN propuesta p ON ct.COD_PROPUESTA=p.COD_PROPUESTA
        INNER JOIN creditos cr ON cr.COD_PROPUESTA = p.COD_PROPUESTA
        INNER JOIN sucursales s ON cr.COD_SUCURSAL=s.COD_SUCURSAL
        WHERE p.LEGAJO = ?
        """
        
        cursor = con.cursor()
        matches = {}
        
        print(f"[INFO] Probando {len(propuestas)} propuestas...")
        
        for propuesta in propuestas:
            try:
                cursor.execute(query, (propuesta,))
                result = cursor.fetchone()
                
                if result:
                    matches[propuesta] = {
                        'legajo': result[0],
                        'cliente': result[1],
                        'sucursal': result[2],
                        'monto': result[3],
                        'fecha': result[4]
                    }
                    print(f"  [MATCH] {propuesta} -> {result[1]}")
                else:
                    print(f"  [NO MATCH] {propuesta}")
                    
            except Exception as e:
                print(f"  [ERROR] {propuesta}: {e}")
        
        con.close()
        
        print(f"[RESULTADO] {len(matches)} matches encontrados de {len(propuestas)} propuestas")
        return matches
        
    except Exception as e:
        print(f"[ERROR] Consultando base de datos: {e}")
        return {}

def fill_cliente_column(df, matches):
    """Llena la columna CLIENTE con los nombres encontrados"""
    print("[LLENADO] Llenando columna CLIENTE...")
    
    if not matches:
        print("[ERROR] No hay matches para llenar")
        return df
    
    # Encontrar la columna CLIENTE (debería ser la columna F, índice 5)
    cliente_col_index = 5  # Columna F según tu estructura
    
    # Verificar que existe la columna
    if df.width <= cliente_col_index:
        print(f"[ERROR] CSV no tiene suficientes columnas (necesita al menos {cliente_col_index + 1})")
        return df
    
    # Usar el nombre real de la columna CLIENTE
    cliente_col_name = df.columns[cliente_col_index]
    print(f"[INFO] Actualizando columna '{cliente_col_name}' (índice {cliente_col_index})")
    
    # Obtener valores actuales de la columna CLIENTE
    current_values = df.get_column(cliente_col_name).to_list()
    
    # Obtener valores de la columna PROPUESTA (columna B)
    propuesta_col_name = df.columns[1]  # Columna B
    col_b = df.get_column(propuesta_col_name)
    
    updates_count = 0
    
    # Recorrer y actualizar desde fila 11 (después de "PROPUESTA JKM")
    for i in range(10, min(df.height, len(col_b))):  # Desde fila 11
        if i < len(col_b):
            propuesta = str(col_b[i]).strip()
            
            if propuesta in matches:
                if i < len(current_values):
                    # Solo actualizar si está vacío o es placeholder
                    current_val = str(current_values[i]).strip()
                    if not current_val or current_val in ['', 'CLIENTE', 'nan', 'None']:
                        current_values[i] = matches[propuesta]['cliente']
                        updates_count += 1
                        print(f"  Fila {i+1}: LEGAJO '{propuesta}' -> '{matches[propuesta]['cliente']}'")
                    else:
                        print(f"  Fila {i+1}: LEGAJO '{propuesta}' ya tiene cliente: '{current_val}' (no sobrescrito)")    
    # Actualizar el DataFrame
    try:
        # Asegurar que la lista tiene el tamaño correcto
        while len(current_values) < df.height:
            current_values.append("")
        
        # Reemplazar la columna
        df = df.with_columns([
            pl.Series(name=cliente_col_name, values=current_values[:df.height])
        ])
        
        print(f"[OK] {updates_count} registros actualizados en columna CLIENTE")
        return df
        
    except Exception as e:
        print(f"[ERROR] Actualizando DataFrame: {e}")
        return df

def save_processed_csv(df):
    """Guarda el CSV procesado"""
    print("[GUARDADO] Guardando CSV procesado...")
    
    try:
        output_file = "ORDEN DE VENTA CUA_PROCESADO.csv"
        
        df.write_csv(
            output_file,
            separator=','  # Solo separador, sin has_header
        )
        
        print(f"[OK] Archivo guardado: {output_file}")
        return output_file
        
    except Exception as e:
        print(f"[ERROR] Guardando archivo: {e}")
        return None

def main():
    print("MATCH PROPUESTA-LEGAJO Y LLENADO AUTOMATICO")
    print("=" * 60)
    
    # 1. Leer CSV
    df = read_csv_robust()
    if df is None:
        return
    
    # 2. Extraer propuestas
    propuestas = extract_propuestas_from_csv(df)
    if not propuestas:
        return
    
    # 3. Buscar en base de datos
    matches = search_legajos_in_database(propuestas)
    if not matches:
        print("\n[INFO] No se encontraron matches en la base de datos")
        print("Posibles causas:")
        print("  1. Los LEGAJOs del CSV no existen en la BD")
        print("  2. Formato diferente de números")
        print("  3. Datos en tabla diferente")
        return
    
    # 4. Llenar columna CLIENTE
    df_updated = fill_cliente_column(df, matches)
    
    # 5. Guardar archivo procesado
    output_file = save_processed_csv(df_updated)
    
    if output_file:
        print(f"\n{'=' * 60}")
        print("[COMPLETADO] Proceso exitoso!")
        print(f"[ARCHIVO] {output_file}")
        print(f"[MATCHES] {len(matches)} registros completados")
        print("\nEjemplos de matches encontrados:")
        for i, (legajo, data) in enumerate(list(matches.items())[:3]):
            print(f"  {i+1}. LEGAJO {legajo}: {data['cliente']}")
        
        if len(matches) > 3:
            print(f"  ... y {len(matches) - 3} más")
            
    else:
        print("\n[ERROR] No se pudo completar el proceso")

if __name__ == "__main__":
    main()